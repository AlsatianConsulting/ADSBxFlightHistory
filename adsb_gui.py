#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ADSB History GUI (PyQt5, consolidated exports with enriched baseball card)

Features
--------
- Query by date range + ICAO hex.
- Output KML (time-enabled), CSV, JSON (consolidated across days).
- Output folder picker with "Open" to launch Explorer/Finder/xdg-open.
- Baseball card (right pane):
    * Aircraft type (ICAO code, e.g. GLF4)
    * Type name (friendly, e.g. Gulfstream IV / G-IV)
    * Registration (tail)
    * Registered owner
    * Flags (Military / LADD / PIA / Interesting)
    * Callsigns seen
    * ICAO Hex
    * Aircraft image (Planespotters if available)

Data details
------------
- Downloads ADSBexchange globe_history trace_full JSONs.
- Parses segments & points (with timestamps, alt, gs, track, etc.).
- Builds:
    * KML: static per-segment LineStrings + per-point TimeStamp placemarks
      with ExtendedData including:
        - core hit fields
        - meta_* fields (reg, owner, type, type_name, etc.)
        - one attribute per AC data key (type, flight, squawk, category, nic, ...).
    * CSV:
        - Base columns (hex, time, lat, lon, alt, etc.)
        - One column per AC data key.
        - ac_data_json as raw JSON.
    * JSON:
        - segments + full hit dicts.

Requirements
------------
    pip install PyQt5 requests simplekml
"""

import os
import sys
import json
import gzip
import io
import time
import csv
import subprocess
import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Any, Iterable, Optional, Set

import requests
import simplekml

from PyQt5 import QtCore, QtGui, QtWidgets

# -----------------------------
# ADSBx constants & helpers
# -----------------------------

HEADERS = {
    "Referer": "https://globe.adsbexchange.com/",
    "User-Agent": "adsbx-history-downloader-gui/pyqt/2.0",
}

BASE = (
    "https://globe.adsbexchange.com/globe_history/{y}/{m:02d}/{d:02d}/"
    "traces/{suffix}/trace_full_{hex}.json"
)

AC_DB_URL = "http://downloads.adsbexchange.com/downloads/basic-ac-db.json.gz"
_ACDB_CACHE = None  # in-memory cache of ADSBx aircraft DB

# ICAO aircraft type designator → common name
ICAO_TYPE_NAMES = {
    "GLF4": "Gulfstream IV / G-IV",
    "GLF5": "Gulfstream V / G-V",
    "GLF6": "Gulfstream G650 / GVI",

    "B737": "Boeing 737 (classic/NG)",
    "B738": "Boeing 737-800",
    "B739": "Boeing 737-900",
    "B38M": "Boeing 737 MAX 8",
    "B37M": "Boeing 737 MAX 7",
    "B39M": "Boeing 737 MAX 9",

    "A319": "Airbus A319",
    "A320": "Airbus A320",
    "A321": "Airbus A321",
    "A20N": "Airbus A320neo",
    "A21N": "Airbus A321neo",

    "B744": "Boeing 747-400",
    "B748": "Boeing 747-8",
    "B752": "Boeing 757-200",
    "B763": "Boeing 767-300",
    "B772": "Boeing 777-200",
    "B773": "Boeing 777-300",
    "B788": "Boeing 787-8 Dreamliner",
    "B789": "Boeing 787-9 Dreamliner",
    "B78X": "Boeing 787-10 Dreamliner",

    "E170": "Embraer 170",
    "E175": "Embraer 175",
    "E190": "Embraer 190",
    "E195": "Embraer 195",
    "E75L": "Embraer 175 (long wing)",
    "E75S": "Embraer 175 (short wing)",

    "CRJ2": "Bombardier CRJ200",
    "CRJ7": "Bombardier CRJ700",
    "CRJ9": "Bombardier CRJ900",
    "CRJX": "Bombardier CRJ1000",

    "AT45": "ATR 42-500",
    "AT46": "ATR 42-600",
    "AT72": "ATR 72",
    "AT76": "ATR 72-600",

    "C172": "Cessna 172 Skyhawk",
    "C182": "Cessna 182 Skylane",
    "C208": "Cessna 208 Caravan",
    "PC12": "Pilatus PC-12",
    "BE20": "Beechcraft King Air 200",
    "PAY2": "Piper PA-31 Navajo",
    # Extend as needed.
}


def daterange(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    """Inclusive date range."""
    cur = start
    one = dt.timedelta(days=1)
    while cur <= end:
        yield cur
        cur += one


def extract_hits(blob: Any) -> List[List[Dict[str, Any]]]:
    """
    Extract per-hit dictionaries from various ADSBx trace formats.

    Returns:
        list of segments; each segment is a list of hits.

    Each hit dict may contain:
        - timestamp (unix, seconds)
        - time_iso (ISO 8601 UTC string)
        - lat, lon
        - alt_ft, gs_knots, track_deg, flags, vrt_fpm
        - ac_data (nested ADS-B data dict, if present)
    """
    base_ts = None
    if isinstance(blob, dict):
        base_ts = blob.get("timestamp")
        seq = blob.get("trace") or blob.get("positions") or blob.get("trail")
    else:
        seq = blob
    segments: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []

    if not isinstance(seq, list):
        return segments

    def flush_segment():
        nonlocal current
        if current:
            segments.append(current)
            current = []

    for row in seq:
        lat = lon = None
        hit: Dict[str, Any] = {}

        if isinstance(row, list) and len(row) >= 3:
            # v2 trace_full format: [dt, lat, lon, alt, gs, track, flags, vrt, ac_data, ...]
            dt_offset = row[0] if len(row) >= 1 else None
            lat = row[1]
            lon = row[2]
            alt_ft = row[3] if len(row) >= 4 else None
            gs_knots = row[4] if len(row) >= 5 else None
            track_deg = row[5] if len(row) >= 6 else None
            flags = row[6] if len(row) >= 7 else None
            vrt_fpm = row[7] if len(row) >= 8 else None
            ac_data = row[8] if len(row) >= 9 and isinstance(row[8], dict) else None

            ts = None
            if isinstance(base_ts, (int, float)) and isinstance(dt_offset, (int, float)):
                ts = base_ts + dt_offset

            time_iso = None
            if isinstance(ts, (int, float)):
                try:
                    time_iso = dt.datetime.utcfromtimestamp(ts).isoformat() + "Z"
                except (OverflowError, OSError):
                    time_iso = None

            hit.update(
                {
                    "timestamp": ts,
                    "time_iso": time_iso,
                    "lat": lat,
                    "lon": lon,
                    "alt_ft": alt_ft,
                    "gs_knots": gs_knots,
                    "track_deg": track_deg,
                    "flags": flags,
                    "vrt_fpm": vrt_fpm,
                }
            )
            if ac_data is not None:
                hit["ac_data"] = ac_data

            # New leg detection: flags bit 2 (per ADSBx docs)
            if isinstance(flags, int) and (flags & 2):
                flush_segment()

        elif isinstance(row, dict):
            lat = row.get("lat")
            lon = row.get("lon") or row.get("lng")
            ts = row.get("time") or row.get("ts") or row.get("timestamp")
            time_iso = None
            if isinstance(ts, (int, float)):
                try:
                    time_iso = dt.datetime.utcfromtimestamp(ts).isoformat() + "Z"
                except (OverflowError, OSError):
                    time_iso = None
            hit.update(
                {
                    "timestamp": ts,
                    "time_iso": time_iso,
                    "lat": lat,
                    "lon": lon,
                }
            )
            # Copy some common extras if present
            for key in (
                "alt",
                "alt_ft",
                "gs",
                "gs_knots",
                "track",
                "track_deg",
                "flags",
                "vrt",
                "vrt_fpm",
                "ac_data",
            ):
                if key in row:
                    hit[key] = row[key]
        else:
            continue

        if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
            continue

        current.append(hit)

    flush_segment()
    return segments


def ensure_dir_for_file(path: str) -> None:
    directory = os.path.dirname(os.path.abspath(path))
    if directory:
        os.makedirs(directory, exist_ok=True)


def build_kml(
    segments: List[List[Dict[str, Any]]],
    hex_code: str,
    meta: Dict[str, Any],
    out_path: str,
) -> None:
    """
    Build a time-enabled KML:

    - Static LineString per segment (for context).
    - Point placemarks per hit with TimeStamp (time slider in Google Earth).
    - ExtendedData includes:
        * core hit fields (time, alt, speed, etc.)
        * aircraft meta (reg, owner, type, type_name, description)
        * one field per unique key in ac_data across all hits.
    """
    # Discover all ac_data keys across all segments
    ac_keys: List[str] = []
    ac_key_set = set()
    for seg in segments:
        for hit in seg:
            ac = hit.get("ac_data")
            if isinstance(ac, dict):
                for k in ac.keys():
                    if k not in ac_key_set:
                        ac_key_set.add(k)
                        ac_keys.append(k)
    ac_keys = sorted(ac_keys)

    kml = simplekml.Kml()
    root_name = f"ADSBx {hex_code.upper()} Track"
    fol = kml.newfolder(name=root_name)

    # Folder description with basic meta
    meta_lines = [f"ICAO: {hex_code.upper()}"]
    for k in ("registration", "type", "type_name", "owner", "description"):
        if meta.get(k):
            meta_lines.append(f"{k.capitalize()}: {meta[k]}")
    fol.description = "\n".join(meta_lines)

    total_points = 0

    # Static route lines
    for i, seg in enumerate(segments, 1):
        coords = []
        for hit in seg:
            try:
                lat = float(hit.get("lat"))
                lon = float(hit.get("lon"))
            except (TypeError, ValueError):
                continue
            coords.append((lon, lat))
        if len(coords) >= 2:
            ls = fol.newlinestring(name=f"Segment {i}")
            ls.coords = coords
            ls.altitudemode = simplekml.AltitudeMode.clamptoground
            ls.extrude = 0
            ls.tessellate = 1
        total_points += len(coords)

    # Per-hit points (time-enabled)
    pts_folder = fol.newfolder(name="Points")
    for seg_idx, seg in enumerate(segments, 1):
        for pt_idx, hit in enumerate(seg, 1):
            try:
                lat = float(hit.get("lat"))
                lon = float(hit.get("lon"))
            except (TypeError, ValueError):
                continue

            time_iso = hit.get("time_iso")
            name = time_iso or f"Seg {seg_idx} Pt {pt_idx}"

            try:
                p = pts_folder.newpoint(name=name, coords=[(lon, lat)])
            except Exception:
                continue

            # TimeStamp for GE time slider
            if time_iso:
                try:
                    p.timestamp.when = time_iso
                except Exception:
                    pass

            # Description
            desc_lines = [
                f"Segment: {seg_idx}",
                f"Index: {pt_idx}",
            ]
            for key in ("time_iso", "alt_ft", "gs_knots", "track_deg", "vrt_fpm", "flags"):
                val = hit.get(key)
                if val is not None:
                    desc_lines.append(f"{key}: {val}")

            # Basic meta
            for key in ("registration", "type", "type_name", "owner", "description"):
                if meta.get(key):
                    desc_lines.append(f"{key}: {meta[key]}")

            ac_data = hit.get("ac_data")
            if isinstance(ac_data, dict) and ac_data:
                desc_lines.append("AC data: " + json.dumps(ac_data, ensure_ascii=False))

            p.description = "\n".join(desc_lines)

            # ExtendedData: core hit fields
            for key, val in hit.items():
                if key == "ac_data":
                    continue
                if val is not None:
                    try:
                        p.extendeddata.simplenode(key, str(val))
                    except Exception:
                        continue

            # ExtendedData: aircraft meta
            for mk, mv in meta.items():
                if mv is not None:
                    try:
                        p.extendeddata.simplenode(f"meta_{mk}", str(mv))
                    except Exception:
                        continue

            # ExtendedData: AC data fields as their own attributes
            ac = ac_data if isinstance(ac_data, dict) else {}
            for ak in ac_keys:
                try:
                    v = ac.get(ak, "")
                    if isinstance(v, (dict, list)):
                        v = json.dumps(v, ensure_ascii=False)
                    p.extendeddata.simplenode(ak, str(v))
                except Exception:
                    continue

            total_points += 1

    if total_points == 0:
        empty = fol.newpoint(name="No valid points", coords=[])
        empty.description = "No valid coordinates found in trace."

    ensure_dir_for_file(out_path)
    kml.save(out_path)


def build_csv(
    segments: List[List[Dict[str, Any]]],
    hex_code: str,
    meta: Dict[str, Any],
    out_path: str,
) -> None:
    """
    Build CSV with per-point entries.

    Base columns:
        icao_hex, segment, point_index, time_unix, time_iso,
        latitude, longitude, alt_ft, gs_knots, track_deg, vrt_fpm, flags,
        registration, type, type_name, owner, description,
    Then:
        one column per unique key in ac_data
    Finally:
        ac_data_json
    """
    # Discover all ac_data keys
    ac_keys: List[str] = []
    ac_key_set = set()
    for seg in segments:
        for hit in seg:
            ac = hit.get("ac_data")
            if isinstance(ac, dict):
                for k in ac.keys():
                    if k not in ac_key_set:
                        ac_key_set.add(k)
                        ac_keys.append(k)
    ac_keys = sorted(ac_keys)

    ensure_dir_for_file(out_path)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        base_header = [
            "icao_hex",
            "segment",
            "point_index",
            "time_unix",
            "time_iso",
            "latitude",
            "longitude",
            "alt_ft",
            "gs_knots",
            "track_deg",
            "vrt_fpm",
            "flags",
            "registration",
            "type",       # ICAO code
            "type_name",  # friendly name
            "owner",
            "description",
        ]
        header = base_header + ac_keys + ["ac_data_json"]
        writer.writerow(header)

        reg = meta.get("registration")
        atype = meta.get("type")
        tname = meta.get("type_name")
        owner = meta.get("owner")
        desc = meta.get("description")

        for seg_idx, seg in enumerate(segments, 1):
            for pt_idx, hit in enumerate(seg, 1):
                try:
                    lat = float(hit.get("lat"))
                    lon = float(hit.get("lon"))
                except (TypeError, ValueError):
                    continue

                ac_data = hit.get("ac_data")
                if not isinstance(ac_data, dict):
                    ac_data = {}

                ac_json = json.dumps(ac_data, ensure_ascii=False) if ac_data else ""

                row = [
                    hex_code.upper(),
                    seg_idx,
                    pt_idx,
                    hit.get("timestamp") if hit.get("timestamp") is not None else "",
                    hit.get("time_iso") or "",
                    lat,
                    lon,
                    hit.get("alt_ft", ""),
                    hit.get("gs_knots", ""),
                    hit.get("track_deg", ""),
                    hit.get("vrt_fpm", ""),
                    hit.get("flags", ""),
                    reg or "",
                    atype or "",
                    tname or "",
                    owner or "",
                    desc or "",
                ]

                for k in ac_keys:
                    v = ac_data.get(k, "")
                    if isinstance(v, (dict, list)):
                        v = json.dumps(v, ensure_ascii=False)
                    row.append(v)

                row.append(ac_json)
                writer.writerow(row)


def build_json(
    segments: List[List[Dict[str, Any]]],
    hex_code: str,
    meta: Dict[str, Any],
    out_path: str,
) -> None:
    """
    Build JSON track structure:

    {
      "icao_hex": "...",
      "meta": {...},
      "segments": [
        {
          "segment": 1,
          "points": [ { hit dict }, ... ]
        },
        ...
      ]
    }
    """
    ensure_dir_for_file(out_path)
    data = {
        "icao_hex": hex_code.upper(),
        "meta": meta,
        "segments": [
            {"segment": i, "points": seg} for i, seg in enumerate(segments, 1)
        ],
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def fetch_trace_for_day(
    hex_code: str,
    day: dt.date,
    session: requests.Session,
    log_cb,
    cache_root: Optional[str] = None,
    retry_wait: int = 30,
) -> Optional[str]:
    """
    Download (or use cached) trace_full JSON for a single day.
    Returns path to JSON file or None.
    """
    suffix = hex_code[-2:]
    url = BASE.format(y=day.year, m=day.month, d=day.day, suffix=suffix, hex=hex_code)

    if cache_root:
        daily_dir = os.path.join(
            cache_root,
            hex_code,
            f"{day.year:04d}-{day.month:02d}-{day.day:02d}",
        )
        os.makedirs(daily_dir, exist_ok=True)
        json_path = os.path.join(daily_dir, "trace_full.json")
    else:
        json_path = os.path.abspath(f"trace_full_{hex_code}_{day}.json")

    if os.path.exists(json_path):
        log_cb(f"[cache] {day} already downloaded")
        return json_path

    log_cb(f"[fetch] {day} → {url}")
    time.sleep(2)  # be kind to ADSBx

    for attempt in range(6):
        try:
            r = session.get(url, headers=HEADERS, timeout=30)
        except Exception as e:
            log_cb(f"[error] {day}: request error {e}, retrying in 5s")
            time.sleep(5)
            continue

        if r.status_code == 200:
            with open(json_path, "wb") as f:
                f.write(r.content)
            log_cb(f"[ok] {day}: saved {len(r.content)} bytes")
            return json_path

        if r.status_code == 404:
            log_cb(f"[skip] {day}: 404 (no data)")
            return None

        if r.status_code == 429:
            log_cb(f"[rate] 429 on {day}, backing off {retry_wait}s")
            time.sleep(retry_wait)
            continue

        log_cb(f"[warn] {day}: HTTP {r.status_code}, retrying in 5s")
        time.sleep(5)

    log_cb(f"[fail] {day}: gave up after retries")
    return None


# -----------------------------
# Aircraft metadata helpers
# -----------------------------

@dataclass
class AircraftMeta:
    hex: str = ""
    registration: Optional[str] = None
    type: Optional[str] = None          # ICAO type designator, e.g. GLF4
    type_name: Optional[str] = None     # Friendly name, e.g. Gulfstream IV / G-IV
    owner: Optional[str] = None
    manufacturer: Optional[str] = None
    model: Optional[str] = None
    photo_url: Optional[str] = None
    country: Optional[str] = None
    flags: Optional[str] = None         # e.g. "Military, LADD"
    callsigns: Optional[List[str]] = None
    description: Optional[str] = None
    raw_record: Optional[dict] = None


def apply_type_mapping(meta: AircraftMeta):
    """
    Normalize meta.type as ICAO code and fill meta.type_name
    from ICAO_TYPE_NAMES or manufacturer/model/description.
    """
    if meta.type:
        code = meta.type.strip().upper()
        meta.type = code
        if not meta.type_name:
            common = ICAO_TYPE_NAMES.get(code)
            if common:
                meta.type_name = common

    if not meta.type_name:
        if meta.manufacturer or meta.model:
            meta.type_name = " ".join(
                [x for x in (meta.manufacturer, meta.model) if x]
            )
        elif meta.description:
            meta.type_name = meta.description


def fetch_opensky_metadata(icao_hex: str, timeout: int = 15) -> AircraftMeta:
    """
    Query OpenSky aircraft metadata API for registration/manufacturer/model/owner.
    Always returns an AircraftMeta (never None).
    """
    meta = AircraftMeta(hex=icao_hex)
    try:
        url = f"https://opensky-network.org/api/metadata/aircraft/icao24/{icao_hex.lower()}"
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200:
            data = r.json() or {}
            meta.registration = data.get("registration") or None
            meta.manufacturer = data.get("manufacturerName") or None
            meta.model = data.get("model") or None
            meta.owner = data.get("owner") or None
            if meta.manufacturer and meta.model:
                meta.type = data.get("icaoType") or f"{meta.manufacturer} {meta.model}"
            elif data.get("icaoType"):
                meta.type = data.get("icaoType")
            elif meta.model:
                meta.type = meta.model
    except Exception:
        pass
    return meta


def fetch_planespotters_photo_and_reg(icao_hex: str, timeout: int = 15) -> (Optional[str], Optional[str]):
    """Query Planespotters public API for a representative photo and (maybe) registration."""
    try:
        url = f"https://api.planespotters.net/pub/photos/hex/{icao_hex.lower()}"
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200:
            data = r.json() or {}
            photos = data.get("photos") or []
            if photos:
                ph = photos[0]
                thumbnails = ph.get("thumbnail") or {}
                large = thumbnails.get("large") or thumbnails.get("src")
                reg = (ph.get("registration") or ph.get("reg")) or None
                return large, reg
    except Exception:
        pass
    return None, None


def load_adsbx_acdb(cache_root: str, log_cb) -> Any:
    """
    Load the ADSBexchange basic aircraft DB from cache or download it.
    If anything goes wrong, log and return {} (non-fatal).
    """
    global _ACDB_CACHE
    if _ACDB_CACHE is not None:
        return _ACDB_CACHE

    try:
        os.makedirs(cache_root, exist_ok=True)
        gz_path = os.path.join(cache_root, "basic-ac-db.json.gz")
        json_path = os.path.join(cache_root, "basic-ac-db.json")

        # Prefer plain JSON cache if present
        if os.path.exists(json_path):
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    db = json.load(f)
                log_cb("[acdb] Loaded cached aircraft DB JSON")
                _ACDB_CACHE = db
                return db
            except Exception as e:
                log_cb(f"[acdb] Failed to read cached JSON ({e}), will re-download")

        # If gz already present, try that
        if os.path.exists(gz_path):
            log_cb("[acdb] Using cached gzipped DB")
            with open(gz_path, "rb") as f:
                content = f.read()
        else:
            log_cb("[acdb] Downloading aircraft database from ADSBexchange…")
            r = requests.get(AC_DB_URL, timeout=120)
            r.raise_for_status()
            content = r.content
            with open(gz_path, "wb") as f:
                f.write(content)

        try:
            decompressed = gzip.decompress(content).decode("utf-8", errors="ignore")
        except Exception:
            decompressed = content.decode("utf-8", errors="ignore")

        try:
            db = json.loads(decompressed)
        except Exception as e:
            log_cb(f"[acdb] JSON decode failed ({e}); ignoring DB.")
            _ACDB_CACHE = {}
            return _ACDB_CACHE

        _ACDB_CACHE = db

        # Also write plain JSON cache
        try:
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(db, f)
            log_cb("[acdb] Saved decompressed DB to JSON cache")
        except Exception as e:
            log_cb(f"[acdb] Warning: failed to write JSON cache ({e})")

        return db

    except Exception as e:
        log_cb(f"[acdb] Fatal error loading DB ({e}); continuing without DB.")
        _ACDB_CACHE = {}
        return _ACDB_CACHE


def find_acdb_record(db: Any, icao_hex: str) -> Optional[dict]:
    """Look up a single ICAO hex record in ADSBx basic aircraft DB."""
    icao_hex = icao_hex.lower()
    if not db:
        return None

    if isinstance(db, dict):
        iterable = db.values()
    else:
        iterable = db

    for rec in iterable:
        if not isinstance(rec, dict):
            continue
        icao_field = (
            rec.get("ICAO")
            or rec.get("icao")
            or rec.get("icao24")
            or rec.get("ICAO24")
            or rec.get("hex")
        )
        if not isinstance(icao_field, str):
            continue
        if icao_field.lower() == icao_hex:
            return rec
    return None


def flags_from_dbflags(dbflags: Any) -> Optional[str]:
    """
    Decode ADSBx dbFlags bitfield if available.
    bit 1: military
    bit 2: interesting
    bit 4: PIA
    bit 8: LADD
    """
    try:
        v = int(dbflags)
    except Exception:
        return None
    flags = []
    if v & 1:
        flags.append("Military")
    if v & 2:
        flags.append("Interesting")
    if v & 4:
        flags.append("PIA")
    if v & 8:
        flags.append("LADD")
    return ", ".join(flags) if flags else None


def merge_adsbx_record_into_meta(rec: dict, meta: AircraftMeta):
    """Merge ADSBx DB record into AircraftMeta (type, reg, owner, flags, etc.)."""

    def get_any_ci(*keys):
        if not isinstance(rec, dict):
            return None
        lower_map = {k.lower(): k for k in rec.keys()}
        for k in keys:
            real_key = lower_map.get(k.lower())
            if real_key is not None:
                v = rec.get(real_key)
                if v not in (None, ""):
                    return str(v)
        return None

    reg = get_any_ci("REG", "reg", "r", "registration", "tail")
    if reg and not meta.registration:
        meta.registration = reg

    icao_type = get_any_ci("ICAOTYPE", "icaoType", "t", "type")
    man = get_any_ci("Manufacturer", "manufacturer", "man", "mfr")
    mdl = get_any_ci("Model", "model", "mdl")

    if man and not meta.manufacturer:
        meta.manufacturer = man
    if mdl and not meta.model:
        meta.model = mdl

    if not meta.type and icao_type:
        meta.type = icao_type
    elif not meta.type and (man or mdl):
        meta.type = " ".join([x for x in (man, mdl) if x])

    owner = get_any_ci(
        "OWNOP",
        "Owner",
        "owner",
        "OWN",
        "operator",
        "Operator",
        "op",
        "ownercode",
        "opicao",
    )
    if owner and not meta.owner:
        meta.owner = owner

    country = get_any_ci("Country", "country", "Cou", "COU")
    if country:
        meta.country = country

    dbf = get_any_ci("dbFlags", "DBFLAGS")
    f = flags_from_dbflags(dbf) if dbf is not None else None
    if f and not meta.flags:
        meta.flags = f

    meta.raw_record = rec


def merge_trace_blob_into_meta(blob: Any, meta: AircraftMeta):
    """
    Merge top-level metadata present in trace_full blob:
      - r / reg: registration
      - t / type: aircraft type
      - desc / description
      - op / owner / operator
      - dbFlags (decode to flags)
      - flight / call / callsign (single callsign)
    """
    if not isinstance(blob, dict):
        return

    lower_map = {k.lower(): k for k in blob.keys()}

    def get_ci(*keys):
        for k in keys:
            real = lower_map.get(k.lower())
            if real is not None:
                v = blob.get(real)
                if isinstance(v, str) and v.strip():
                    return v.strip()
        return None

    if not meta.registration:
        reg = get_ci("r", "reg", "registration", "tail")
        if reg:
            meta.registration = reg

    if not meta.type:
        t = get_ci("t", "type", "icaoType", "icao_type")
        if t:
            meta.type = t

    if not meta.description:
        desc = get_ci("desc", "description")
        if desc:
            meta.description = desc

    if not meta.owner:
        owner = get_ci("owner", "Owner", "op", "operator", "OWNOP")
        if owner:
            meta.owner = owner

    if not meta.flags:
        dbf_key = lower_map.get("dbflags")
        dbf = blob.get(dbf_key) if dbf_key else None
        f = flags_from_dbflags(dbf)
        if f:
            meta.flags = f

    cs = get_ci("flight", "call", "callsign", "cs")
    if cs:
        if meta.callsigns is None:
            meta.callsigns = [cs]
        elif cs not in meta.callsigns:
            meta.callsigns.append(cs)


def enrich_meta_from_hits(meta: AircraftMeta, segments: List[List[Dict[str, Any]]]):
    """
    Look into ac_data inside hits to extract:
      - registration
      - type
      - owner
      - flags (military etc.)
      - callsigns
    """
    callsigns: Set[str] = set(meta.callsigns or [])

    for seg in segments:
        for hit in seg:
            ac = hit.get("ac_data")
            if not isinstance(ac, dict):
                continue

            lower_map = {k.lower(): k for k in ac.keys()}

            def get_ci(*keys):
                for k in keys:
                    real = lower_map.get(k.lower())
                    if real is not None:
                        v = ac.get(real)
                        if isinstance(v, str) and v.strip():
                            return v.strip()
                return None

            if not meta.registration:
                reg = get_ci("r", "reg", "registration", "tail", "tailnum", "tail_num")
                if reg:
                    meta.registration = reg

            if not meta.type:
                t = get_ci("t", "type", "icaoType", "icao_type")
                if t:
                    meta.type = t

            if not meta.owner:
                owner = get_ci("owner", "Owner", "op", "operator", "OWNOP")
                if owner:
                    meta.owner = owner

            if not meta.flags:
                dbf_key = lower_map.get("dbflags")
                dbf = ac.get(dbf_key) if dbf_key else None
                f = flags_from_dbflags(dbf) if dbf is not None else None
                if not f:
                    mil = get_ci("MIL", "mil", "military")
                    if mil:
                        f = f"Military={mil}"
                if f:
                    meta.flags = f

            for k in ("call", "callsign", "cs", "flight"):
                real = lower_map.get(k.lower())
                if real is None:
                    continue
                v = ac.get(real)
                if isinstance(v, str):
                    v = v.strip()
                    if v:
                        callsigns.add(v)

    if callsigns:
        meta.callsigns = sorted(callsigns)


# -----------------------------
# Worker thread
# -----------------------------

class Worker(QtCore.QThread):
    progress = QtCore.pyqtSignal(str)
    card_update = QtCore.pyqtSignal(object)  # AircraftMeta
    finished_ok = QtCore.pyqtSignal()
    finished_err = QtCore.pyqtSignal(str)

    def __init__(
        self,
        icao_hex: str,
        start_date: dt.date,
        end_date: dt.date,
        do_kml: bool,
        do_csv: bool,
        do_json: bool,
        out_dir: str,
    ):
        super().__init__()
        self.icao_hex = icao_hex.lower()
        self.start_date = start_date
        self.end_date = end_date
        self.do_kml = do_kml
        self.do_csv = do_csv
        self.do_json = do_json
        self.out_dir = out_dir
        self._stop = False

    def stop(self):
        self._stop = True

    def log(self, msg: str):
        self.progress.emit(msg)

    def run(self):
        try:
            # 1) Start with just hex
            meta_obj = AircraftMeta(hex=self.icao_hex)

            # 2) External metadata (best effort, non-fatal)
            try:
                os_meta = fetch_opensky_metadata(self.icao_hex)
                for field in ("registration", "manufacturer", "model", "owner", "type"):
                    v = getattr(os_meta, field, None)
                    if v and not getattr(meta_obj, field):
                        setattr(meta_obj, field, v)
            except Exception as e:
                self.log(f"[meta] OpenSky error: {e}")

            try:
                photo_url, reg2 = fetch_planespotters_photo_and_reg(self.icao_hex)
                if photo_url:
                    meta_obj.photo_url = photo_url
                if reg2 and not meta_obj.registration:
                    meta_obj.registration = reg2
            except Exception as e:
                self.log(f"[meta] Planespotters error: {e}")

            try:
                acdb_root = os.path.join(self.out_dir, "acdb_cache")
                db = load_adsbx_acdb(acdb_root, self.log)
                rec = find_acdb_record(db, self.icao_hex)
                if rec:
                    self.log("[acdb] Found record in ADSBx DB")
                    merge_adsbx_record_into_meta(rec, meta_obj)
                else:
                    self.log("[acdb] No record found in ADSBx DB")
            except Exception as e:
                self.log(f"[acdb] Error loading/merging DB: {e}")

            # Normalize type code & friendly name
            apply_type_mapping(meta_obj)

            # Emit initial card
            self.card_update.emit(meta_obj)

            session = requests.Session()
            all_segments: List[List[Dict[str, Any]]] = []

            # 3) Fetch all days
            for day in daterange(self.start_date, self.end_date):
                if self._stop:
                    self.log("[stop] Stopping as requested.")
                    self.finished_err.emit("Stopped")
                    return

                self.log(f"[day] {day}")
                cache_root = os.path.join(self.out_dir, "cache")
                path = fetch_trace_for_day(
                    self.icao_hex, day, session, self.log, cache_root=cache_root
                )
                if not path:
                    continue

                try:
                    with open(path, "rb") as f:
                        raw = f.read()
                    try:
                        blob = json.loads(raw)
                    except json.JSONDecodeError:
                        blob = json.loads(gzip.GzipFile(fileobj=io.BytesIO(raw)).read())
                except Exception as e:
                    self.log(f"[error] parse {day}: {e}")
                    continue

                merge_trace_blob_into_meta(blob, meta_obj)
                apply_type_mapping(meta_obj)
                self.card_update.emit(meta_obj)

                segments = extract_hits(blob)
                if segments:
                    all_segments.extend(segments)
                    total_pts = sum(len(seg) for seg in segments)
                    self.log(f"[points] {day}: {total_pts} points in {len(segments)} segment(s)")
                else:
                    self.log(f"[points] {day}: no valid points found")

            if not all_segments:
                self.log("No points parsed; nothing to write.")
                self.finished_err.emit("No data")
                return

            # 4) Enrich metadata from hits and callsigns
            enrich_meta_from_hits(meta_obj, all_segments)
            apply_type_mapping(meta_obj)
            self.card_update.emit(meta_obj)

            # 5) Build meta dict for exporters
            meta = {
                "icao": meta_obj.hex.lower(),
                "registration": meta_obj.registration,
                "type": meta_obj.type,            # ICAO designator
                "type_name": meta_obj.type_name,  # friendly name
                "owner": meta_obj.owner,
                "manufacturer": meta_obj.manufacturer,
                "model": meta_obj.model,
                "description": meta_obj.description,
            }

            start_str = self.start_date.strftime("%Y%m%d")
            end_str = self.end_date.strftime("%Y%m%d")
            base_root = os.path.join(
                self.out_dir, f"{self.icao_hex.upper()}_{start_str}_{end_str}"
            )

            try:
                os.makedirs(self.out_dir, exist_ok=True)
            except Exception:
                pass

            total_points = sum(len(seg) for seg in all_segments)
            self.log(f"[stats] Total points across all days: {total_points}")

            if self.do_kml:
                kml_path = base_root + ".kml"
                self.log(f"[kml] Building time-enabled KML at {kml_path}…")
                try:
                    build_kml(all_segments, self.icao_hex, meta, kml_path)
                    self.log(f"[kml] Wrote {kml_path}")
                except Exception as e:
                    self.log(f"[error] KML: {e}")

            if self.do_csv:
                csv_path = base_root + ".csv"
                self.log(f"[csv] Building CSV at {csv_path}…")
                try:
                    build_csv(all_segments, self.icao_hex, meta, csv_path)
                    self.log(f"[csv] Wrote {csv_path}")
                except Exception as e:
                    self.log(f"[error] CSV: {e}")

            if self.do_json:
                json_path = base_root + ".json"
                self.log(f"[json] Building JSON at {json_path}…")
                try:
                    build_json(all_segments, self.icao_hex, meta, json_path)
                    self.log(f"[json] Wrote {json_path}")
                except Exception as e:
                    self.log(f"[error] JSON: {e}")

            self.finished_ok.emit()

        except Exception as e:
            self.log(f"[fatal] {e}")
            self.finished_err.emit(str(e))


# -----------------------------
# UI widgets
# -----------------------------

class ToggleCheckBox(QtWidgets.QCheckBox):
    """Checkbox that turns green when checked."""

    def __init__(self, text=""):
        super().__init__(text)
        self.stateChanged.connect(self._restyle)
        self._restyle()

    def _restyle(self):
        if self.isChecked():
            self.setStyleSheet(
                "QCheckBox { background-color: #1fab4a; color: white; "
                "padding:4px; border-radius:6px;}"
            )
        else:
            self.setStyleSheet(
                "QCheckBox { background-color: none; color: none; padding:4px; }"
            )


class ImageLabel(QtWidgets.QLabel):
    def set_remote_image(self, url: Optional[str]):
        if not url:
            self.setText("No image available")
            self.setAlignment(QtCore.Qt.AlignCenter)
            return
        try:
            r = requests.get(url, timeout=20)
            if r.status_code == 200:
                pix = QtGui.QPixmap()
                pix.loadFromData(r.content)
                if not pix.isNull():
                    self.setPixmap(
                        pix.scaled(
                            420,
                            280,
                            QtCore.Qt.KeepAspectRatio,
                            QtCore.Qt.SmoothTransformation,
                        )
                    )
                    self.setAlignment(QtCore.Qt.AlignCenter)
                    return
        except Exception:
            pass
        self.setText("Image load failed")
        self.setAlignment(QtCore.Qt.AlignCenter)


# -----------------------------
# Main window
# -----------------------------

class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("ADSBx Flight History")
        self.resize(1100, 700)

        central = QtWidgets.QWidget()
        self.setCentralWidget(central)
        vbox = QtWidgets.QVBoxLayout(central)

        splitter = QtWidgets.QSplitter(QtCore.Qt.Horizontal)

        # Left pane: inputs
        left = QtWidgets.QWidget()
        form = QtWidgets.QFormLayout(left)

        self.start_date = QtWidgets.QDateEdit()
        self.start_date.setCalendarPopup(True)
        self.start_date.setDate(QtCore.QDate.currentDate().addDays(-1))

        self.end_date = QtWidgets.QDateEdit()
        self.end_date.setCalendarPopup(True)
        self.end_date.setDate(QtCore.QDate.currentDate())

        self.hex_edit = QtWidgets.QLineEdit()
        self.hex_edit.setPlaceholderText("ICAO HEX (e.g., A1B2C3)")
        self.hex_edit.textChanged.connect(self._upper_hex)

        self.kml_chk = ToggleCheckBox("Export KML")
        self.csv_chk = ToggleCheckBox("Export CSV")
        self.json_chk = ToggleCheckBox("Export JSON")
        self.kml_chk.setChecked(True)
        self.csv_chk.setChecked(True)
        self.json_chk.setChecked(True)

        # Output folder row: path + Browse + Open
        out_box = QtWidgets.QHBoxLayout()
        self.out_edit = QtWidgets.QLineEdit()
        self.out_btn = QtWidgets.QPushButton("Browse…")
        self.out_btn.clicked.connect(self.choose_folder)
        self.out_open_btn = QtWidgets.QPushButton("Open")
        self.out_open_btn.clicked.connect(self.open_folder_in_explorer)
        out_box.addWidget(self.out_edit)
        out_box.addWidget(self.out_btn)
        out_box.addWidget(self.out_open_btn)
        out_wrap = QtWidgets.QWidget()
        out_wrap.setLayout(out_box)

        self.run_btn = QtWidgets.QPushButton("Run Query")
        self.stop_btn = QtWidgets.QPushButton("Stop Query")
        self.run_btn.clicked.connect(self.run_query)
        self.stop_btn.clicked.connect(self.stop_query)
        btns = QtWidgets.QHBoxLayout()
        btns.addWidget(self.run_btn)
        btns.addWidget(self.stop_btn)
        btn_wrap = QtWidgets.QWidget()
        btn_wrap.setLayout(btns)

        form.addRow("Start date:", self.start_date)
        form.addRow("Stop date:", self.end_date)
        form.addRow("ICAO HEX:", self.hex_edit)
        form.addRow(self.kml_chk)
        form.addRow(self.csv_chk)
        form.addRow(self.json_chk)
        form.addRow("Output folder:", out_wrap)
        form.addRow(btn_wrap)

        # Right pane: baseball card + photo
        right = QtWidgets.QWidget()
        rv = QtWidgets.QVBoxLayout(right)

        self.image = ImageLabel()
        self.image.setFixedSize(440, 300)
        self.image.setStyleSheet(
            "QLabel { background-color: #222; color: #bbb; border: 1px solid #444; }"
        )

        grid = QtWidgets.QGridLayout()
        lab_style = "QLabel { color: #ccc; }"
        val_style = "QLabel { color: #fff; font-weight: bold; }"

        self.lbl_type_code = QtWidgets.QLabel("-")
        self.lbl_type_code.setStyleSheet(val_style)
        self.lbl_type_name = QtWidgets.QLabel("-")
        self.lbl_type_name.setStyleSheet(val_style)
        self.lbl_type_name.setWordWrap(True)

        self.lbl_reg = QtWidgets.QLabel("-")
        self.lbl_reg.setStyleSheet(val_style)
        self.lbl_owner = QtWidgets.QLabel("-")
        self.lbl_owner.setStyleSheet(val_style)
        self.lbl_hex_lbl = QtWidgets.QLabel("-")
        self.lbl_hex_lbl.setStyleSheet(val_style)
        self.lbl_flags = QtWidgets.QLabel("-")
        self.lbl_flags.setStyleSheet(val_style)
        self.lbl_callsigns = QtWidgets.QLabel("-")
        self.lbl_callsigns.setStyleSheet(val_style)
        self.lbl_callsigns.setWordWrap(True)

        l0 = QtWidgets.QLabel("Aircraft type (ICAO):")
        l0.setStyleSheet(lab_style)
        l0b = QtWidgets.QLabel("Type name:")
        l0b.setStyleSheet(lab_style)
        l1 = QtWidgets.QLabel("Registration (tail #):")
        l1.setStyleSheet(lab_style)
        l2 = QtWidgets.QLabel("Registered owner:")
        l2.setStyleSheet(lab_style)
        l3 = QtWidgets.QLabel("ICAO Hex:")
        l3.setStyleSheet(lab_style)
        l4 = QtWidgets.QLabel("Flags:")
        l4.setStyleSheet(lab_style)
        l5 = QtWidgets.QLabel("Callsign(s):")
        l5.setStyleSheet(lab_style)

        grid.addWidget(l0, 0, 0)
        grid.addWidget(self.lbl_type_code, 0, 1)
        grid.addWidget(l0b, 1, 0)
        grid.addWidget(self.lbl_type_name, 1, 1)
        grid.addWidget(l1, 2, 0)
        grid.addWidget(self.lbl_reg, 2, 1)
        grid.addWidget(l2, 3, 0)
        grid.addWidget(self.lbl_owner, 3, 1)
        grid.addWidget(l3, 4, 0)
        grid.addWidget(self.lbl_hex_lbl, 4, 1)
        grid.addWidget(l4, 5, 0)
        grid.addWidget(self.lbl_flags, 5, 1)
        grid.addWidget(l5, 6, 0)
        grid.addWidget(self.lbl_callsigns, 6, 1)

        rv.addWidget(self.image)
        rv.addLayout(grid)
        rv.addStretch(1)

        splitter.addWidget(left)
        splitter.addWidget(right)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 1)

        vbox.addWidget(splitter)

        # Bottom: status/log
        self.log = QtWidgets.QPlainTextEdit()
        self.log.setReadOnly(True)
        self.log.setMaximumBlockCount(2000)
        self.log.setStyleSheet(
            "QPlainTextEdit { background:#111; color:#0f0; font-family: Consolas, monospace; }"
        )
        vbox.addWidget(self.log)

        self.worker: Optional[Worker] = None
        self.out_edit.setText(os.path.abspath("outputs"))
        self.status = QtWidgets.QStatusBar()
        self.setStatusBar(self.status)

    # --- UI helpers ---

    def _upper_hex(self, s: str):
        cur = s.upper()
        if cur != s:
            pos = self.hex_edit.cursorPosition()
            self.hex_edit.setText(cur)
            self.hex_edit.setCursorPosition(pos)

    def choose_folder(self):
        folder = QtWidgets.QFileDialog.getExistingDirectory(
            self,
            "Choose output folder",
            self.out_edit.text() or os.path.expanduser("~"),
        )
        if folder:
            self.out_edit.setText(folder)

    def open_folder_in_explorer(self):
        path = self.out_edit.text().strip()
        if not path:
            path = os.path.expanduser("~")
        if not os.path.isdir(path):
            QtWidgets.QMessageBox.warning(
                self, "Folder not found", f"Folder does not exist:\n{path}"
            )
            return
        if sys.platform.startswith("win"):
            os.startfile(path)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])

    def append_log(self, text: str):
        self.log.appendPlainText(text)
        self.status.showMessage(text, 5000)

    def on_card_update(self, meta: AircraftMeta):
        self.lbl_type_code.setText(meta.type or "-")
        self.lbl_type_name.setText(meta.type_name or "-")
        self.lbl_hex_lbl.setText(meta.hex.upper())
        self.lbl_reg.setText(meta.registration or "-")
        self.lbl_owner.setText(meta.owner or "-")
        self.lbl_flags.setText(meta.flags or "-")
        cs_text = ", ".join(meta.callsigns) if meta.callsigns else "-"
        self.lbl_callsigns.setText(cs_text)
        self.image.set_remote_image(meta.photo_url)

    # --- Run / Stop ---

    def run_query(self):
        if self.worker and self.worker.isRunning():
            self.append_log("[busy] A query is already running.")
            return

        hex_code = self.hex_edit.text().strip().upper()
        if not hex_code or any(c not in "0123456789ABCDEF" for c in hex_code):
            QtWidgets.QMessageBox.warning(
                self, "Invalid ICAO HEX", "Please enter a valid hex (0-9, A-F)."
            )
            return

        sdate = self.start_date.date().toPyDate()
        edate = self.end_date.date().toPyDate()
        if edate < sdate:
            sdate, edate = edate, sdate

        out_dir = self.out_edit.text().strip()
        if not out_dir:
            QtWidgets.QMessageBox.warning(
                self, "Output folder", "Please choose an output folder."
            )
            return

        do_kml = self.kml_chk.isChecked()
        do_csv = self.csv_chk.isChecked()
        do_json = self.json_chk.isChecked()
        if not any([do_kml, do_csv, do_json]):
            QtWidgets.QMessageBox.information(
                self, "Nothing to export", "Enable at least one export format."
            )
            return

        os.makedirs(out_dir, exist_ok=True)

        self.log.clear()
        self.append_log(
            f"[run] HEX={hex_code}, {sdate} → {edate}, out={out_dir}"
        )
        self.worker = Worker(hex_code, sdate, edate, do_kml, do_csv, do_json, out_dir)
        self.worker.progress.connect(self.append_log)
        self.worker.card_update.connect(self.on_card_update)
        self.worker.finished_ok.connect(
            lambda: self.append_log("[done] All days processed (consolidated).")
        )
        self.worker.finished_err.connect(
            lambda m: self.append_log(f"[stopped] {m}")
        )
        self.worker.start()

    def stop_query(self):
        if self.worker and self.worker.isRunning():
            self.append_log("[req] Stop requested…")
            self.worker.stop()
        else:
            self.append_log("[idle] No running query.")


# -----------------------------
# Entrypoint
# -----------------------------

def main():
    app = QtWidgets.QApplication(sys.argv)
    app.setStyle("Fusion")
    # Dark palette
    palette = QtGui.QPalette()
    palette.setColor(QtGui.QPalette.Window, QtGui.QColor(30, 30, 30))
    palette.setColor(QtGui.QPalette.WindowText, QtCore.Qt.white)
    palette.setColor(QtGui.QPalette.Base, QtGui.QColor(18, 18, 18))
    palette.setColor(QtGui.QPalette.AlternateBase, QtGui.QColor(45, 45, 45))
    palette.setColor(QtGui.QPalette.ToolTipBase, QtCore.Qt.white)
    palette.setColor(QtGui.QPalette.ToolTipText, QtCore.Qt.white)
    palette.setColor(QtGui.QPalette.Text, QtCore.Qt.white)
    palette.setColor(QtGui.QPalette.Button, QtGui.QColor(45, 45, 45))
    palette.setColor(QtGui.QPalette.ButtonText, QtCore.Qt.white)
    palette.setColor(QtGui.QPalette.BrightText, QtGui.QColor(255, 80, 80))
    palette.setColor(QtGui.QPalette.Highlight, QtGui.QColor(38, 79, 120))
    palette.setColor(QtGui.QPalette.HighlightedText, QtCore.Qt.white)
    app.setPalette(palette)

    w = MainWindow()
    w.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
