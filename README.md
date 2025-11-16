# ADS-B History Tracker GUI

ADS-B History Tracker is a cross-platform desktop application for retrieving, analyzing, enriching, and exporting historical ADS-B flight data from ADS-B Exchange. The tool provides a PyQt5-based graphical interface, multi-day data collection, metadata enrichment, and multi-format exports including KML, CSV, and JSON.

This project is intended for researchers, analysts, and OSINT practitioners who require a structured and repeatable workflow for gathering flight history.

------------------------------------------------------------

## Overview

The program retrieves "trace_full" historical ADS-B data from ADSBexchange's Globe History system and processes it into structured segments and points. Metadata is merged from OpenSky Network, ADS-B Exchange Basic Aircraft DB, and the Planespotters Photo API. A GUI allows users to specify date ranges, export formats, the target ICAO hex, and the output directory. The results can be exported into time-enabled KML, flattened CSV, and full JSON.

The right-hand "baseball card" panel shows aircraft type, mapped common name, owner, registration, flags (military, LADD, etc.), and any callsigns detected in the trace. A representative aircraft image is displayed if found.

------------------------------------------------------------

## Features

1. Historical Data Retrieval
   - Multi-day retrieval from ADS-B Exchange.
   - Automatic caching and retry logic.
   - Handles rate limiting and missing data gracefully.

2. Metadata Enrichment
   - Integration with ADS-B Exchange Basic Aircraft DB.
   - Integration with OpenSky metadata API.
   - Integration with Planespotters API for aircraft photos.
   - Normalization of ICAO type codes and mapping to common aircraft names.
   - Extraction of all callsigns observed in trace hits.

3. Export Formats
   - KML: Time-enabled playback for Google Earth, with ExtendedData attributes for every available field.
   - CSV: One row per point with flattened AC data fields.
   - JSON: Full structured export including metadata, segments, and all hits.

4. GUI Capabilities
   - Dark-mode PyQt5 interface.
   - Start and stop date pickers.
   - ICAO hex input field.
   - Export toggles for KML, CSV, JSON.
   - Output folder picker with “Open Folder” button.
   - Real-time verbose status log.
   - Baseball card panel showing aircraft data and image.

5. Cross-platform
   - Works on Windows, macOS, and Linux.
   - Can be packaged into standalone executables.

------------------------------------------------------------

## Installation

### Clone the repository
    git clone https://github.com/<yourusername>/adsb-history-tracker.git
    cd adsb-history-tracker

### Create a virtual environment
    python3 -m venv venv

### Activate the environment
macOS/Linux:
    source venv/bin/activate

Windows:
    venv\Scripts\activate

### Install requirements
    pip install --upgrade pip
    pip install -r requirements.txt

------------------------------------------------------------

## Running the Application

    python3 adsb_gui.py

Windows:
    python adsb_gui.py

------------------------------------------------------------

## Building Executables

### Windows (.exe) using PyInstaller
    pyinstaller --onefile --windowed --icon=adsbtrack.ico adsb_gui.py

Output:
    dist/ADSBTracker.exe

### macOS (.app) using PyInstaller
    pyinstaller --onefile --windowed --icon=adsbtrack.icns adsb_gui.py

Output:
    dist/ADSBTracker.app

macOS Gatekeeper may block unsigned apps; remove quarantine:
    xattr -dr com.apple.quarantine dist/ADSBTracker.app

### macOS alternative: py2app
Create setup.py:
    from setuptools import setup

    APP = ['adsb_gui.py']
    OPTIONS = {
        'argv_emulation': True,
        'iconfile': 'adsbtrack.icns',
        'packages': ['requests', 'simplekml', 'PyQt5', 'PIL'],
    }

    setup(
        app=APP,
        options={'py2app': OPTIONS},
        setup_requires=['py2app'],
    )

Build:
    python3 setup.py py2app

------------------------------------------------------------

## Data Sources

The application uses:
- ADS-B Exchange Globe History (trace_full).
- ADS-B Exchange Basic Aircraft DB.
- OpenSky Metadata API.
- Planespotters Photo API.

Extracted data includes:
- Coordinates and timestamps.
- Altitude, groundspeed, track, vertical rate.
- AC data fields (squawk, NIC, NACP, SDA, SIL, emergency, category, etc.).
- Registration, type, mapped type name, owner/operator.
- Callsigns observed during the track.

------------------------------------------------------------

## Export Details

### KML Export
- Time-enabled playback in Google Earth.
- Static per-segment LineStrings.
- Per-point placemarks with TimeStamp.
- ExtendedData containing:
  - metadata fields,
  - hit fields,
  - flattened AC data dictionary.

### CSV Export
- One row per point.
- Consistent ordering of AC data keys as columns.
- Human-readable and GIS-compatible.

### JSON Export
- Full structured dataset of:
  - metadata,
  - segments,
  - all hits,
  - all AC data fields.

------------------------------------------------------------

## Troubleshooting

### Missing Python Modules
If you see “ModuleNotFoundError: No module named 'requests'”:
1. Ensure the venv is active:
       source venv/bin/activate
2. Reinstall:
       pip install -r requirements.txt

### macOS: Application Blocked
Run:
    xattr -dr com.apple.quarantine dist/ADSBTracker.app

### ADSBexchange 429 Errors
The program automatically backs off and retries. Use conservative date ranges.

------------------------------------------------------------

## Project Structure

    adsb-history-tracker/
    ├── adsb_gui.py
    ├── adsbtrack.ico
    ├── adsbtrack.icns
    ├── requirements.txt
    ├── README.md
    └── outputs/

------------------------------------------------------------

## License

This project is provided under the MIT License. See the LICENSE file for details.

