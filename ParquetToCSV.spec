# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec file for Parquet → CSV Converter

Build command:
    pyinstaller ParquetToCSV.spec

Output:
    dist/ParquetToCSV/ParquetToCSV.exe (onedir mode)
"""

import sys
from pathlib import Path

block_cipher = None

# Collect data files
datas = [
    ('streamlit_app.py', '.'),
    ('pages', 'pages'),
    ('app', 'app'),
    ('.streamlit/config.toml', '.streamlit'),
]

# Hidden imports for Streamlit and dependencies
hiddenimports = [
    # Streamlit core
    'streamlit',
    'streamlit.web.cli',
    'streamlit.web.server',
    'streamlit.web.bootstrap',
    'streamlit.runtime',
    'streamlit.runtime.scriptrunner',
    
    # PyArrow
    'pyarrow',
    'pyarrow.parquet',
    'pyarrow.csv',
    'pyarrow.lib',
    'pyarrow._parquet',
    
    # DuckDB
    'duckdb',
    
    # Common dependencies Streamlit needs
    'altair',
    'pandas',
    'numpy',
    'toml',
    'packaging',
    'watchdog',
    'validators',
    'gitpython',
    'pydeck',
    'pympler',
    'tornado',
    'click',
    'rich',
    'protobuf',
    'typing_extensions',
]

a = Analysis(
    ['run_app.py'],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Exclude unnecessary modules to reduce size
        'tkinter',
        'matplotlib',
        'scipy',
        'PIL',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='ParquetToCSV',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,  # Keep console for logs; set to False for no console window
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,  # Add icon path here if desired: icon='assets/icon.ico'
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='ParquetToCSV',
)
