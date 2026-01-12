# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=[('tailwick\\templates', 'tailwick\\templates'), ('tailwick\\static', 'tailwick\\static'), ('tailwick\\apps.py', 'tailwick'), ('tailwick\\utils.py', 'tailwick'), ('tailwick\\select_folder.py', 'tailwick'), ('tailwick\\models.py', 'tailwick'), ('tailwick\\__init__.py', 'tailwick'), ('tailwick\\pages.py', 'tailwick'), ('tailwick\\components.py', 'tailwick'), ('tailwick\\dashboard.py', 'tailwick'), ('tailwick\\landing.py', 'tailwick'), ('tailwick\\config.json', 'tailwick'), ('C:\\Users\\Admin\\AppData\\Local\\ms-playwright', 'ms-playwright')],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='main',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
