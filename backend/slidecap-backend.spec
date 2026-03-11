# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec for SlideCap backend.
Bundles the FastAPI backend + all dependencies into a single directory.

Usage:
    cd backend
    pyinstaller slidecap-backend.spec --noconfirm
"""

from PyInstaller.utils.hooks import collect_all, collect_submodules

block_cipher = None

# Collect entire packages that PyInstaller struggles to detect
packages_to_collect = [
    'uvicorn',
    'fastapi',
    'starlette',
    'pydantic',
    'pydantic_settings',
    'pydantic_core',
    'sqlalchemy',
    'dotenv',
    'anyio',
    'sniffio',
    'httptools',
    'watchfiles',
    'websockets',
    'click',
    'h11',
    'typing_extensions',
    'annotated_types',
    'idna',
    'openslide',
    'openslide_bin',
    'paramiko',
]

all_datas = []
all_binaries = []
all_hiddenimports = []

for pkg in packages_to_collect:
    try:
        datas, binaries, hiddenimports = collect_all(pkg)
        all_datas.extend(datas)
        all_binaries.extend(binaries)
        all_hiddenimports.extend(hiddenimports)
    except Exception as e:
        print(f"Warning: could not collect {pkg}: {e}")

# Also collect multipart (python-multipart for FastAPI form handling)
try:
    d, b, h = collect_all('multipart')
    all_datas.extend(d)
    all_binaries.extend(b)
    all_hiddenimports.extend(h)
except:
    pass

a = Analysis(
    ['run_server.py'],
    pathex=['.'],
    binaries=all_binaries,
    datas=[
        ('app', 'app'),
    ] + all_datas,
    hiddenimports=all_hiddenimports + [
        'uvicorn.logging',
        'uvicorn.loops',
        'uvicorn.loops.auto',
        'uvicorn.protocols',
        'uvicorn.protocols.http',
        'uvicorn.protocols.http.auto',
        'uvicorn.protocols.http.h11_impl',
        'uvicorn.protocols.http.httptools_impl',
        'uvicorn.protocols.websockets',
        'uvicorn.protocols.websockets.auto',
        'uvicorn.lifespan',
        'uvicorn.lifespan.on',
        'uvicorn.lifespan.off',
        'email_validator',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
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
    name='slidecap-backend',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='slidecap-backend',
)
