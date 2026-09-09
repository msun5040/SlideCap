# slidecap

A desktop application for organizing and searching pathology slides with built-in analysis pipeline capabilities.

## Quick Start

### 1. Set Up Test Environment

First, create a test directory with sample slide files:

```bash
# Create a test directory (adjust path as needed)
python scripts/setup_test_dir.py ~/slide-test

# Or with more slides
python scripts/setup_test_dir.py ~/slide-test 50
```

### 2. Configure the Backend

Edit `backend/app/config.py` and update `NETWORK_ROOT`:

```python
NETWORK_ROOT: str = "/Users/yourname/slide-test"  # macOS
# or
NETWORK_ROOT: str = "C:/slide-test"               # Windows
# or  
NETWORK_ROOT: str = "/home/yourname/slide-test"   # Linux
```

Or set via environment variable:
```bash
export NETWORK_ROOT=/path/to/your/slides
```

### 3. Install Dependencies

```bash
cd backend
pip install -r requirements.txt
```

### 4. Run the API Server

```bash
cd backend
python -m uvicorn app.main:app --reload
```

### 5. Test the API

Open http://localhost:8000/docs in your browser to see the interactive API documentation.

Or use curl:

```bash
# Check health
curl http://localhost:8000/health

# Run full index
curl -X POST http://localhost:8000/index/full

# Search for slides
curl "http://localhost:8000/search?q=S24"

# Get stats
curl http://localhost:8000/stats
```

## Project Structure

```
slidecap/
├── backend/
│   ├── app/
│   │   ├── main.py              # FastAPI application
│   │   ├── config.py            # Configuration
│   │   ├── api/                 # API route handlers
│   │   ├── db/
│   │   │   └── models.py        # Database models
│   │   └── services/
│   │       ├── hasher.py        # PHI-safe hashing
│   │       ├── filename_parser.py
│   │       └── indexer.py       # Slide indexing
│   └── requirements.txt
├── frontend/                    # Electron + React
├── scripts/
│   └── setup_test_dir.py        # Create test data
└── README.md
```

## Filename Convention

Slides should follow this naming pattern:
```
{accession}_{block}_{stain}_{randomId}.svs
```

Examples:
- `S24-12345_A1_HE_153452.svs`
- `S24-12345_A1_IHC-CD3_123545.svs`
- `S23-00042_B2_PAS_123554.svs`

## Directory Structure

Organize slides by year:
```
/network/slides/
├── 2024/
│   ├── S24-00001_A1_HE_865123.svs
│   └── ...
├── 2023/
│   └── ...
└── .slidecap/
    ├── .salt                    # Hashing salt (auto-generated)
    ├── database.sqlite          # Metadata database
    └── thumbnails/              # Cached thumbnails
```

## Privacy & Security

- **No PHI in database**: Accession numbers are hashed before storage
- **Salt stored on network drive**: Database alone cannot reveal patient IDs
- **Local processing**: No cloud uploads required

### HTTPS (needed for multi-user access)

Browsers treat a plain-http origin that isn't `localhost` as untrustworthy and
quietly withhold capabilities from it. Two bite us directly:

- Chromium **blocks `.zip` downloads** from such an origin. Chrome shows a
  "Keep" prompt; browsers that don't surface it (Arc, for one) leave a Data Pull
  export sitting at 100% forever, fully written but never finalized.
- `navigator.clipboard` is undefined, which is why copies go through
  `frontend/src/lib/clipboard.ts`.

So anyone reaching SlideCap over the network — i.e. every user but the one sat
at the server — should be served over https. Install
[mkcert](https://github.com/FiloSottile/mkcert), then:

```bash
scripts/make-certs.sh              # or scripts\make-certs.bat on Windows
export SSL_CERTFILE="$PWD/certs/slidecap.pem"
export SSL_KEYFILE="$PWD/certs/slidecap-key.pem"
```

Both halves read those two variables — `backend/run_server.py` passes them to
uvicorn, and `frontend/vite.config.js` serves the dev server over TLS. Start
them as usual and browse to `https://<hostname>:5173`. On Windows,
`scripts/run-dev-windows.bat` picks up `certs/` automatically if it exists.

#### Trusting the certificate on other machines

mkcert signs with a CA it invents on the server, so other machines don't know it
yet and will show a certificate warning until told to trust it. This is a
one-time, per-workstation step — do it yourself when setting a machine up
rather than asking users to:

```bash
mkcert -CAROOT                     # on the SERVER: prints the CA folder
```

Copy `rootCA.pem` from that folder to the workstation, then there:

```bash
mkcert -install                    # installs mkcert's CA into the OS trust store
# or, without installing mkcert:
#   Windows: certutil -addstore -f ROOT rootCA.pem   (as Administrator)
#   macOS:   sudo security add-trusted-cert -d -r trustRoot \
#              -k /Library/Keychains/System.keychain rootCA.pem
```

After that the machine gets a clean padlock and downloads work normally.

If you'd rather not visit each workstation, get the certificate from an
authority the machines already trust instead — your institution's internal CA
(already distributed via Group Policy on managed machines), or a public cert
from Let's Encrypt if the server has a real DNS name. Either drops in as
`SSL_CERTFILE`/`SSL_KEYFILE` with no code change and needs no per-machine setup.

`certs/` and `*.pem` are gitignored — never commit a private key.

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/stats` | Index statistics |
| POST | `/index/full` | Run full index |
| GET | `/search?q=...` | Search slides |
| GET | `/slides/{hash}` | Get slide details |
| GET | `/tags` | List all tags |
| POST | `/tags` | Create tag |
| POST | `/slides/{hash}/tags/{name}` | Add tag to slide |
| GET | `/projects` | List projects |
| POST | `/projects` | Create project |
| POST | `/projects/{id}/cases/{hash}` | Add case to project |
