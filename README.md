# slidecap

A desktop application for organizing and searching pathology slides with AI analysis capabilities.

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
