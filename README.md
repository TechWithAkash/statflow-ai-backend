# Backend – StatFlow AI Prototype

FastAPI service providing:
- /health: healthcheck
- /ingest (POST multipart/form-data file): stream upload CSV, run schema detection, imputation, outlier detection, compute quality score, and emit WebSocket progress at ws://localhost:8000/ws/progress/{dataset_id}

Run locally:
1) Create venv and install requirements.txt
2) python -m uvicorn app:app --reload
