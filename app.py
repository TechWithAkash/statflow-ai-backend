# # backend/app.py
# from __future__ import annotations

# import asyncio
# import csv
# import io
# import os
# import time
# import uuid
# from typing import Any, Dict, List, Optional

# import orjson
# import pandas as pd
# from fastapi import FastAPI, File, UploadFile, WebSocket, WebSocketDisconnect, HTTPException
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import JSONResponse
# from pydantic import BaseModel
# from starlette.websockets import WebSocketState

# # Basic storage paths
# BASE_DIR = os.path.dirname(__file__)
# STORAGE_DIR = os.path.join(BASE_DIR, "storage")
# DB_PATH = os.path.join(BASE_DIR, "statflow.db")
# os.makedirs(STORAGE_DIR, exist_ok=True)

# app = FastAPI(title="StatFlow AI Prototype", version="0.1")

# # Configure CORS for production via env, default to permissive for dev
# # Comma-separated exact origins or a regex via ALLOW_ORIGIN_REGEX
# origins_env = os.getenv("ALLOW_ORIGINS")  # comma-separated
# origin_regex = os.getenv("ALLOW_ORIGIN_REGEX")  # optional regex
# allow_origins = [o.strip() for o in origins_env.split(",")] if origins_env else (["*"] if not origin_regex else [])
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=allow_origins,
#     allow_origin_regex=origin_regex,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # In-memory progress state for demo
# progress_streams: Dict[str, Dict[str, Any]] = {}
# websocket_clients: Dict[str, List[WebSocket]] = {}


# class ProcessSummary(BaseModel):
#     dataset_id: str
#     row_count: int
#     column_count: int
#     missing_cells: int
#     missing_rate: float
#     imputations_applied: int
#     outliers_detected: int
#     quality_score: float
#     weighting: Dict[str, Any]
#     inferred_schema: Dict[str, str]


# def infer_schema(df: pd.DataFrame) -> Dict[str, str]:
#     schema: Dict[str, str] = {}
#     for col in df.columns:
#         s = df[col]
#         if pd.api.types.is_numeric_dtype(s):
#             schema[col] = "numeric"
#         elif pd.api.types.is_datetime64_any_dtype(s):
#             schema[col] = "datetime"
#         else:
#             # try to coerce to datetime
#             try:
#                 pd.to_datetime(s, errors="raise")
#                 schema[col] = "datetime"
#             except Exception:
#                 schema[col] = "categorical"
#     return schema


# def simple_impute(df: pd.DataFrame, schema: Dict[str, str]) -> int:
#     imputations = 0
#     for col, typ in schema.items():
#         if typ == "numeric":
#             numeric = pd.to_numeric(df[col], errors="coerce")
#             mean_val = numeric.mean()
#             before = df[col].isna().sum()
#             df[col] = df[col].fillna(mean_val)
#             imputations += int(before)
#         else:
#             mode_val = df[col].mode(dropna=True)
#             mode_val = mode_val.iloc[0] if not mode_val.empty else "UNKNOWN"
#             before = df[col].isna().sum()
#             df[col] = df[col].fillna(mode_val)
#             imputations += int(before)
#     return imputations


# def detect_outliers(df: pd.DataFrame, schema: Dict[str, str]) -> int:
#     from sklearn.ensemble import IsolationForest

#     numeric_cols = [c for c, t in schema.items() if t == "numeric"]
#     if not numeric_cols:
#         return 0
#     x = df[numeric_cols].select_dtypes(include=["number"]).fillna(0)
#     if x.empty:
#         return 0
#     # Bound the sample size for speed
#     sample = x.sample(n=min(len(x), 5000), random_state=42) if len(x) > 5000 else x
#     model = IsolationForest(n_estimators=100, contamination="auto", random_state=42)
#     preds = model.fit_predict(sample)
#     # Map predictions back proportionally
#     outlier_rate = (preds == -1).mean() if len(preds) else 0.0
#     return int(round(outlier_rate * len(x)))


# def compute_quality_score(missing_rate: float, imputations: int, outliers: int, rows: int) -> float:
#     # Weighted components: missingness (50%), outliers (30%), stability (20%)
#     miss_component = max(0.0, 1.0 - missing_rate)  # higher is better
#     outlier_component = max(0.0, 1.0 - (outliers / max(rows, 1)))
#     stability_component = max(0.0, 1.0 - (imputations / max(rows, 1)))
#     score = 0.5 * miss_component + 0.3 * outlier_component + 0.2 * stability_component
#     return round(score * 100, 2)


# def quick_weighting(df: pd.DataFrame, schema: Dict[str, str]) -> Dict[str, Any]:
#     # Toy weighting: inverse response rate by a chosen domain (first categorical)
#     domain_col: Optional[str] = next((c for c, t in schema.items() if t == "categorical"), None)
#     if not domain_col:
#         return {"method": "uniform", "weights": 1.0, "note": "No categorical domain found"}
#     counts = df[domain_col].value_counts(dropna=False)
#     total = counts.sum()
#     weights = {str(k): float(total / v) for k, v in counts.items() if v > 0}
#     return {"method": "inverse-frequency", "domain": domain_col, "weights": weights}


# def guess_encoding(path: str) -> str:
#     """Try a few common encodings to avoid UnicodeDecodeError on CSVs.
#     Returns the first encoding that can read a small sample, else latin1.
#     """
#     candidates = ["utf-8", "utf-8-sig", "cp1252", "latin1"]
#     for enc in candidates:
#         try:
#             with open(path, "r", encoding=enc, errors="strict") as f:
#                 f.read(4096)
#             return enc
#         except UnicodeDecodeError:
#             continue
#         except Exception:
#             # If any other error occurs, keep trying next
#             continue
#     return "latin1"


# async def broadcast_progress(dataset_id: str, payload: Dict[str, Any]):
#     # Record last state
#     progress_streams[dataset_id] = payload
#     for ws in list(websocket_clients.get(dataset_id, [])):
#         if ws.application_state != WebSocketState.CONNECTED:
#             continue
#         try:
#             await ws.send_text(orjson.dumps(payload).decode("utf-8"))
#         except Exception:
#             try:
#                 await ws.close()
#             except Exception:
#                 pass
#             websocket_clients[dataset_id].remove(ws)


# # --- Minimal SQLite logging for demo auditability ---
# async def init_db():
#     import aiosqlite
#     async with aiosqlite.connect(DB_PATH) as db:
#         await db.execute(
#             """
#             CREATE TABLE IF NOT EXISTS datasets (
#                 dataset_id TEXT PRIMARY KEY,
#                 filename TEXT,
#                 row_count INTEGER,
#                 column_count INTEGER,
#                 missing_rate REAL,
#                 quality_score REAL,
#                 created_at TEXT
#             )
#             """
#         )
#         await db.execute(
#             """
#             CREATE TABLE IF NOT EXISTS audit_trail (
#                 id TEXT PRIMARY KEY,
#                 event_timestamp TEXT,
#                 operation_type TEXT,
#                 resource_type TEXT,
#                 resource_id TEXT,
#                 details TEXT
#             )
#             """
#         )
#         await db.commit()


# async def log_dataset(summary: ProcessSummary, filename: str):
#     import aiosqlite, datetime
#     async with aiosqlite.connect(DB_PATH) as db:
#         await db.execute(
#             "INSERT OR REPLACE INTO datasets(dataset_id, filename, row_count, column_count, missing_rate, quality_score, created_at) VALUES(?,?,?,?,?,?,?)",
#             (
#                 summary.dataset_id,
#                 filename,
#                 summary.row_count,
#                 summary.column_count,
#                 summary.missing_rate,
#                 summary.quality_score,
#                 datetime.datetime.utcnow().isoformat(),
#             ),
#         )
#         await db.commit()


# async def log_audit(operation_type: str, resource_type: str, resource_id: str, details: Dict[str, Any]):
#     import aiosqlite, datetime
#     async with aiosqlite.connect(DB_PATH) as db:
#         await db.execute(
#             "INSERT INTO audit_trail(id, event_timestamp, operation_type, resource_type, resource_id, details) VALUES(?,?,?,?,?,?)",
#             (
#                 str(uuid.uuid4()),
#                 datetime.datetime.utcnow().isoformat(),
#                 operation_type,
#                 resource_type,
#                 resource_id,
#                 orjson.dumps(details).decode("utf-8"),
#             ),
#         )
#         await db.commit()


# @app.on_event("startup")
# async def on_startup():
#     await init_db()


# @app.websocket("/ws/progress/{dataset_id}")
# async def ws_progress(websocket: WebSocket, dataset_id: str):
#     await websocket.accept()
#     websocket_clients.setdefault(dataset_id, []).append(websocket)
#     # Send last known state
#     if dataset_id in progress_streams:
#         await websocket.send_text(orjson.dumps(progress_streams[dataset_id]).decode("utf-8"))
#     try:
#         while True:
#             await websocket.receive_text()  # keep-alive; ignore content
#     except WebSocketDisconnect:
#         pass
#     finally:
#         if websocket in websocket_clients.get(dataset_id, []):
#             websocket_clients[dataset_id].remove(websocket)


# @app.get("/health")
# def health():
#     return {"status": "ok"}


# @app.get("/")
# def root():
#     return {
#         "name": "StatFlow AI Prototype API",
#         "version": app.version,
#         "endpoints": ["/ingest", "/ws/progress/{dataset_id}", "/datasets/{dataset_id}", "/health"],
#     }


# async def process_file(dataset_id: str, raw_path: str, filename: str):
#     start = time.time()
#     try:
#         row_count = 0
#         column_count = 0
#         missing_cells = 0
#         schema: Dict[str, str] = {}
#         chunks: List[pd.DataFrame] = []

#         # Determine loader by extension
#         _, ext = os.path.splitext(filename.lower())
#         if ext in ['.xlsx', '.xls']:
#             # For Excel, load whole file (chunked xl read is not straightforward)
#             if ext == '.xlsx':
#                 df_full = pd.read_excel(raw_path, engine='openpyxl')
#             else:  # .xls
#                 df_full = pd.read_excel(raw_path, engine='xlrd')
#             chunks_iter = [df_full]
#         else:
#             def read_csv_resilient(path: str):
#                 enc_candidates = [guess_encoding(path), 'utf-8-sig', 'cp1252', 'latin1']
#                 for enc_try in enc_candidates:
#                     try:
#                         # Sniff delimiter for this encoding
#                         try:
#                             with open(path, 'rb') as fb:
#                                 sample_bytes = fb.read(8192)
#                             sample_text = sample_bytes.decode(enc_try, errors='ignore')
#                             sep_try = csv.Sniffer().sniff(sample_text).delimiter
#                         except Exception:
#                             sep_try = ','
#                         # Prefer chunked read; fallback to full read if chunking fails
#                         try:
#                             for chunk_df in pd.read_csv(
#                                 path,
#                                 chunksize=20000,
#                                 encoding=enc_try,
#                                 sep=sep_try,
#                                 engine='python',  # robust to bad lines
#                                 on_bad_lines='skip',
#                             ):
#                                 yield chunk_df
#                             return
#                         except Exception:
#                             # Try full read fallback
#                             df_all = pd.read_csv(
#                                 path,
#                                 encoding=enc_try,
#                                 sep=sep_try,
#                                 engine='python',
#                                 on_bad_lines='skip',
#                             )
#                             yield df_all
#                             return
#                     except Exception:
#                         continue
#                 # Final fallback: latin1 + comma
#                 df_all = pd.read_csv(path, encoding='latin1', sep=',', engine='python', on_bad_lines='skip')
#                 yield df_all

#             chunks_iter = read_csv_resilient(raw_path)

#         for i, chunk in enumerate(chunks_iter):
#             if i == 0:
#                 schema = infer_schema(chunk)
#                 column_count = len(chunk.columns)
#             row_count += len(chunk)
#             missing_cells += int(chunk.isna().sum().sum())
#             chunks.append(chunk)
#             await broadcast_progress(dataset_id, {"stage": "processing", "rows": row_count})

#         df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
#         imputations_applied = simple_impute(df, schema) if not df.empty else 0
#         outliers_detected = detect_outliers(df, schema) if not df.empty else 0
#         missing_rate = (missing_cells / max(row_count * max(column_count, 1), 1)) if row_count else 0.0
#         quality_score = compute_quality_score(missing_rate, imputations_applied, outliers_detected, row_count)
#         weighting = quick_weighting(df, schema) if not df.empty else {"method": "uniform", "weights": 1.0}

#         summary = ProcessSummary(
#             dataset_id=dataset_id,
#             row_count=row_count,
#             column_count=column_count,
#             missing_cells=missing_cells,
#             missing_rate=round(missing_rate, 6),
#             imputations_applied=imputations_applied,
#             outliers_detected=outliers_detected,
#             quality_score=quality_score,
#             weighting=weighting,
#             inferred_schema=schema,
#         )

#         await broadcast_progress(dataset_id, {"stage": "done", "summary": summary.model_dump()})
#         await log_dataset(summary, filename)
#         await log_audit(
#             operation_type="ingest_complete",
#             resource_type="dataset",
#             resource_id=dataset_id,
#             details={"filename": filename, "elapsed_sec": round(time.time() - start, 3)},
#         )
#     except Exception as e:
#         await broadcast_progress(dataset_id, {"stage": "error", "message": str(e)})
#         await log_audit("ingest_error", "dataset", dataset_id, {"error": str(e)})


# @app.post("/ingest")
# async def ingest(file: UploadFile = File(...)):
#     dataset_id = str(uuid.uuid4())
#     raw_path = os.path.join(STORAGE_DIR, f"{dataset_id}_{file.filename}")
#     size_bytes = 0

#     try:
#         # Read in chunks for compatibility across Starlette versions
#         with open(raw_path, "wb") as f:
#             while True:
#                 chunk = await file.read(1024 * 1024)
#                 if not chunk:
#                     break
#                 size_bytes += len(chunk)
#                 f.write(chunk)
#                 if size_bytes % (2 * 1024 * 1024) == 0:
#                     # Throttle progress events a bit
#                     await broadcast_progress(dataset_id, {"stage": "upload", "bytes": size_bytes})
#         # Final upload progress emit
#         await broadcast_progress(dataset_id, {"stage": "upload", "bytes": size_bytes})
#     except Exception as e:
#         await broadcast_progress(dataset_id, {"stage": "error", "message": f"upload_failed: {e}"})
#         raise HTTPException(status_code=400, detail="Upload failed")

#     # Kick off processing in background
#     asyncio.create_task(process_file(dataset_id, raw_path, file.filename))
#     return JSONResponse({"ok": True, "dataset_id": dataset_id, "status": "accepted"})


# @app.get("/datasets/{dataset_id}")
# async def get_dataset(dataset_id: str):
#     import aiosqlite
#     async with aiosqlite.connect(DB_PATH) as db:
#         db.row_factory = aiosqlite.Row
#         async with db.execute("SELECT * FROM datasets WHERE dataset_id=?", (dataset_id,)) as cur:
#             row = await cur.fetchone()
#             if not row:
#                 raise HTTPException(status_code=404, detail="Not found")
#             return dict(row)


# if __name__ == "__main__":
#     import uvicorn

#     uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)



from __future__ import annotations

import asyncio
import csv
import io
import os
import time
import uuid
from typing import Any, Dict, List, Optional

import orjson
import pandas as pd
from fastapi import FastAPI, File, UploadFile, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.websockets import WebSocketState
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Basic storage paths
BASE_DIR = os.path.dirname(__file__)
STORAGE_DIR = os.path.join(BASE_DIR, "storage")
DB_PATH = os.path.join(BASE_DIR, "statflow.db")
os.makedirs(STORAGE_DIR, exist_ok=True)

app = FastAPI(title="StatFlow AI", version="1.0.0")

# Production-ready CORS configuration
def get_cors_origins():
    origins = []
    
    # Production origins
    if os.getenv("ALLOW_ORIGINS"):
        env_origins = os.getenv("ALLOW_ORIGINS").split(",")
        origins.extend([origin.strip() for origin in env_origins])
    
    # Default production origins
    origins.extend([
        "https://statflowai.vercel.app",
        "https://*.vercel.app"
    ])
    
    # Development origins
    environment = os.getenv("ENVIRONMENT", "development")
    if environment == "development":
        origins.extend([
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            "http://localhost:3000",
            "http://127.0.0.1:3000"
        ])
    
    return origins

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_cors_origins(),
    allow_origin_regex=os.getenv("ALLOW_ORIGIN_REGEX"),
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# In-memory progress state for demo
progress_streams: Dict[str, Dict[str, Any]] = {}
websocket_clients: Dict[str, List[WebSocket]] = {}

class ProcessSummary(BaseModel):
    dataset_id: str
    row_count: int
    column_count: int
    missing_cells: int
    missing_rate: float
    imputations_applied: int
    outliers_detected: int
    quality_score: float
    weighting: Dict[str, Any]
    inferred_schema: Dict[str, str]

def infer_schema(df: pd.DataFrame) -> Dict[str, str]:
    schema: Dict[str, str] = {}
    for col in df.columns:
        s = df[col]
        if pd.api.types.is_numeric_dtype(s):
            schema[col] = "numeric"
        elif pd.api.types.is_datetime64_any_dtype(s):
            schema[col] = "datetime"
        else:
            # try to coerce to datetime
            try:
                pd.to_datetime(s, errors="raise")
                schema[col] = "datetime"
            except Exception:
                schema[col] = "categorical"
    return schema

def simple_impute(df: pd.DataFrame, schema: Dict[str, str]) -> int:
    imputations = 0
    for col, typ in schema.items():
        if typ == "numeric":
            numeric = pd.to_numeric(df[col], errors="coerce")
            mean_val = numeric.mean()
            before = df[col].isna().sum()
            df[col] = df[col].fillna(mean_val)
            imputations += int(before)
        else:
            mode_val = df[col].mode(dropna=True)
            mode_val = mode_val.iloc[0] if not mode_val.empty else "UNKNOWN"
            before = df[col].isna().sum()
            df[col] = df[col].fillna(mode_val)
            imputations += int(before)
    return imputations

def detect_outliers(df: pd.DataFrame, schema: Dict[str, str]) -> int:
    try:
        from sklearn.ensemble import IsolationForest
        numeric_cols = [c for c, t in schema.items() if t == "numeric"]
        if not numeric_cols:
            return 0
        x = df[numeric_cols].select_dtypes(include=["number"]).fillna(0)
        if x.empty:
            return 0
        # Bound the sample size for speed
        sample = x.sample(n=min(len(x), 5000), random_state=42) if len(x) > 5000 else x
        model = IsolationForest(n_estimators=100, contamination="auto", random_state=42)
        preds = model.fit_predict(sample)
        # Map predictions back proportionally
        outlier_rate = (preds == -1).mean() if len(preds) else 0.0
        return int(round(outlier_rate * len(x)))
    except ImportError:
        # Fallback if sklearn not available
        return 0

def compute_quality_score(missing_rate: float, imputations: int, outliers: int, rows: int) -> float:
    # Weighted components: missingness (50%), outliers (30%), stability (20%)
    miss_component = max(0.0, 1.0 - missing_rate)  # higher is better
    outlier_component = max(0.0, 1.0 - (outliers / max(rows, 1)))
    stability_component = max(0.0, 1.0 - (imputations / max(rows, 1)))
    score = 0.5 * miss_component + 0.3 * outlier_component + 0.2 * stability_component
    return round(score * 100, 2)

def quick_weighting(df: pd.DataFrame, schema: Dict[str, str]) -> Dict[str, Any]:
    # Toy weighting: inverse response rate by a chosen domain (first categorical)
    domain_col: Optional[str] = next((c for c, t in schema.items() if t == "categorical"), None)
    if not domain_col:
        return {"method": "uniform", "weights": 1.0, "note": "No categorical domain found"}
    counts = df[domain_col].value_counts(dropna=False)
    total = counts.sum()
    weights = {str(k): float(total / v) for k, v in counts.items() if v > 0}
    return {"method": "inverse-frequency", "domain": domain_col, "weights": weights}

def guess_encoding(path: str) -> str:
    """Try a few common encodings to avoid UnicodeDecodeError on CSVs."""
    candidates = ["utf-8", "utf-8-sig", "cp1252", "latin1"]
    for enc in candidates:
        try:
            with open(path, "r", encoding=enc, errors="strict") as f:
                f.read(4096)
            return enc
        except UnicodeDecodeError:
            continue
        except Exception:
            continue
    return "latin1"

async def broadcast_progress(dataset_id: str, payload: Dict[str, Any]):
    # Record last state
    progress_streams[dataset_id] = payload
    for ws in list(websocket_clients.get(dataset_id, [])):
        if ws.application_state != WebSocketState.CONNECTED:
            continue
        try:
            await ws.send_text(orjson.dumps(payload).decode("utf-8"))
        except Exception:
            try:
                await ws.close()
            except Exception:
                pass
            websocket_clients[dataset_id].remove(ws)

# --- Minimal SQLite logging for demo auditability ---
async def init_db():
    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS datasets (
                dataset_id TEXT PRIMARY KEY,
                filename TEXT,
                row_count INTEGER,
                column_count INTEGER,
                missing_rate REAL,
                quality_score REAL,
                created_at TEXT
            )
            """
        )
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_trail (
                id TEXT PRIMARY KEY,
                event_timestamp TEXT,
                operation_type TEXT,
                resource_type TEXT,
                resource_id TEXT,
                details TEXT
            )
            """
        )
        await db.commit()

async def log_dataset(summary: ProcessSummary, filename: str):
    import aiosqlite, datetime
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO datasets(dataset_id, filename, row_count, column_count, missing_rate, quality_score, created_at) VALUES(?,?,?,?,?,?,?)",
            (
                summary.dataset_id,
                filename,
                summary.row_count,
                summary.column_count,
                summary.missing_rate,
                summary.quality_score,
                datetime.datetime.utcnow().isoformat(),
            ),
        )
        await db.commit()

async def log_audit(operation_type: str, resource_type: str, resource_id: str, details: Dict[str, Any]):
    import aiosqlite, datetime
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO audit_trail(id, event_timestamp, operation_type, resource_type, resource_id, details) VALUES(?,?,?,?,?,?)",
            (
                str(uuid.uuid4()),
                datetime.datetime.utcnow().isoformat(),
                operation_type,
                resource_type,
                resource_id,
                orjson.dumps(details).decode("utf-8"),
            ),
        )
        await db.commit()

@app.on_event("startup")
async def on_startup():
    await init_db()
    print(f"StatFlow AI started - Environment: {os.getenv('ENVIRONMENT', 'development')}")
    print(f"CORS Origins: {get_cors_origins()}")

@app.websocket("/ws/progress/{dataset_id}")
async def ws_progress(websocket: WebSocket, dataset_id: str):
    await websocket.accept()
    websocket_clients.setdefault(dataset_id, []).append(websocket)
    # Send last known state
    if dataset_id in progress_streams:
        await websocket.send_text(orjson.dumps(progress_streams[dataset_id]).decode("utf-8"))
    try:
        while True:
            await websocket.receive_text()  # keep-alive; ignore content
    except WebSocketDisconnect:
        pass
    finally:
        if websocket in websocket_clients.get(dataset_id, []):
            websocket_clients[dataset_id].remove(websocket)

@app.get("/health")
def health():
    return {
        "status": "ok", 
        "environment": os.getenv("ENVIRONMENT", "development"),
        "cors_origins": get_cors_origins()
    }

@app.get("/")
def root():
    return {
        "name": "StatFlow AI API",
        "version": app.version,
        "status": "healthy",
        "environment": os.getenv("ENVIRONMENT", "development"),
        "endpoints": ["/ingest", "/ws/progress/{dataset_id}", "/datasets/{dataset_id}", "/health"],
    }

async def process_file(dataset_id: str, raw_path: str, filename: str):
    start = time.time()
    try:
        row_count = 0
        column_count = 0
        missing_cells = 0
        schema: Dict[str, str] = {}
        chunks: List[pd.DataFrame] = []

        # Determine loader by extension
        _, ext = os.path.splitext(filename.lower())
        if ext in ['.xlsx', '.xls']:
            # For Excel, load whole file
            if ext == '.xlsx':
                df_full = pd.read_excel(raw_path, engine='openpyxl')
            else:  # .xls
                df_full = pd.read_excel(raw_path, engine='xlrd')
            chunks_iter = [df_full]
        else:
            def read_csv_resilient(path: str):
                enc_candidates = [guess_encoding(path), 'utf-8-sig', 'cp1252', 'latin1']
                for enc_try in enc_candidates:
                    try:
                        # Sniff delimiter for this encoding
                        try:
                            with open(path, 'rb') as fb:
                                sample_bytes = fb.read(8192)
                            sample_text = sample_bytes.decode(enc_try, errors='ignore')
                            sep_try = csv.Sniffer().sniff(sample_text).delimiter
                        except Exception:
                            sep_try = ','
                        # Prefer chunked read; fallback to full read if chunking fails
                        try:
                            for chunk_df in pd.read_csv(
                                path,
                                chunksize=20000,
                                encoding=enc_try,
                                sep=sep_try,
                                engine='python',  # robust to bad lines
                                on_bad_lines='skip',
                            ):
                                yield chunk_df
                            return
                        except Exception:
                            # Try full read fallback
                            df_all = pd.read_csv(
                                path,
                                encoding=enc_try,
                                sep=sep_try,
                                engine='python',
                                on_bad_lines='skip',
                            )
                            yield df_all
                            return
                    except Exception:
                        continue
                # Final fallback: latin1 + comma
                df_all = pd.read_csv(path, encoding='latin1', sep=',', engine='python', on_bad_lines='skip')
                yield df_all

            chunks_iter = read_csv_resilient(raw_path)

        for i, chunk in enumerate(chunks_iter):
            if i == 0:
                schema = infer_schema(chunk)
                column_count = len(chunk.columns)
            row_count += len(chunk)
            missing_cells += int(chunk.isna().sum().sum())
            chunks.append(chunk)
            await broadcast_progress(dataset_id, {"stage": "processing", "rows": row_count})

        df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        imputations_applied = simple_impute(df, schema) if not df.empty else 0
        outliers_detected = detect_outliers(df, schema) if not df.empty else 0
        missing_rate = (missing_cells / max(row_count * max(column_count, 1), 1)) if row_count else 0.0
        quality_score = compute_quality_score(missing_rate, imputations_applied, outliers_detected, row_count)
        weighting = quick_weighting(df, schema) if not df.empty else {"method": "uniform", "weights": 1.0}

        summary = ProcessSummary(
            dataset_id=dataset_id,
            row_count=row_count,
            column_count=column_count,
            missing_cells=missing_cells,
            missing_rate=round(missing_rate, 6),
            imputations_applied=imputations_applied,
            outliers_detected=outliers_detected,
            quality_score=quality_score,
            weighting=weighting,
            inferred_schema=schema,
        )

        await broadcast_progress(dataset_id, {"stage": "done", "summary": summary.model_dump()})
        await log_dataset(summary, filename)
        await log_audit(
            operation_type="ingest_complete",
            resource_type="dataset",
            resource_id=dataset_id,
            details={"filename": filename, "elapsed_sec": round(time.time() - start, 3)},
        )
    except Exception as e:
        await broadcast_progress(dataset_id, {"stage": "error", "message": str(e)})
        await log_audit("ingest_error", "dataset", dataset_id, {"error": str(e)})

@app.post("/ingest")
async def ingest(file: UploadFile = File(...)):
    dataset_id = str(uuid.uuid4())
    raw_path = os.path.join(STORAGE_DIR, f"{dataset_id}_{file.filename}")
    size_bytes = 0

    try:
        # Read in chunks for compatibility across Starlette versions
        with open(raw_path, "wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                size_bytes += len(chunk)
                f.write(chunk)
                if size_bytes % (2 * 1024 * 1024) == 0:
                    # Throttle progress events a bit
                    await broadcast_progress(dataset_id, {"stage": "upload", "bytes": size_bytes})
        # Final upload progress emit
        await broadcast_progress(dataset_id, {"stage": "upload", "bytes": size_bytes})
    except Exception as e:
        await broadcast_progress(dataset_id, {"stage": "error", "message": f"upload_failed: {e}"})
        raise HTTPException(status_code=400, detail="Upload failed")

    # Kick off processing in background
    asyncio.create_task(process_file(dataset_id, raw_path, file.filename or "unknown"))
    return JSONResponse({"ok": True, "dataset_id": dataset_id, "status": "accepted"})

@app.get("/datasets/{dataset_id}")
async def get_dataset(dataset_id: str):
    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM datasets WHERE dataset_id=?", (dataset_id,)) as cur:
            row = await cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Not found")
            return dict(row)

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=os.getenv("ENVIRONMENT") == "development")