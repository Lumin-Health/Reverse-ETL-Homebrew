#!/usr/bin/env python3
"""
Reverse ETL: AdvancedMD (BigQuery) -> HubSpot
Production-hardened sync with idempotency, DLQ, checkpointing, structured logging, and PHI minimization.
"""
import os
import sys
import json
import time
import math
import hashlib
from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import datetime, timezone, timedelta

import requests
from google.cloud import bigquery
from google.cloud import secretmanager
from dotenv import load_dotenv

# -------------------------
# Environment & Constants
# -------------------------
load_dotenv()  # for local dev

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "lumininternal")

# Source tables
BIGQUERY_PATIENT_TABLE = os.getenv("BIGQUERY_PATIENT_TABLE", "lumininternal.amd.PatientsWithStatistics")
BIGQUERY_ROI_TABLE     = os.getenv("BIGQUERY_ROI_TABLE", "lumininternal.amd.ROIs")

# Optional delta column names (if present). If not present, code will full-scan.
PATIENT_UPDATED_AT_COL = os.getenv("PATIENT_UPDATED_AT_COL", "updated_at")
ROI_UPDATED_AT_COL     = os.getenv("ROI_UPDATED_AT_COL", "updated_at")

# Internal control tables (create these in BQ: dataset etl)
BQ_DATASET             = os.getenv("BQ_DATASET", "etl")
RUN_LEDGER_TABLE       = os.getenv("RUN_LEDGER_TABLE", f"{GCP_PROJECT_ID}.{BQ_DATASET}.reverse_etl_run_ledger")
DLQ_TABLE              = os.getenv("DLQ_TABLE", f"{GCP_PROJECT_ID}.{BQ_DATASET}.reverse_etl_dlq")
ID_MAP_TABLE           = os.getenv("ID_MAP_TABLE", f"{GCP_PROJECT_ID}.{BQ_DATASET}.hubspot_id_map")

# Secrets
HUBSPOT_API_SECRET_NAME = os.getenv("HUBSPOT_API_SECRET_NAME", "HubspotApiKey")

# HubSpot details
HUBSPOT_BASE    = os.getenv("HUBSPOT_BASE", "https://api.hubapi.com")
HUBSPOT_TIMEOUT = int(os.getenv("HUBSPOT_TIMEOUT", "20"))
# Batching
BATCH_SIZE      = int(os.getenv("BATCH_SIZE", "50"))
MAX_RETRIES     = int(os.getenv("MAX_RETRIES", "5"))
INITIAL_BACKOFF = float(os.getenv("INITIAL_BACKOFF", "0.5"))

# Object type names (adjust if your portal uses different custom object names)
ROI_OBJECT_TYPE = os.getenv("ROI_OBJECT_TYPE", "p_roi")  # example: "p_roi"
# Custom property names
PATIENT_NATURAL_ID_PROP = os.getenv("PATIENT_NATURAL_ID_PROP", "amd_patient_id")
ROI_NATURAL_ID_PROP     = os.getenv("ROI_NATURAL_ID_PROP", "roi_id")

# -------------------------
# Utilities
# -------------------------

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def log(event: str, **kwargs):
    """Structured, PHI-minimized logging to stdout (Cloud Logging captures)."""
    payload = {"ts": now_utc().isoformat(), "event": event}
    # redact obvious PHI fields if present
    redacted = {}
    for k,v in kwargs.items():
        if isinstance(v, str) and any(key in k.lower() for key in ["name","email","address","dob","phone","gender"]):
            redacted[k] = hash8(v)
        else:
            redacted[k] = v
    payload.update(redacted)
    print(json.dumps(payload, default=str))

def hash8(value: str) -> str:
    try:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]
    except Exception:
        return "hash_err"

def chunks(iterable: List[Dict], size: int) -> Iterable[List[Dict]]:
    for i in range(0, len(iterable), size):
        yield iterable[i:i+size]

# -------------------------
# Secret Manager
# -------------------------

def fetch_hubspot_api_key(project_id: str, secret_name: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# -------------------------
# BigQuery helpers
# -------------------------

def ensure_control_tables(bq: bigquery.Client):
    """Create control tables if they don't exist (idempotent)."""
    # Run ledger
    bq.query(f"""
        CREATE TABLE IF NOT EXISTS `{RUN_LEDGER_TABLE}` (
            run_id STRING,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            job_type STRING,         -- 'patients' | 'rois'
            high_watermark TIMESTAMP,
            read_count INT64,
            updated_count INT64,
            created_count INT64,
            skipped_count INT64,
            error_count INT64,
            status STRING            -- 'success' | 'failed' | 'partial'
        )
    """).result()

    # DLQ
    bq.query(f"""
        CREATE TABLE IF NOT EXISTS `{DLQ_TABLE}` (
            ts TIMESTAMP,
            job_type STRING,
            natural_key STRING,
            hubspot_object_type STRING,
            payload STRING,
            error STRING,
            attempt INT64
        )
    """).result()

    # ID map
    bq.query(f"""
        CREATE TABLE IF NOT EXISTS `{ID_MAP_TABLE}` (
            hubspot_object_type STRING,  -- 'contacts' | custom object name
            natural_key STRING,          -- e.g., amd_patient_id or roi_id
            hubspot_id STRING,
            updated_at TIMESTAMP
        )
    """).result()

def read_high_watermark(bq: bigquery.Client, job_type: str) -> Optional[datetime]:
    q = f"""
      SELECT high_watermark
      FROM `{RUN_LEDGER_TABLE}`
      WHERE job_type = @job_type AND status = 'success'
      ORDER BY finished_at DESC
      LIMIT 1
    """
    job = bq.query(q, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("job_type","STRING",job_type)]
    ))
    rows = list(job.result())
    return rows[0]["high_watermark"] if rows else None

def write_run_ledger(bq: bigquery.Client, row: Dict[str, Any]):
    table = RUN_LEDGER_TABLE
    bq.insert_rows_json(table, [row])

def upsert_id_map(bq: bigquery.Client, obj_type: str, natural_key: str, hubspot_id: str):
    q = f"""
      MERGE `{ID_MAP_TABLE}` T
      USING (SELECT @obj_type AS obj_type, @natural_key AS nk, @hubspot_id AS hid) S
      ON T.hubspot_object_type = S.obj_type AND T.natural_key = S.nk
      WHEN MATCHED THEN UPDATE SET hubspot_id = S.hid, updated_at = CURRENT_TIMESTAMP()
      WHEN NOT MATCHED THEN INSERT (hubspot_object_type, natural_key, hubspot_id, updated_at)
      VALUES (S.obj_type, S.nk, S.hid, CURRENT_TIMESTAMP())
    """
    bq.query(q, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("obj_type","STRING",obj_type),
            bigquery.ScalarQueryParameter("natural_key","STRING",natural_key),
            bigquery.ScalarQueryParameter("hubspot_id","STRING",hubspot_id),
        ]
    )).result()

def get_mapped_hubspot_id(bq: bigquery.Client, obj_type: str, natural_key: str) -> Optional[str]:
    q = f"""
      SELECT hubspot_id FROM `{ID_MAP_TABLE}`
      WHERE hubspot_object_type = @obj_type AND natural_key = @nk
      LIMIT 1
    """
    job = bq.query(q, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("obj_type","STRING",obj_type),
            bigquery.ScalarQueryParameter("nk","STRING",natural_key),
        ]
    ))
    rows = list(job.result())
    return rows[0]["hubspot_id"] if rows else None

def write_dlq(bq: bigquery.Client, job_type: str, natural_key: str, hubspot_object_type: str, payload: Dict[str,Any], error: str, attempt: int):
    bq.insert_rows_json(DLQ_TABLE, [{
        "ts": datetime.utcnow().isoformat(),
        "job_type": job_type,
        "natural_key": natural_key,
        "hubspot_object_type": hubspot_object_type,
        "payload": json.dumps(payload)[:90000],
        "error": error[:10000],
        "attempt": attempt
    }])

# -------------------------
# HubSpot API helpers
# -------------------------

class HubSpot:
    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        })

    def _request(self, method: str, path: str, **kwargs) -> Tuple[int, Dict[str,Any]]:
        url = f"{HUBSPOT_BASE}{path}"
        for attempt in range(1, MAX_RETRIES+1):
            try:
                resp = self.session.request(method, url, timeout=HUBSPOT_TIMEOUT, **kwargs)
                if resp.status_code in (429, 500, 502, 503, 504):
                    backoff = min(30, INITIAL_BACKOFF * (2 ** (attempt-1)))
                    log("hubspot_retry", status=resp.status_code, attempt=attempt, backoff_seconds=backoff)
                    time.sleep(backoff)
                    continue
                data = {}
                try:
                    data = resp.json()
                except Exception:
                    pass
                return resp.status_code, data
            except requests.RequestException as e:
                backoff = min(30, INITIAL_BACKOFF * (2 ** (attempt-1)))
                log("hubspot_exception_retry", error=str(e), attempt=attempt, backoff_seconds=backoff)
                time.sleep(backoff)
        return 599, {"error": "max retries exceeded"}

    # Contacts
    def search_contacts(self, filters: List[Dict[str,Any]], properties: Optional[List[str]]=None) -> List[Dict[str,Any]]:
        body = {"filterGroups": [{"filters": filters}]}
        if properties:
            body["properties"] = properties
        status, data = self._request("POST", "/crm/v3/objects/contacts/search", json=body)
        if status != 200:
            log("hubspot_search_error", status=status, body=body, data=data)
            return []
        return data.get("results", [])

    def create_or_update_contact(self, properties: Dict[str,Any], hubspot_id: Optional[str]=None) -> Tuple[Optional[str], bool]:
        """Returns (hubspot_id, created_bool)."""
        if hubspot_id:
            status, data = self._request("PATCH", f"/crm/v3/objects/contacts/{hubspot_id}", json={"properties": properties})
            if status in (200, 201):
                return data.get("id"), False
            log("hubspot_update_contact_error", status=status, data=data)
            return None, False
        else:
            status, data = self._request("POST", "/crm/v3/objects/contacts", json={"properties": properties})
            if status in (200, 201):
                return data.get("id"), True
            log("hubspot_create_contact_error", status=status, data=data)
            return None, False

    # Generic custom object
    def search_custom(self, object_type: str, filters: List[Dict[str,Any]], properties: Optional[List[str]]=None) -> List[Dict[str,Any]]:
        body = {"filterGroups": [{"filters": filters}]}
        if properties:
            body["properties"] = properties
        status, data = self._request("POST", f"/crm/v3/objects/{object_type}/search", json=body)
        if status != 200:
            log("hubspot_search_custom_error", status=status, object_type=object_type, data=data)
            return []
        return data.get("results", [])

    def create_or_update_custom(self, object_type: str, properties: Dict[str,Any], hubspot_id: Optional[str]=None) -> Tuple[Optional[str], bool]:
        if hubspot_id:
            status, data = self._request("PATCH", f"/crm/v3/objects/{object_type}/{hubspot_id}", json={"properties": properties})
            if status in (200, 201):
                return data.get("id"), False
            log("hubspot_update_custom_error", status=status, object_type=object_type, data=data)
            return None, False
        else:
            status, data = self._request("POST", f"/crm/v3/objects/{object_type}", json={"properties": properties})
            if status in (200, 201):
                return data.get("id"), True
            log("hubspot_create_custom_error", status=status, object_type=object_type, data=data)
            return None, False

# -------------------------
# Extraction
# -------------------------

def fetch_rows(bq: bigquery.Client, table: str, updated_col: str, watermark: Optional[datetime]) -> List[Dict[str,Any]]:
    if watermark:
        q = f"SELECT * FROM `{table}` WHERE {updated_col} >= @wm"
        params = [bigquery.ScalarQueryParameter("wm","TIMESTAMP", watermark)]
    else:
        q = f"SELECT * FROM `{table}`"
        params = []
    try:
        job = bq.query(q, job_config=bigquery.QueryJobConfig(query_parameters=params))
        rows = [dict(row) for row in job.result()]
        log("fetch_rows_ok", table=table, count=len(rows), delta=bool(watermark))
        return rows
    except Exception as e:
        log("fetch_rows_error", table=table, error=str(e))
        return []

# -------------------------
# Transformation (mapping)
# -------------------------

def map_patient_to_contact(row: Dict[str,Any]) -> Tuple[str, Dict[str,Any]]:
    """
    Returns (natural_key, properties). Natural key is amd_patient_id (falls back to Chart or ID if present).
    Adjust field names to your schema.
    """
    natural_key = str(row.get("ID") or row.get("Chart") or row.get("PatientID") or row.get("Email") or hash8(json.dumps(row)))
    props = {
        "email": (row.get("Email") or "").strip().lower() or None,
        "firstname": row.get("FirstName"),
        "lastname": row.get("LastName"),
        PATIENT_NATURAL_ID_PROP: natural_key,
        "date_of_birth": str(row.get("DOB")) if row.get("DOB") else None,
        "address": row.get("Address1"),
        "city": row.get("City"),
        "state": row.get("State"),
        "zip": row.get("PostalCode"),
        "phone": row.get("Phone"),
    }
    # Drop Nones to avoid overwriting with nulls
    props = {k:v for k,v in props.items() if v not in (None,"")}
    return natural_key, props

def map_roi_to_custom(row: Dict[str,Any]) -> Tuple[str, Dict[str,Any]]:
    natural_key = str(row.get("ROI_ID") or row.get("roi_id") or row.get("ID") or hash8(json.dumps(row)))
    props = {
        ROI_NATURAL_ID_PROP: natural_key,
        "status": row.get("Status"),
        "template_id": row.get("TemplateID") or row.get("template_id"),
        "patient_chart": row.get("Chart") or row.get("chart"),
    }
    props = {k:v for k,v in props.items() if v not in (None,"")}
    return natural_key, props

# -------------------------
# Loading (upsert with id map and ambiguity guard)
# -------------------------

def upsert_contacts(bq: bigquery.Client, hs: HubSpot, rows: List[Dict[str,Any]]) -> Tuple[int,int,int]:
    created=updated=skipped=0
    for batch in chunks(rows, BATCH_SIZE):
        for row in batch:
            natural_key, props = map_patient_to_contact(row)
            hubspot_id = get_mapped_hubspot_id(bq, "contacts", natural_key)
            # ambiguity guard: if not mapped and no email, skip to DLQ
            if not hubspot_id and "email" not in props:
                write_dlq(bq, "patients", natural_key, "contacts", props, "ambiguous_no_email_unmapped", 1)
                log("contact_ambiguous_skip", natural_key=natural_key)
                continue
            # If unmapped, try search by email
            if not hubspot_id and "email" in props:
                results = hs.search_contacts(filters=[{"propertyName":"email","operator":"EQ","value":props["email"]}], properties=[PATIENT_NATURAL_ID_PROP])
                if len(results) == 1:
                    hubspot_id = results[0]["id"]
                elif len(results) > 1:
                    # ambiguous
                    write_dlq(bq, "patients", natural_key, "contacts", props, "ambiguous_multiple_matches", 1)
                    log("contact_ambiguous_multiple", email_hash=hash8(props["email"]), count=len(results))
                    continue
            # upsert
            obj_id, created_flag = hs.create_or_update_contact(props, hubspot_id)
            if obj_id:
                upsert_id_map(bq, "contacts", natural_key, obj_id)
                if created_flag: created += 1
                else: updated += 1
            else:
                write_dlq(bq, "patients", natural_key, "contacts", props, "create_or_update_failed", 1)
    return created, updated, skipped

def upsert_rois(bq: bigquery.Client, hs: HubSpot, rows: List[Dict[str,Any]]) -> Tuple[int,int,int]:
    created=updated=skipped=0
    for batch in chunks(rows, BATCH_SIZE):
        for row in batch:
            natural_key, props = map_roi_to_custom(row)
            hubspot_id = get_mapped_hubspot_id(bq, ROI_OBJECT_TYPE, natural_key)
            obj_id, created_flag = hs.create_or_update_custom(ROI_OBJECT_TYPE, props, hubspot_id)
            if obj_id:
                upsert_id_map(bq, ROI_OBJECT_TYPE, natural_key, obj_id)
                if created_flag: created += 1
                else: updated += 1
            else:
                write_dlq(bq, "rois", natural_key, ROI_OBJECT_TYPE, props, "create_or_update_failed", 1)
    return created, updated, skipped

# -------------------------
# Entrypoints
# -------------------------

def run_job(job_type: str):
    bq = bigquery.Client(project=GCP_PROJECT_ID)
    ensure_control_tables(bq)
    hs_key = fetch_hubspot_api_key(GCP_PROJECT_ID, HUBSPOT_API_SECRET_NAME)
    hs = HubSpot(hs_key)

    started = now_utc()
    watermark = read_high_watermark(bq, job_type)
    read = created = updated = skipped = errors = 0

    try:
        if job_type == "patients":
            rows = fetch_rows(bq, BIGQUERY_PATIENT_TABLE, PATIENT_UPDATED_AT_COL, watermark)
            read = len(rows)
            c,u,s = upsert_contacts(bq, hs, rows)
            created+=c; updated+=u; skipped+=s
        elif job_type == "rois":
            rows = fetch_rows(bq, BIGQUERY_ROI_TABLE, ROI_UPDATED_AT_COL, watermark)
            read = len(rows)
            c,u,s = upsert_rois(bq, hs, rows)
            created+=c; updated+=u; skipped+=s
        else:
            raise ValueError(f"Unknown job_type {job_type}")

        status = "success"
        high_watermark = now_utc()  # conservative; you can compute max(updated_at) from rows if desired
    except Exception as e:
        errors += 1
        status = "failed"
        high_watermark = watermark  # don't advance
        log("job_exception", job_type=job_type, error=str(e))
    finally:
        write_run_ledger(bigquery.Client(project=GCP_PROJECT_ID), {
            "run_id": f"{job_type}-{started.timestamp()}",
            "started_at": started,
            "finished_at": now_utc(),
            "job_type": job_type,
            "high_watermark": high_watermark,
            "read_count": read,
            "updated_count": updated,
            "created_count": created,
            "skipped_count": skipped,
            "error_count": errors,
            "status": status
        })
        log("job_summary", job_type=job_type, read=read, created=created, updated=updated, skipped=skipped, errors=errors, status=status)

    return {"status": status, "read": read, "created": created, "updated": updated, "skipped": skipped, "errors": errors}

# Cloud Functions HTTP entrypoints
def sync_patients(request=None):
    return run_job("patients")

def sync_rois(request=None):
    return run_job("rois")

if __name__ == "__main__":
    # Local testing (requires ADC and Secret Manager access)
    print(sync_patients())
    print(sync_rois())
