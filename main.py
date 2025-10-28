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
from datetime import datetime, timezone, timedelta, date
from zoneinfo import ZoneInfo
import urllib.request

import requests
from google.cloud import bigquery
from google.cloud import secretmanager
from dotenv import load_dotenv
from decimal import Decimal

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
AMD_SYNC_LOCK_PROP      = os.getenv("AMD_SYNC_LOCK_PROP", "amd_synced")
ROI_PROTECTED_PROPERTIES = {
    p.strip() for p in os.getenv("ROI_PROTECTED_PROPERTIES", "").split(",") if p.strip()
}
ROI_SLACK_WEBHOOK_SECRET_NAME = os.getenv("SLACK_ROI_WEBHOOK_SECRET", "")
PATIENT_SLACK_WEBHOOK_SECRET_NAME = os.getenv("SLACK_PATIENT_WEBHOOK_SECRET", "")
ROI_OVERRIDE_PROPERTY = os.getenv("ROI_OVERRIDE_PROPERTY", "roi_manual_override")
ROI_PATIENT_MATCH_ERROR = "patient_match_missing"
DEFAULT_SLACK_WEBHOOK_SECRET_NAME = PATIENT_SLACK_WEBHOOK_SECRET_NAME or ROI_SLACK_WEBHOOK_SECRET_NAME

EASTERN_TZ = ZoneInfo("America/New_York")
_SLACK_WEBHOOK_URL_CACHE: Dict[str, Optional[str]] = {}

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

def clean_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        if value.tzinfo:
            return value.astimezone(EASTERN_TZ).isoformat()
        return value.replace(tzinfo=timezone.utc).astimezone(EASTERN_TZ).isoformat()
    return value

def to_epoch_millis(value: Any) -> Optional[int]:
    if value in (None, "",):
        return None
    dt: Optional[datetime] = None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"
            dt = datetime.fromisoformat(value)
        except ValueError:
            try:
                dt = datetime.strptime(value, "%Y-%m-%d")
            except ValueError:
                return None
        except ValueError:
            return None
    if not dt:
        return None
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=EASTERN_TZ)
    else:
        dt = dt.astimezone(EASTERN_TZ)
    return int(dt.timestamp() * 1000)

def to_eastern_date_string(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, (datetime,)):
        dt = value.astimezone(EASTERN_TZ) if value.tzinfo else value.replace(tzinfo=EASTERN_TZ)
        return dt.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    try:
        parsed = datetime.fromisoformat(str(value))
        if parsed.tzinfo:
            parsed = parsed.astimezone(EASTERN_TZ)
        else:
            parsed = parsed.replace(tzinfo=EASTERN_TZ)
        return parsed.date().isoformat()
    except ValueError:
        try:
            parsed_date = datetime.strptime(str(value), "%Y-%m-%d").date()
            return parsed_date.isoformat()
        except ValueError:
            return None

def compute_next_birthday(dob_value: Any) -> Optional[str]:
    if dob_value in (None, ""):
        return None
    dob_date: Optional[date] = None
    if isinstance(dob_value, date) and not isinstance(dob_value, datetime):
        dob_date = dob_value
    else:
        dob_str = str(dob_value)
        try:
            dob_date = datetime.fromisoformat(dob_str).date()
        except ValueError:
            try:
                dob_date = datetime.strptime(dob_str, "%Y-%m-%d").date()
            except Exception:
                return None
    if not dob_date:
        return None
    today = datetime.now(EASTERN_TZ).date()
    next_bday = dob_date.replace(year=today.year)
    if next_bday < today:
        next_bday = next_bday.replace(year=today.year + 1)
    return next_bday.isoformat()

def format_identifier(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            return str(int(value))
        return format(value.normalize(), 'f').rstrip('0').rstrip('.')
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value)
    return str(value)

# -------------------------
# Secret Manager
# -------------------------

def fetch_hubspot_api_key(project_id: str, secret_name: str) -> str:
    log("secret_fetch_start", project_id=project_id, secret_name=secret_name)
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    secret = response.payload.data.decode("UTF-8")
    log("secret_fetch_ok", secret_name=secret_name)
    return secret

def fetch_secret(project_id: str, secret_name: str) -> Optional[str]:
    if not secret_name:
        return None
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def get_slack_webhook_url(secret_name: Optional[str]) -> Optional[str]:
    if not secret_name:
        return None
    if secret_name in _SLACK_WEBHOOK_URL_CACHE:
        return _SLACK_WEBHOOK_URL_CACHE[secret_name]
    try:
        url = fetch_secret(GCP_PROJECT_ID, secret_name)
        _SLACK_WEBHOOK_URL_CACHE[secret_name] = url
        return url
    except Exception as exc:
        log("slack_secret_fetch_error", error=str(exc), secret=secret_name)
        _SLACK_WEBHOOK_URL_CACHE[secret_name] = None
        return None

def send_slack_alert(message: str, secret_name: Optional[str]):
    webhook_url = get_slack_webhook_url(secret_name)
    if not webhook_url:
        log("slack_alert_skipped", reason="no_webhook_configured", secret=secret_name, message=message)
        return
    payload = {"text": message}
    try:
        req = urllib.request.Request(
            webhook_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10):
            pass
        log("slack_alert_sent")
    except Exception as exc:
        log("slack_alert_failed", error=str(exc))

# -------------------------
# BigQuery helpers
# -------------------------

def ensure_control_tables(bq: bigquery.Client):
    """Create control tables if they don't exist (idempotent)."""
    log("ensure_control_tables_start", run_ledger=RUN_LEDGER_TABLE, dlq=DLQ_TABLE, id_map=ID_MAP_TABLE)
    # Run ledger
    log("ensure_control_table_create", table=RUN_LEDGER_TABLE)
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
    log("ensure_control_table_ready", table=RUN_LEDGER_TABLE)

    # DLQ
    log("ensure_control_table_create", table=DLQ_TABLE)
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
    log("ensure_control_table_ready", table=DLQ_TABLE)

    # ID map
    log("ensure_control_table_create", table=ID_MAP_TABLE)
    bq.query(f"""
        CREATE TABLE IF NOT EXISTS `{ID_MAP_TABLE}` (
            hubspot_object_type STRING,  -- 'contacts' | custom object name
            natural_key STRING,          -- e.g., amd_patient_id or roi_id
            hubspot_id STRING,
            updated_at TIMESTAMP
        )
    """).result()
    log("ensure_control_table_ready", table=ID_MAP_TABLE)
    log("ensure_control_tables_complete")

def read_high_watermark(bq: bigquery.Client, job_type: str) -> Optional[datetime]:
    log("high_watermark_read_start", job_type=job_type, run_ledger=RUN_LEDGER_TABLE)
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
    watermark = rows[0]["high_watermark"] if rows else None
    log("high_watermark_read_complete", job_type=job_type, watermark=str(watermark) if watermark else None)
    return watermark

def write_run_ledger(bq: bigquery.Client, row: Dict[str, Any]):
    table = RUN_LEDGER_TABLE
    log("run_ledger_write_start", table=table, job_type=row.get("job_type"), status=row.get("status"))
    sanitized = {k: clean_value(v) for k,v in row.items()}
    bq.insert_rows_json(table, [sanitized])
    log("run_ledger_write_complete", table=table, job_type=row.get("job_type"), status=row.get("status"))

def upsert_id_map(bq: bigquery.Client, obj_type: str, natural_key: str, hubspot_id: str):
    log("id_map_upsert_start", object_type=obj_type, natural_key=natural_key)
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
    log("id_map_upsert_complete", object_type=obj_type, natural_key=natural_key)

def get_mapped_hubspot_id(bq: bigquery.Client, obj_type: str, natural_key: str) -> Optional[str]:
    log("id_map_lookup_start", object_type=obj_type, natural_key=natural_key)
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
    hubspot_id = rows[0]["hubspot_id"] if rows else None
    log("id_map_lookup_complete", object_type=obj_type, natural_key=natural_key, found=bool(hubspot_id))
    return hubspot_id

def write_dlq(bq: bigquery.Client, job_type: str, natural_key: str, hubspot_object_type: str, payload: Dict[str,Any], error: str, attempt: int):
    log("dlq_write_start", job_type=job_type, natural_key=natural_key, hubspot_object_type=hubspot_object_type, error=error)
    bq.insert_rows_json(DLQ_TABLE, [{
        "ts": datetime.utcnow().isoformat(),
        "job_type": job_type,
        "natural_key": natural_key,
        "hubspot_object_type": hubspot_object_type,
        "payload": json.dumps(payload)[:90000],
        "error": error[:10000],
        "attempt": attempt
    }])
    log("dlq_write_complete", job_type=job_type, natural_key=natural_key, hubspot_object_type=hubspot_object_type)

def read_failure_attempts(bq: bigquery.Client, job_type: str, natural_key: str, error: str) -> int:
    q = f"""
      SELECT IFNULL(MAX(attempt), 0) AS max_attempt
      FROM `{DLQ_TABLE}`
      WHERE job_type = @job_type
        AND natural_key = @natural_key
        AND error = @error
    """
    job = bq.query(q, job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("job_type","STRING",job_type),
            bigquery.ScalarQueryParameter("natural_key","STRING",natural_key),
            bigquery.ScalarQueryParameter("error","STRING",error),
        ]
    ))
    rows = list(job.result())
    return int(rows[0]["max_attempt"]) if rows else 0

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
                log("hubspot_request_start", method=method, path=path, attempt=attempt)
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
                log("hubspot_request_complete", method=method, path=path, attempt=attempt, status=resp.status_code)
                return resp.status_code, data
            except requests.RequestException as e:
                backoff = min(30, INITIAL_BACKOFF * (2 ** (attempt-1)))
                log("hubspot_exception_retry", error=str(e), attempt=attempt, backoff_seconds=backoff)
                time.sleep(backoff)
        log("hubspot_request_failed", method=method, path=path)
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

    def get_contact(self, hubspot_id: str, properties: Optional[List[str]]=None) -> Optional[Dict[str,Any]]:
        params = {}
        if properties:
            params["properties"] = ",".join(properties)
        status, data = self._request("GET", f"/crm/v3/objects/contacts/{hubspot_id}", params=params)
        if status != 200:
            log("hubspot_get_contact_error", status=status, hubspot_id=hubspot_id, data=data)
            return None
        return data

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

    def get_custom(self, object_type: str, hubspot_id: str, properties: Optional[List[str]]=None) -> Optional[Dict[str,Any]]:
        params = {}
        if properties:
            params["properties"] = ",".join(properties)
        status, data = self._request("GET", f"/crm/v3/objects/{object_type}/{hubspot_id}", params=params)
        if status != 200:
            log("hubspot_get_custom_error", status=status, object_type=object_type, hubspot_id=hubspot_id, data=data)
            return None
        return data

# -------------------------
# Extraction
# -------------------------

def fetch_rows(bq: bigquery.Client, table: str, updated_col: str, watermark: Optional[datetime]) -> List[Dict[str,Any]]:
    log("fetch_rows_start", table=table, delta=bool(watermark), watermark=str(watermark) if watermark else None)
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
    patient_id_raw = row.get("ID")
    patient_chart_raw = row.get("Chart")
    patient_id = format_identifier(patient_id_raw)
    patient_chart = format_identifier(patient_chart_raw)
    fallback_key = row.get("PatientID") or row.get("Email") or hash8(json.dumps(row))
    natural_key = patient_id or patient_chart or str(fallback_key)
    dob_value = row.get("DOB")
    next_treatment = row.get("NextTreatment")
    next_follow_up = row.get("NextFollowUp")
    first_treatment = row.get("FirstTreatment") or row.get("FirstInitialConsult")
    props = {
        "email": (row.get("Email") or "").strip().lower() or None,
        "firstname": row.get("FirstName"),
        "preferred_first_name": row.get("PreferredFirstName"),
        "middlename": row.get("MiddleName") or row.get("PreferredMiddleName"),
        "lastname": row.get("LastName") or row.get("PreferredLastName"),
        "gender": row.get("Gender"),
        "date_of_birth": str(dob_value) if dob_value else None,
        "next_birthday": compute_next_birthday(dob_value),
        "address": row.get("Address1"),
        "street_address_line_2": row.get("Address2"),
        "city": row.get("City"),
        "state": row.get("State"),
        "zip": row.get("Zipcode"),
        "phone": row.get("HomePhone") or row.get("Phone"),
        "otherphone": row.get("OtherPhone"),
        "patient_id": patient_id,
        "patient_chart": patient_chart,
        "primary_facility": row.get("PrimaryFacility"),
        "primary_facility_code": row.get("PrimaryFacilityCode"),
        "spravatostodate": row.get("SpravatosToDate"),
        "ketaminestodate": row.get("KetaminesToDate"),
        "treatmentstodate": row.get("TreatmentsToDate"),
        "future_treatment_count": row.get("FutureTreatmentCount"),
        "future_follow_up_count": row.get("FutureFollowUpCount"),
        "next_treatment_date": to_eastern_date_string(next_treatment or row.get("MaxScheduledTreatment")),
        "next_follow_up_date": to_eastern_date_string(next_follow_up),
        "first_initial_consult__treatment_": to_epoch_millis(first_treatment),
        "started": row.get("Started"),
        "active_treatment": row.get("Active"),
        "care_type": row.get("CareType"),
        "lifecyclestage": "customer",
    }
    # add AMD lock flag default false; will be set to true once synced
    props[AMD_SYNC_LOCK_PROP] = True
    if PATIENT_NATURAL_ID_PROP:
        props[PATIENT_NATURAL_ID_PROP] = natural_key
    # Drop Nones/blank strings and coerce Decimal/Datetime to JSON-safe values
    props = {k: clean_value(v) for k,v in props.items() if v not in (None,"")}
    return natural_key, props

def find_patient_contact(bq: bigquery.Client, hs: HubSpot, patient_id: Any, patient_chart: Any) -> Optional[str]:
    formatted_patient_id = format_identifier(patient_id)
    formatted_patient_chart = format_identifier(patient_chart)
    candidates = []
    if formatted_patient_id:
        candidates.append(formatted_patient_id)
    if formatted_patient_chart:
        candidates.append(formatted_patient_chart)
    for candidate in candidates:
        hubspot_id = get_mapped_hubspot_id(bq, "contacts", candidate)
        if hubspot_id:
            return hubspot_id
    # Fallback to HubSpot search
    for candidate, property_name in ((formatted_patient_id, "patient_id"), (formatted_patient_chart, "patient_chart")):
        if not candidate:
            continue
        filters = [{"propertyName": property_name, "operator": "EQ", "value": candidate}]
        results = hs.search_contacts(filters=filters, properties=[AMD_SYNC_LOCK_PROP])
        if results:
            return results[0]["id"]
    return None

def map_roi_to_custom(row: Dict[str,Any]) -> Tuple[str, Dict[str,Any]]:
    roi_id = format_identifier(row.get("ROI_ID") or row.get("roi_id"))
    natural_key = roi_id or str(row.get("ID") or hash8(json.dumps(row)))
    patient_id = format_identifier(row.get("PatientID"))
    patient_chart = format_identifier(row.get("PatientChart"))
    props = {
        "roi_type": row.get("TemplateName"),
        "patient_chart": patient_chart,
        "raw_provider_name": row.get("ProviderName"),
        "patient_signed_dob": to_epoch_millis(row.get("DOB")),
        "patient_signed_name": row.get("Patient Name") or row.get("Patient_Name"),
        "raw_provider_specialty": row.get("Specialty"),
        "raw_provider_email": row.get("Email"),
        "raw_provider_phone": row.get("Phone"),
        "raw_provider_fax": row.get("Fax"),
        "completed_date": to_epoch_millis(row.get("CompletedDate")),
        "patient_id": patient_id,
        "accepted_datetime": to_epoch_millis(row.get("AcceptedDatetime")),
        "amd_template_id": row.get("TemplateID"),
        "roi_id": roi_id,
    }
    props = {k: clean_value(v) for k,v in props.items() if v not in (None,"")}
    for protected in ROI_PROTECTED_PROPERTIES:
        props.pop(protected, None)
    if ROI_NATURAL_ID_PROP:
        props[ROI_NATURAL_ID_PROP] = natural_key
    return natural_key, props

# -------------------------
# Loading (upsert with id map and ambiguity guard)
# -------------------------

def upsert_contacts(bq: bigquery.Client, hs: HubSpot, rows: List[Dict[str,Any]]) -> Tuple[int,int,int]:
    created=updated=skipped=0
    log("upsert_contacts_start", total=len(rows))
    for batch in chunks(rows, BATCH_SIZE):
        for row in batch:
            natural_key, props = map_patient_to_contact(row)
            hubspot_id = get_mapped_hubspot_id(bq, "contacts", natural_key)
            locked = False
            if hubspot_id:
                contact = hs.get_contact(hubspot_id, properties=[AMD_SYNC_LOCK_PROP, "patient_id", "patient_chart"])
                if contact:
                    contact_props = contact.get("properties", {})
                    locked = str(contact_props.get(AMD_SYNC_LOCK_PROP, "")).lower() == "true"
                    if locked:
                        props.pop("patient_id", None)
                        props.pop("patient_chart", None)
                        props.pop(AMD_SYNC_LOCK_PROP, None)
                    else:
                        props[AMD_SYNC_LOCK_PROP] = True
                else:
                    props[AMD_SYNC_LOCK_PROP] = True
            # ambiguity guard: if not mapped and no email, skip to DLQ
            if not hubspot_id and "email" not in props:
                write_dlq(bq, "patients", natural_key, "contacts", props, "ambiguous_no_email_unmapped", 1)
                log("contact_ambiguous_skip", natural_key=natural_key)
                continue
            # If unmapped, try search by email
            if not hubspot_id and "email" in props:
                search_properties = [PATIENT_NATURAL_ID_PROP] if PATIENT_NATURAL_ID_PROP else None
                results = hs.search_contacts(
                    filters=[{"propertyName":"email","operator":"EQ","value":props["email"]}],
                    properties=search_properties
                )
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
                attempts = read_failure_attempts(bq, "patients", natural_key, "create_or_update_failed") + 1
                write_dlq(bq, "patients", natural_key, "contacts", props, "create_or_update_failed", attempts)
                if attempts >= 5:
                    timestamp = datetime.now(EASTERN_TZ).isoformat()
                    patient_id_fmt = format_identifier(row.get('ID'))
                    patient_chart_fmt = format_identifier(row.get('Chart'))
                    send_slack_alert(
                        ":warning: Patient sync failed after 5 retries\n"
                        f"*Natural key:* {natural_key}\n"
                        f"*Patient ID / Chart:* {patient_id_fmt} / {patient_chart_fmt}\n"
                        f"*Email:* {row.get('Email') or 'unknown'}\n"
                        f"*Timestamp:* {timestamp}",
                        DEFAULT_SLACK_WEBHOOK_SECRET_NAME,
                    )
    log("upsert_contacts_complete", total=len(rows), created=created, updated=updated, skipped=skipped)
    return created, updated, skipped

def upsert_rois(bq: bigquery.Client, hs: HubSpot, rows: List[Dict[str,Any]]) -> Tuple[int,int,int]:
    created=updated=skipped=0
    log("upsert_rois_start", total=len(rows))
    for batch in chunks(rows, BATCH_SIZE):
        for row in batch:
            status = row.get("processing_status") or row.get("ProcessingStatus")
            processed_at = row.get("processing_datetime") or row.get("ProcessingDatetime") or row.get("ProcessingDateTime")
            if status and status.lower() == "processed" and processed_at not in (None, ""):
                natural_key = str(row.get("ROI_ID") or row.get("roi_id") or row.get("ID") or hash8(json.dumps(row)))
                skipped += 1
                log("roi_manual_processed_skip", natural_key=natural_key, status=status, processed_at=str(processed_at))
                continue
            patient_id = row.get("PatientID")
            patient_chart = row.get("PatientChart")
            patient_contact_id = find_patient_contact(bq, hs, patient_id, patient_chart)
            if not patient_contact_id:
                natural_key = str(row.get("ROI_ID") or row.get("roi_id") or row.get("ID") or hash8(json.dumps(row)))
                attempts = read_failure_attempts(bq, "rois", natural_key, ROI_PATIENT_MATCH_ERROR) + 1
                skipped += 1
                log("roi_patient_missing", natural_key=natural_key, patient_id=patient_id, patient_chart=patient_chart, attempt=attempts)
                write_dlq(
                    bq,
                    "rois",
                    natural_key,
                    ROI_OBJECT_TYPE,
                    {
                        "patient_id": patient_id,
                        "patient_chart": patient_chart,
                        "processing_status": status,
                    },
                    ROI_PATIENT_MATCH_ERROR,
                    attempts
                )
                if attempts >= 5:
                    timestamp = datetime.now(EASTERN_TZ).isoformat()
                    send_slack_alert(
                        ":warning: ROI sync skipped after 5 retries\n"
                        f"*ROI ID:* {natural_key}\n"
                        f"*Patient ID / Chart:* {patient_id} / {patient_chart}\n"
                        f"*Last status:* {status or 'unknown'}\n"
                        f"*Timestamp:* {timestamp}",
                        DEFAULT_SLACK_WEBHOOK_SECRET_NAME,
                    )
                continue
            natural_key, props = map_roi_to_custom(row)
            hubspot_id = get_mapped_hubspot_id(bq, ROI_OBJECT_TYPE, natural_key)
            if hubspot_id:
                existing = hs.get_custom(ROI_OBJECT_TYPE, hubspot_id, properties=[ROI_OVERRIDE_PROPERTY])
                if existing:
                    properties = existing.get("properties", {})
                    override_flag = str(properties.get(ROI_OVERRIDE_PROPERTY, "")).lower() == "true"
                    if override_flag:
                        skipped += 1
                        log("roi_manual_override_skip", natural_key=natural_key, hubspot_id=hubspot_id)
                        continue
            obj_id, created_flag = hs.create_or_update_custom(ROI_OBJECT_TYPE, props, hubspot_id)
            if obj_id:
                upsert_id_map(bq, ROI_OBJECT_TYPE, natural_key, obj_id)
                if created_flag: created += 1
                else: updated += 1
            else:
                write_dlq(bq, "rois", natural_key, ROI_OBJECT_TYPE, props, "create_or_update_failed", 1)
    log("upsert_rois_complete", total=len(rows), created=created, updated=updated, skipped=skipped)
    return created, updated, skipped

# -------------------------
# Entrypoints
# -------------------------

def run_job(job_type: str):
    log("run_job_start", job_type=job_type)
    
    log("initializing_bigquery_client")
    bq = bigquery.Client(project=GCP_PROJECT_ID)
    log("initializing_bigquery_client_ok")

    log("ensuring_control_tables")
    ensure_control_tables(bq)
    log("ensuring_control_tables_ok")

    log("fetching_hubspot_api_key")
    hs_key = fetch_hubspot_api_key(GCP_PROJECT_ID, HUBSPOT_API_SECRET_NAME)
    log("fetching_hubspot_api_key_ok")

    log("initializing_hubspot_client")
    hs = HubSpot(hs_key)
    log("initializing_hubspot_client_ok")

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
