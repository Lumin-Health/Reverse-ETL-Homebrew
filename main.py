import os
import json
import requests
import pytz
from datetime import datetime
from google.cloud import bigquery
from google.cloud import secretmanager
from dotenv import load_dotenv

# Load environment variables from .env file for local development
load_dotenv()

# --- Configuration ---
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "lumininternal")
BIGQUERY_PATIENT_TABLE = "lumininternal.amd.PatientsWithStatistics"
BIGQUERY_ROI_TABLE = "lumininternal.amd.ROIs"
HUBSPOT_API_SECRET_NAME = os.getenv("HUBSPOT_API_SECRET_NAME", "hubspot-api-key")
TIMEZONE = "America/New_York"

# --- Helper Functions ---

def get_hubspot_api_key():
    """Retrieves the Hubspot API key from Google Secret Manager."""
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{GCP_PROJECT_ID}/secrets/{HUBSPOT_API_SECRET_NAME}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        print(f"Error retrieving Hubspot API key from Secret Manager: {e}")
        raise

def get_amd_rois_from_bigquery():
    """Fetches ROI data from the BigQuery source table."""
    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        query = f"""
            SELECT
                *
            FROM
                `{BIGQUERY_ROI_TABLE}`
        """
        query_job = client.query(query)
        rois = [dict(row) for row in query_job]
        return rois
    except Exception as e:
        print(f"Error fetching ROIs from BigQuery: {e}")
        raise

def get_amd_patients_from_bigquery():
    """Fetches patient data from the BigQuery source table."""
    try:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        query = f"""
            SELECT
                ID,
                Chart,
                FirstName,
                LastName,
                Email,
                DOB,
                Gender,
                Address1,
                Address2,
                City,
                State,
                Zipcode,
                HomePhone
            FROM
                `{BIGQUERY_PATIENT_TABLE}`
        """
        query_job = client.query(query)
        patients = [dict(row) for row in query_job]
        return patients
    except Exception as e:
        print(f"Error fetching patients from BigQuery: {e}")
        raise

# --- Hubspot API Functions ---

def format_patient_for_hubspot(patient):
    """Formats a patient dictionary from BigQuery to a Hubspot contact properties dictionary."""
    # Hubspot expects date properties to be midnight UTC on the date.
    dob = patient.get("DOB")
    if dob:
        dob_datetime = datetime(dob.year, dob.month, dob.day)
        dob_utc = pytz.utc.localize(dob_datetime)
        ncf_date_of_birth = dob_utc.isoformat().replace("+00:00", "Z")
    else:
        ncf_date_of_birth = None

    properties = {
        "firstname": patient.get("FirstName"),
        "lastname": patient.get("LastName"),
        "email": patient.get("Email"),
        "gender": patient.get("Gender"),
        "address": patient.get("Address1"),
        "street_address_line_2": patient.get("Address2"),
        "city": patient.get("City"),
        "state": patient.get("State"),
        "zip": patient.get("Zipcode"),
        "phone": patient.get("HomePhone"),
        "ncf_date_of_birth": ncf_date_of_birth,
        "patient_id": patient.get("ID"),
        "patient_chart": patient.get("Chart"),
        "contact_type": "Patient"
    }
    # Remove keys with None values
    return {k: v for k, v in properties.items() if v is not None}

def search_hubspot_contact_by_properties(api_key, filters):
    """Searches for a Hubspot contact by a list of filters."""
    url = f"https://api.hubapi.com/crm/v3/objects/contacts/search"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "filterGroups": [{"filters": filters}],
        "properties": ["email", "firstname", "lastname", "hs_object_id"],
        "limit": 2 # Limit to 2 to detect multiple matches
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"Error searching for Hubspot contact with filters {filters}: {e}")
        return []

def search_hubspot_contact(api_key, email):
    """Searches for a Hubspot contact by email."""
    url = f"https://api.hubapi.com/crm/v3/objects/contacts/search"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "email",
                        "operator": "EQ",
                        "value": email
                    }
                ]
            }
        ],
        "properties": ["email", "firstname", "lastname", "hs_object_id"],
        "limit": 1
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        results = response.json().get("results", [])
        if results:
            return results[0]
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error searching for Hubspot contact with email {email}: {e}")
        return None

def create_hubspot_contact(api_key, patient_data):
    """Creates a new Hubspot contact."""
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {"properties": format_patient_for_hubspot(patient_data)}
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        print(f"Successfully created Hubspot contact for email {patient_data.get('Email')}")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error creating Hubspot contact for email {patient_data.get('Email')}: {e}")
        return None

def update_hubspot_contact(api_key, contact_id, patient_data):
    """Updates an existing Hubspot contact."""
def update_hubspot_contact(api_key, contact_id, patient_data):
    """Updates an existing Hubspot contact."""
    url = f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {"properties": format_patient_for_hubspot(patient_data)}
    try:
        response = requests.patch(url, headers=headers, json=payload)
        response.raise_for_status()
        print(f"Successfully updated Hubspot contact ID {contact_id} for email {patient_data.get('Email')}")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error updating Hubspot contact ID {contact_id} for email {patient_data.get('Email')}: {e}")
        return None

def format_roi_for_hubspot(roi):
    """Formats an ROI dictionary from BigQuery to a Hubspot custom object properties dictionary."""
    ny_tz = pytz.timezone(TIMEZONE)
    
    accepted_datetime = roi.get("AcceptedDatetime")
    if accepted_datetime:
        # Assuming BigQuery datetime is naive (UTC)
        accepted_datetime_utc = pytz.utc.localize(accepted_datetime)
        accepted_datetime_ny = accepted_datetime_utc.astimezone(ny_tz)
        accepted_datetime_formatted = accepted_datetime_ny.isoformat()
    else:
        accepted_datetime_formatted = None

    properties = {
        "roi_id": roi.get("roi_id"),
        "amd_template_id": roi.get("TemplateID"),
        "roi_type": roi.get("TemplateName"),
        "patient_id": roi.get("PatientID"),
        "patient_chart": roi.get("PatientChart"),
        "accepted_datetime": accepted_datetime_formatted,
        "completed_date": roi.get("CompletedDate"),
        "patient_signed_name": roi.get("Patient_Name"),
        "patient_signed_dob": roi.get("DOB"),
        "raw_provider_name": roi.get("ProviderName"),
        "raw_provider_specialty": roi.get("Specialty"),
        "raw_provider_email": roi.get("Email"),
        "raw_provider_phone": roi.get("Phone"),
        "raw_provider_fax": roi.get("Fax")
    }
    return {k: v for k, v in properties.items() if v is not None}

def search_hubspot_roi(api_key, roi_id):
    """Searches for a Hubspot ROI custom object by roi_id."""
    url = f"https://api.hubapi.com/crm/v3/objects/p46446185_2_46579893/search"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": "roi_id",
                        "operator": "EQ",
                        "value": roi_id
                    }
                ]
            }
        ],
        "properties": ["roi_id", "hs_object_id"],
        "limit": 1
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        results = response.json().get("results", [])
        if results:
            return results[0]
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error searching for Hubspot ROI with roi_id {roi_id}: {e}")
        return None

def create_hubspot_roi(api_key, roi_data):
    """Creates a new Hubspot ROI custom object."""
    url = "https://api.hubapi.com/crm/v3/objects/p46446185_2_46579893"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {"properties": format_roi_for_hubspot(roi_data)}
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        print(f"Successfully created Hubspot ROI for roi_id {roi_data.get('roi_id')}")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error creating Hubspot ROI for roi_id {roi_data.get('roi_id')}: {e}")
        return None

def update_hubspot_roi(api_key, object_id, roi_data):
    """Updates an existing Hubspot ROI custom object."""
    url = f"https://api.hubapi.com/crm/v3/objects/p46446185_2_46579893/{object_id}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {"properties": format_roi_for_hubspot(roi_data)}
    try:
        response = requests.patch(url, headers=headers, json=payload)
        response.raise_for_status()
        print(f"Successfully updated Hubspot ROI ID {object_id} for roi_id {roi_data.get('roi_id')}")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error updating Hubspot ROI ID {object_id} for roi_id {roi_data.get('roi_id')}: {e}")
        return None

# --- Main Cloud Function Logic ---

def sync_patients(request):
    """
    Cloud Function entry point for syncing AMD patients to Hubspot contacts.
    This function can be triggered by an HTTP request or Cloud Scheduler.
    """
    print("Starting patient sync process...")

    try:
        # 1. Get Hubspot API Key
        api_key = get_hubspot_api_key()

        # 2. Get Patients from BigQuery
        patients = get_amd_patients_from_bigquery()
        print(f"Found {len(patients)} patients to process from BigQuery.")

        # 3. Process each patient
        for patient in patients:
            contact_to_update = None
            
            # Tier 1: Match by Email
            if patient.get("Email"):
                contact = search_hubspot_contact(api_key, patient["Email"])
                if contact:
                    contact_to_update = contact
            
            # Tier 2: Match by FirstName, LastName, and DOB
            if not contact_to_update and patient.get("FirstName") and patient.get("LastName") and patient.get("DOB"):
                filters = [
                    {"propertyName": "firstname", "operator": "EQ", "value": patient["FirstName"]},
                    {"propertyName": "lastname", "operator": "EQ", "value": patient["LastName"]},
                    {"propertyName": "ncf_date_of_birth", "operator": "EQ", "value": patient["DOB"].isoformat()}
                ]
                contacts = search_hubspot_contact_by_properties(api_key, filters)
                if len(contacts) == 1:
                    contact_to_update = contacts[0]
                elif len(contacts) > 1:
                    print(f"Warning: Found multiple contacts for {patient['FirstName']} {patient['LastName']} with DOB {patient['DOB']}. Skipping update.")

            # Tier 3: Match by Address
            if not contact_to_update and patient.get("Address1") and patient.get("City") and patient.get("State") and patient.get("Zipcode"):
                filters = [
                    {"propertyName": "address", "operator": "EQ", "value": patient["Address1"]},
                    {"propertyName": "city", "operator": "EQ", "value": patient["City"]},
                    {"propertyName": "state", "operator": "EQ", "value": patient["State"]},
                    {"propertyName": "zip", "operator": "EQ", "value": patient["Zipcode"]}
                ]
                contacts = search_hubspot_contact_by_properties(api_key, filters)
                if len(contacts) == 1:
                    contact_to_update = contacts[0]
                elif len(contacts) > 1:
                    print(f"Warning: Found multiple contacts for address {patient['Address1']}, {patient['City']}, {patient['State']}. Skipping update.")

            # Upsert logic
            if contact_to_update:
                # Update existing contact
                contact_id = contact_to_update["id"]
                update_hubspot_contact(api_key, contact_id, patient)
            else:
                # Create new contact
                create_hubspot_contact(api_key, patient)

        print("Patient sync process completed successfully.")
        return ("Patient sync completed successfully.", 200)

    except Exception as e:
        error_message = f"An error occurred during the patient sync process: {e}"
        print(error_message)
        return (error_message, 500)

def sync_rois(request):
    """
    Cloud Function entry point for syncing AMD ROIs to Hubspot custom objects.
    This function can be triggered by an HTTP request or Cloud Scheduler.
    """
    print("Starting ROI sync process...")

    try:
        # 1. Get Hubspot API Key
        api_key = get_hubspot_api_key()

        # 2. Get ROIs from BigQuery
        rois = get_amd_rois_from_bigquery()
        print(f"Found {len(rois)} ROIs to process from BigQuery.")

        # 3. Process each ROI
        for roi in rois:
            if not roi.get("roi_id"):
                print(f"Warning: Skipping ROI with missing roi_id.")
                continue

            existing_roi = search_hubspot_roi(api_key, roi["roi_id"])

            if existing_roi:
                # Update existing ROI
                object_id = existing_roi["id"]
                update_hubspot_roi(api_key, object_id, roi)
            else:
                # Create new ROI
                create_hubspot_roi(api_key, roi)

        print("ROI sync process completed successfully.")
        return ("ROI sync process completed successfully.", 200)

    except Exception as e:
        error_message = f"An error occurred during the ROI sync process: {e}"
        print(error_message)
        return (error_message, 500)

# --- For Local Testing ---
if __name__ == "__main__":
    # To test locally, you'll need to have Application Default Credentials set up:
    # `gcloud auth application-default login`
    # You'll also need a .env file with GCP_PROJECT_ID and HUBSPOT_API_SECRET_NAME if they are not the default.
    # The Hubspot API key will be fetched from Secret Manager.
    sync_patients(None)
    sync_rois(None)
