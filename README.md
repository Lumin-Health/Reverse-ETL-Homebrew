[README.md](https://github.com/user-attachments/files/22407800/README.md)
# Reverse ETL Homebrew: AdvancedMD to Hubspot Sync

## Project Overview

This project implements a custom Reverse ETL solution to synchronize patient data from AdvancedMD (AMD) and Request for Information (ROI) data from AMD into Hubspot. The goal is to reconcile live patient data with workable lead data in Hubspot and enable provider relationship personnel to integrate ROIs into their nurturing pipelines. This solution leverages Google Cloud Platform (GCP) services for durability, scalability, and HIPAA compliance.

## Goals & Key Performance Indicators (KPIs)

*   **Patient Data Sync**: All patient data from AMD's `lumininternal.amd.PatientsWithStatistics` mapped to Hubspot "Contact" objects.
    *   **KPI**: Patient data accurately mapped to lead data in Hubspot via a durable composite join key (First/Last Name, Email, DOB, Physical Address).
*   **ROI Data Sync**: All ROIs from AMD's `lumininternal.amd.ROIs` mapped to a custom "ROI" object in Hubspot.
    *   **KPI**: ROIs mapped to Hubspot custom objects, including provider and patient information, and processable via Hubspot workflows.

## Architecture

The solution will be built on Google Cloud Platform (GCP) using Python for the integration logic.

*   **Data Source**: BigQuery (`lumininternal.amd.PatientsWithStatistics`, `lumininternal.amd.ROIs`)
*   **ETL Logic**: Google Cloud Functions (Python)
*   **Scheduler**: Google Cloud Scheduler (for daily triggers)
*   **Secret Management**: Google Secret Manager (for Hubspot API key)
*   **Data Destination**: Hubspot (Contacts API, Custom Objects API)

## Data Flow

1.  **Extraction**: Cloud Functions query BigQuery tables (`lumininternal.amd.PatientsWithStatistics` and `lumininternal.amd.ROIs`) to extract relevant data.
2.  **Transformation**: Data is cleaned, standardized, and mapped to Hubspot's expected schema. Date/time fields are converted to `America/New_York` timezone.
3.  **Loading**: Cloud Functions perform upsert operations to Hubspot:
    *   **Patients**: Matched to existing Hubspot Contacts using a three-tiered composite key (Email -> FirstName/LastName/DOB -> Address). If no match, a new Contact is created.
    *   **ROIs**: Matched to existing custom ROI objects using `AMD ROIs.roi_id` to `Hubspot ROIs.property_roi_id`. If no match, a new custom ROI object is created.

## Implementation Details

### Patient Data Sync

*   **Source**: `lumininternal.amd.PatientsWithStatistics`
*   **Destination**: Hubspot "Contact" object
*   **Join Key Logic**:
    1.  **Tier 1**: `Email` (case-insensitive, trimmed)
    2.  **Tier 2 (if Tier 1 fails)**: `FirstName`, `LastName`, `DOB` (`property_ncf_date_of_birth`)
    3.  **Tier 3 (if Tier 2 fails)**: `Address1`, `Address2`, `City`, `State`, `Zipcode`
    *   If no match after Tier 3, a new Hubspot Contact is created.
*   **Field Mappings**:
    *   `FirstName` -> `property_firstname`
    *   `LastName` -> `property_lastname`
    *   `DOB` -> `property_ncf_date_of_birth`
    *   `Gender` -> `property_gender`
    *   `Email` -> `property_email`
    *   `Address1` -> `property_address`
    *   `Address2` -> `property_street_address_line_2`
    *   `City` -> `property_city`
    *   `State` -> `property_state`
    *   `Zipcode` -> `property_zip`
    *   `HomePhone` -> `property_phone`
    *   Additional fields like `ID`, `Chart`, `SpravatosToDate`, `KetaminesToDate`, `TreatmentsToDate`, `FutureTreatmentCount`, `FutureFollowUpCount`, `PrimaryFacility`, `PrimaryFacilityCode`, `FirstInitialConsult`, `Started`, `Active` will be mapped to existing or new custom properties in Hubspot Contact.

### ROI Data Sync

*   **Source**: `lumininternal.amd.ROIs`
*   **Destination**: Hubspot Custom "ROI" object
*   **Unique Identifier**: `AMD ROIs.roi_id` -> `Hubspot ROIs.property_roi_id`
*   **Field Mappings**:
    *   `AMD ROIs.roi_id` -> `property_roi_id`
    *   `AMD ROIs.TemplateID` -> `property_amd_template_id` (new custom property, must be created in Hubspot)
    *   `AMD ROIs.TemplateName` -> `property_roi_type`
    *   `PatientID` -> `property_patient_id`
    *   `PatientChart` -> `property_patient_chart`
    *   `AcceptedDatetime` -> `property_accepted_datetime`
    *   `CompletedDate` -> `property_completed_date`
    *   `Patient Name` -> `property_patient_signed_name`
    *   `DOB` -> `property_patient_signed_dob`
    *   `ProviderName` -> `property_raw_provider_name`
    *   `Specialty` -> `property_raw_provider_specialty`
    *   `Email` -> `property_raw_provider_email`
    *   `Phone` -> `property_raw_provider_phone`
    *   `Fax` -> `property_raw_provider_fax`
*   **Associations**: Associations between ROI objects and Contacts will be handled by Hubspot workflows, not by this integration.

## Security & Compliance (HIPAA)

*   **Encryption**: Data encrypted at rest (GCP defaults) and in transit (HTTPS for API calls).
*   **Access Control**: Granular IAM roles for Cloud Functions to access BigQuery and Secret Manager (least privilege).
*   **API Keys**: Hubspot API key stored securely in Google Secret Manager.

## Error Handling, Logging & Monitoring

*   **Logging**: Comprehensive and deep logging to Google Cloud Logging for all operations (successes, warnings, errors).
*   **Alerting**: Cloud Monitoring alerts for Cloud Function failures or high error rates.
*   **Retries**: Exponential backoff and retry mechanisms for transient API errors.
*   **Notifications**: Critical errors will trigger an email to `alec.sherman@lumin.health` with error messages, log steps, and progress.

## Deployment & Operations

*   **GCP Project**: `lumininternal`
*   **Initial Load**: A distinct, one-time script will be used for the initial bulk load of existing data.
*   **Daily Sync**: Cloud Scheduler will trigger Cloud Functions daily.
*   **Code Repository**: Dedicated directory within the current sandbox.
*   **Deployment**: `gcloud functions deploy` from the dedicated directory.
*   **Testing**: A staging Hubspot environment is available for testing.

## Pre-Deployment Checklist (Action Items for User)

Before proceeding to Act mode, please ensure the following:

*   **Hubspot API Key**: Ensure the Hubspot API key has the necessary scopes: `crm.objects.contacts.read`, `crm.objects.contacts.write`, `crm.objects.custom_objects.read`, `crm.objects.custom_objects.write`.
*   **GCP Permissions**: Provide necessary permissions for enabling Google Cloud APIs (Cloud Functions, Cloud Scheduler, Secret Manager, BigQuery) and creating/configuring service accounts with appropriate IAM roles in the `lumininternal` project.
*   **Hubspot Custom Property**: Confirm that the custom property `property_amd_template_id` has been created in your Hubspot instance.
</content>
<task_progress>
- [x] Gather initial requirements and context
- [x] Propose architecture and detailed implementation plan
- [ ] Implement BigQuery data extraction for Patients
- [ ] Implement Hubspot Contact matching and upsert logic
- [ ] Implement BigQuery data extraction for ROIs
-   **Hubspot Custom Property**: Confirm that the custom property `property_amd_template_id` has been created in your Hubspot instance.
