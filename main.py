import os
import json
import time
import base64
import requests
import io
import pandas as pd
from datetime import datetime
import google.auth
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from googleapiclient.discovery import build
from openpyxl.styles import Font
from datetime import datetime, timedelta, timezone
import re
from collections import defaultdict


# --- FIELD-LOOKUP HELPERS (tolerant to API key format changes) ---
# The Zite API ships fields in two shapes that we must both support:
#   * "prefixed"  e.g. 'Details of Alert-Name of Person Completing the Form'
#   * "flattened" e.g. 'Name of Person Completing the Form' (group prefixes stripped),
#                 where names that then collide get pandas-style '.1'/'.2' suffixes
#                 (e.g. three 'Adult males (18+)' fields -> '', '.1', '.2').
# The Google Sheet header additionally carries a trailing ' [Most Recent]' tag and
# uses 'Case Id' instead of 'Case ID'.
#
# Strategy: reduce every name to a canonical "leaf" (prefix + suffix + tag removed,
# lowercased) and resolve duplicates by ORDER OF APPEARANCE. Because the deaths /
# injuries / missing blocks appear in the same order in both shapes and in the sheet
# header, occurrence-index matching is correct regardless of which shape arrives.

# Longest / most specific prefixes first so nested ones are removed before the parent.
_SECTION_PREFIX_RE = re.compile(
    r'^(details of alert-'
    r'|event information-'
    r'|impact of incident-reported [^-]+?-'     # reported deaths- / injuries- / missing persons-
    r'|impact of incident-facility damage-'
    r'|impact of incident-'
    r'|top needs-)'
)

# The API renamed the region/location group's slash-prefix from 'Site Information/' to
# 'Region Information/', but the existing sheet header still uses 'Site Information/'.
# Collapse both to a common token so old header columns match the renamed API keys.
_SLASH_PREFIX_RE = re.compile(r'^(site information/|region information/)')


def _leaf(name):
    """Canonical leaf name: drop ' [Most Recent]', drop pandas '.1'/'.2', drop the
    section/group prefix (dash groups and the Site/Region Information slash group),
    collapse whitespace, lowercase."""
    s = str(name)
    s = re.sub(r'\s*\[most recent\]\s*$', '', s, flags=re.I)   # ' [Most Recent]'
    s = re.sub(r'\.\d+$', '', s)                               # pandas '.1' / '.2'
    s = re.sub(r'\s+', ' ', s).strip().lower()
    s = _SECTION_PREFIX_RE.sub('', s)                          # 'Event Information-' etc.
    s = _SLASH_PREFIX_RE.sub('', s)                            # 'Site/Region Information/'
    return s.strip()


def build_occ(item):
    """Map each leaf name -> list of the item's actual keys, in JSON order."""
    occ = defaultdict(list)
    for k in item.keys():
        occ[_leaf(k)].append(k)
    return occ


def rget(item, occ, wanted_name, occurrence=0, default=''):
    """Fetch a value by leaf name, picking the Nth occurrence (0-based)."""
    keys = occ.get(_leaf(wanted_name), [])
    if occurrence < len(keys):
        v = item.get(keys[occurrence], default)
        return default if v is None else v
    return default


def get_case_id(item):
    """Return the case identifier regardless of 'Case ID' / 'Case Id' / spacing."""
    for k, v in item.items():
        if _leaf(k) == 'case id' and str(v).strip():
            return str(v).strip()
    return ''


# --- CONFIGURATION ---
SPREADSHEET_ID = '15cGy5EhzuR330e6XmFaAXSaokoRsFxBUugzXybPqZkw'
SENDER_EMAIL = 'info@smcopt.org'
RECIPIENT_EMAIL = 'coordination@smcopt.org'
API_URL = 'https://app.zitemanager.org/api/v2/reports-file/?report_id=2137&key=7kq1bSino0AcI86hIFbmM6mmTU425121134211' 
SERVICE_ACCOUNT_EMAIL = 'incident-alert@incidentalert-490412.iam.gserviceaccount.com'

# PASTE YOUR GITHUB RAW LOGO URL HERE:
LOGO_URL = 'https://raw.githubusercontent.com/smcopt/incident_alert/main/CountryLogo_Palestine_V01.png'

# --- SORT SETTINGS ---
# Field used to order rows (matched by leaf name). The incident-date values are ISO
# 'YYYY-MM-DD', which sort chronologically as plain text. To order by the report date
# instead, set this to 'Details of Alert-Date of report'.
SORT_DATE_FIELD = 'Details of Alert-Date of the incident'
SORT_NEWEST_FIRST = False   # True = most recent at the top; set False for oldest-first
SHEET_TAB_NAME = 'ALERT'

def run_workflow(request):
    try:
        # 1. Base Keyless Authentication
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/cloud-platform' 
        ]
        creds, project = google.auth.default(scopes=scopes)
        creds.refresh(Request())
        sheet_service = build('sheets', 'v4', credentials=creds)

        # 2. Gmail Domain-Wide Delegation
        jwt_payload = json.dumps({
            "iss": SERVICE_ACCOUNT_EMAIL,
            "sub": SENDER_EMAIL,
            "scope": "https://www.googleapis.com/auth/gmail.send",
            "aud": "https://oauth2.googleapis.com/token",
            "iat": int(time.time()),
            "exp": int(time.time()) + 3600
        })
        iam_url = f"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{SERVICE_ACCOUNT_EMAIL}:signJwt"
        iam_headers = {"Authorization": f"Bearer {creds.token}", "Content-Type": "application/json"}
        iam_response = requests.post(iam_url, headers=iam_headers, json={"payload": jwt_payload}).json()
        
        if 'error' in iam_response:
            raise Exception(f"IAM Signing Error: {iam_response['error']}")
            
        signed_jwt = iam_response.get('signedJwt')
        oauth_res = requests.post("https://oauth2.googleapis.com/token", data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": signed_jwt
        }).json()
        
        if 'error' in oauth_res:
            raise Exception(f"Gmail OAuth Error: {oauth_res.get('error_description', oauth_res)}")
            
        gmail_creds = Credentials(oauth_res['access_token'])
        gmail_service = build('gmail', 'v1', credentials=gmail_creds)

        # 3. Get Existing Data to Prevent Duplicates
        result = sheet_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range="ALERT!A:A").execute()
        existing_ids = set([str(row[0]).strip() for row in result.get('values', []) if row and str(row[0]).strip()])

        # Read the existing header row (row 1) so new rows are written in the sheet's
        # own column order, matched by leaf name regardless of the API's key format.
        hdr_result = sheet_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range="ALERT!1:1").execute()
        sheet_header = hdr_result.get('values', [[]])
        sheet_header = sheet_header[0] if sheet_header else []

        # 4. Fetch API Data
        response = requests.get(API_URL)
        api_data = response.json() 

        new_records_for_sheet = []
        new_records_for_email = []
        all_keys = []

        if api_data:
            print(f"Fetched {len(api_data)} records from API.")
            # Dynamically grab all column headers for Excel/Sheets (first-run fallback)
            for item in api_data:
                for k in item.keys():
                    if k not in all_keys:
                        all_keys.append(k)

            # Force the case-id column to always be Column A (tolerant to ID/Id variants)
            for ck in list(all_keys):
                if _leaf(ck) == 'case id':
                    all_keys.remove(ck)
                    all_keys.insert(0, ck)
                    break

            # Use the sheet's existing header order if present; otherwise (first-ever
            # run on an empty sheet) define the columns from the API and write a header.
            if sheet_header:
                output_header = sheet_header
            else:
                output_header = all_keys
                new_records_for_sheet.append(output_header)

            for item in api_data:
                occ = build_occ(item)
                case_id = get_case_id(item)

                # Exclude incidents from sites that are no longer active
                site_status = str(rget(item, occ, 'Site Information/Site Status', 0, '')).strip().lower()
                if site_status in ['inactive', 'not found']:
                    continue

                if case_id and case_id not in existing_ids:
                    # Build the row in the sheet's column order. Repeated leaf names
                    # (e.g. the deaths/injuries/missing 'Adult males (18+)' triplets)
                    # are matched by order of appearance via a per-row occurrence counter.
                    row_data = []
                    leaf_counter = defaultdict(int)
                    for col in output_header:
                        lf = _leaf(col)
                        if lf == 'case id':
                            row_data.append(case_id)
                            continue
                        idx = leaf_counter[lf]
                        leaf_counter[lf] += 1
                        row_data.append(str(rget(item, occ, col, idx, '')))
                    new_records_for_sheet.append(row_data)

                    # --- Handle "Other" Logic ---
                    raw_main_incident = str(rget(item, occ, 'Event Information-What was the main incident?', 0, '')).strip()
                    raw_other_incident = str(rget(item, occ, 'Event Information-If other, please specify', 0, '')).strip()
                    
                    if not raw_main_incident or raw_main_incident.lower() == 'other':
                        final_main_incident = raw_other_incident if raw_other_incident else 'N/A'
                    else:
                        final_main_incident = raw_main_incident

                    # Create a structured dictionary of the renamed fields for Email & External Excel
                    email_incident = {
                        "Site ID": rget(item, occ, 'Site ID', 0, 'N/A'),
                        "Site Name": rget(item, occ, 'Site Name', 0, 'N/A'),
                        "Site Name (Arabic)": rget(item, occ, 'Site Information/Site Name (Arabic)', 0, 'N/A'),
                        # 'Site Information' was renamed to 'Region Information' by the API
                        "Governorate": rget(item, occ, 'Region Information/First Level Region Name', 0, 'N/A'),
                        "Neighborhood": rget(item, occ, 'Region Information/Second Level Region Name', 0, 'N/A'),
                        "Date of Incident": rget(item, occ, 'Details of Alert-Date of the incident', 0, 'N/A'),
                        "Agency Name": rget(item, occ, 'Site Information/Site Type', 0, 'N/A'),
                        "Name of Reporter": rget(item, occ, 'Details of Alert-Name of Person Completing the Form', 0, 'N/A'),
                        "Reporter Contact Information": rget(item, occ, "Details of Alert-Please provide the reporter's contact information in case we need to follow up.", 0, 'N/A'),
                        "Main Incident": final_main_incident,
                        "Details About the Incident": rget(item, occ, 'Event Information-Details about the incident (as relevant)', 0, 'N/A'),
                        "Individuals Affected": str(rget(item, occ, 'Impact of Incident-Individuals affected', 0, '0')),
                        "Households Affected": str(rget(item, occ, 'Impact of Incident-Households affected', 0, '0')),
                        "Shelters Completely Damaged": str(rget(item, occ, 'Impact of Incident-Number of Shelters Completely Damaged', 0, '0')),
                        "Shelters Partially Damaged": str(rget(item, occ, 'Impact of Incident-Number of Shelters Partially Damaged:', 0, '0')),
                        "HHs Sleeping Outside Shelter": str(rget(item, occ, 'Impact of Incident-Number of Households sleeping outside of shelter:', 0, '0')),
                        "Quantities Required for Support": rget(item, occ, 'Top Needs-Quantities Required for Support', 0, 'N/A'),
                        "URL": rget(item, occ, 'Url', 0, '#')
                    }
                    new_records_for_email.append(email_incident)
                
        # 5. Update Sheet & Send Email
        print(f"{len(new_records_for_email)} new record(s) after dedup/active-site filtering.")
        if new_records_for_sheet:
            # --- Sort this batch by incident date (ISO 'YYYY-MM-DD' sorts as text) ---
            # Keep a first-run header row pinned at the top while sorting only data rows.
            date_idx = next((i for i, c in enumerate(output_header)
                             if _leaf(c) == _leaf(SORT_DATE_FIELD)), None)
            if not sheet_header and new_records_for_sheet:
                header_row, data_rows = new_records_for_sheet[0], new_records_for_sheet[1:]
            else:
                header_row, data_rows = None, new_records_for_sheet
            if date_idx is not None:
                data_rows.sort(key=lambda r: r[date_idx] if date_idx < len(r) else '',
                               reverse=SORT_NEWEST_FIRST)
            new_records_for_sheet = ([header_row] if header_row else []) + data_rows
            new_records_for_email.sort(key=lambda d: d.get('Date of Incident', ''),
                                       reverse=SORT_NEWEST_FIRST)

            sheet_service.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TAB_NAME}!A1",
                valueInputOption="RAW",
                body={"values": new_records_for_sheet}
            ).execute()

            # --- Keep the WHOLE sheet sorted by incident date every run ---
            # (reorders existing rows in place; does not touch values or the header)
            try:
                if date_idx is not None:
                    meta = sheet_service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
                    tab_id = next((s['properties']['sheetId'] for s in meta.get('sheets', [])
                                   if s['properties']['title'] == SHEET_TAB_NAME), None)
                    if tab_id is not None:
                        sheet_service.spreadsheets().batchUpdate(
                            spreadsheetId=SPREADSHEET_ID,
                            body={"requests": [{"sortRange": {
                                "range": {"sheetId": tab_id, "startRowIndex": 1},
                                "sortSpecs": [{
                                    "dimensionIndex": date_idx,
                                    "sortOrder": "DESCENDING" if SORT_NEWEST_FIRST else "ASCENDING"
                                }]
                            }}]}
                        ).execute()
            except Exception as sort_err:
                print(f"Warning: sheet re-sort skipped: {sort_err}")

            # Send the data to the email logic
            data_to_excel = new_records_for_sheet[1:] if not sheet_header else new_records_for_sheet
            send_beautified_email(gmail_service, new_records_for_email, full_data=data_to_excel, headers=output_header)
        else:
            send_beautified_email(gmail_service, None)

        return "Success", 200

    except Exception as e:
        print(f"Error: {e}")
        return f"Error: {e}", 500

def send_beautified_email(service, summary_data, full_data=None, headers=None):
    # 1. Force the server to use Amman Timezone (UTC+3)
    amman_tz = timezone(timedelta(hours=3))
    
    # 2. Get the exact current time in Amman
    amman_time = datetime.now(amman_tz)

    # 3. Calculate today and yesterday based securely on Amman time
    current_date = amman_time.strftime("%d-%m-%Y")
    report_date = current_date
    
    message = MIMEMultipart()
    message['to'] = RECIPIENT_EMAIL
    message['from'] = SENDER_EMAIL
    message['subject'] = f"Daily Incident Summary - SM Cluster ({report_date})"
    


    # --- 1. ATTACH FULL INTERNAL EXCEL ---
    if full_data and headers:
        df_full = pd.DataFrame(full_data, columns=headers)
        full_buffer = io.BytesIO()
        df_full.to_excel(full_buffer, index=False, engine='openpyxl')
        full_buffer.seek(0)
        
        part_full = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        part_full.set_payload(full_buffer.read())
        encoders.encode_base64(part_full)
        part_full.add_header('Content-Disposition', f'attachment; filename="Internal_SMC Site Alert - {current_date}.xlsx"')
        message.attach(part_full)

    # --- 2. ATTACH TRUNCATED & FORMATTED EXTERNAL EXCEL ---
    if summary_data:
        # Define the exact columns for the external sheet (excludes 'URL')
        ext_cols = [
            "Site ID", "Site Name", "Site Name (Arabic)", "Governorate", "Neighborhood",
            "Date of Incident", "Agency Name", "Name of Reporter", "Reporter Contact Information",
            "Main Incident", "Details About the Incident", "Individuals Affected", 
            "Households Affected", "Shelters Completely Damaged", "Shelters Partially Damaged", 
            "HHs Sleeping Outside Shelter", "Quantities Required for Support"
        ]
        
        df_ext = pd.DataFrame(summary_data, columns=ext_cols)
        ext_buffer = io.BytesIO()
        
        # Apply formatting to the external Excel file
        with pd.ExcelWriter(ext_buffer, engine='openpyxl') as writer:
            df_ext.to_excel(writer, index=False, sheet_name='Site Alerts')
            worksheet = writer.sheets['Site Alerts']
            
            # Make headers bold and auto-adjust column width (max 50 wide)
            for cell in worksheet[1]:
                cell.font = Font(bold=True)
                
            for col in worksheet.columns:
                max_length = 0
                column_letter = col[0].column_letter
                for cell in col:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(cell.value)
                    except:
                        pass
                worksheet.column_dimensions[column_letter].width = min((max_length + 2), 50)

        ext_buffer.seek(0)
        
        part_ext = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        part_ext.set_payload(ext_buffer.read())
        encoders.encode_base64(part_ext)
        part_ext.add_header('Content-Disposition', f'attachment; filename="ExternalSharing_SMC Site Alert - {current_date}.xlsx"')
        message.attach(part_ext)

    # --- 3. BUILD BEAUTIFIED HTML EMAIL (CARD LAYOUT) ---
    if not summary_data:
        status_msg = "No new incidents reported in the past 24 hours."
        content_html = "<p style='color: #3D405B; font-size: 16px; padding: 20px; text-align: center;'>All clear. No new submissions received since the last update.</p>"
    else:
        status_msg = f"Action Required: {len(summary_data)} New Incidents"
        content_html = ""
        
        # Build an HTML card for each incident
        for r in summary_data:
            content_html += f"""
            <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 20px; border: 1px solid #D4A373; border-radius: 8px; font-family: Arial, sans-serif; background-color: #ffffff;">
                <tr>
                    <td style="padding: 15px; border-bottom: 1px solid #D4A373; background-color: #F5F3E8; border-radius: 8px 8px 0 0;">
                        <table width="100%" cellpadding="0" cellspacing="0">
                            <tr>
                                <td align="left">
                                    <h3 style="margin: 0; color: #1B657C; font-size: 18px;">{r.get('Site ID')} - {r.get('Site Name')}</h3>
                                    <p style="margin: 4px 0 0 0; font-size: 13px; color: #3D405B;">{r.get('Governorate')} - {r.get('Neighborhood')}</p>
                                </td>
                                <td align="right" valign="top">
                                    <span style="display: inline-block; padding: 6px 10px; background-color: #EC6B4D; color: #F5F3E8; border-radius: 4px; font-weight: bold; font-size: 12px;">{r.get('Main Incident')}</span>
                                </td>
                            </tr>
                        </table>
                    </td>
                </tr>
                <tr>
                    <td style="padding: 15px;">
                        <table width="100%" cellpadding="6" cellspacing="0" style="font-size: 13px;">
                            <tr>
                                <td width="25%" style="color: #3D405B;"><strong>Site Name (Ar):</strong></td>
                                <td width="25%" style="color: #3D405B;">{r.get('Site Name (Arabic)')}</td>
                                <td width="25%" style="color: #3D405B;"><strong>Agency:</strong></td>
                                <td width="25%" style="color: #3D405B;">{r.get('Agency Name')}</td>
                            </tr>
                            <tr>
                                <td style="color: #3D405B;"><strong>Reporter:</strong></td>
                                <td style="color: #3D405B;">{r.get('Name of Reporter')}</td>
                                <td style="color: #3D405B;"><strong>Contact:</strong></td>
                                <td style="color: #3D405B;">{r.get('Reporter Contact Information')}</td>
                            </tr>
                            <tr>
                                <td style="color: #3D405B;"><strong>Ind. Affected:</strong></td>
                                <td style="color: #3D405B;">{r.get('Individuals Affected')}</td>
                                <td style="color: #3D405B;"><strong>HH Affected:</strong></td>
                                <td style="color: #3D405B;">{r.get('Households Affected')}</td>
                            </tr>
                            <tr>
                                <td style="color: #3D405B;"><strong>Shelters Destroyed:</strong></td>
                                <td style="color: #3D405B;">{r.get('Shelters Completely Damaged')}</td>
                                <td style="color: #3D405B;"><strong>Shelters Damaged:</strong></td>
                                <td style="color: #3D405B;">{r.get('Shelters Partially Damaged')}</td>
                            </tr>
                            <tr>
                                <td style="color: #3D405B;"><strong>HHs Sleeping Outside:</strong></td>
                                <td style="color: #3D405B;">{r.get('HHs Sleeping Outside Shelter')}</td>
                                <td style="color: #3D405B;"><strong>Quantities Required for Support:</strong></td>
                                <td style="color: #3D405B;">{r.get('Quantities Required for Support')}</td>
                            </tr>
                            <tr>
                                <td colspan="4" style="padding-top: 15px; border-top: 1px dashed #D4A373;">
                                    <strong style="color: #3D405B;">Details:</strong><br>
                                    <span style="color: #3D405B; line-height: 1.5; display: inline-block; margin-top: 5px;">{r.get('Details About the Incident')}</span>
                                </td>
                            </tr>
                        </table>
                    </td>
                </tr>
                <tr>
                    <td style="padding: 15px; border-top: 1px solid #D4A373; background-color: #F5F3E8; border-radius: 0 0 8px 8px;" align="center">
                        <a href="{r.get('URL')}" style="display: inline-block; padding: 10px 20px; background-color: #1B657C; color: #F5F3E8; text-decoration: none; border-radius: 4px; font-weight: bold; font-size: 14px;">Review Case</a>
                    </td>
                </tr>
            </table>
            """

    # Assemble the final email
    html_template = f"""
    <div style="max-width: 700px; margin: auto; border: 1px solid #D4A373; font-family: 'Segoe UI', Arial, sans-serif; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.05); background-color: #F5F3E8;">
        
        <div style="background-color: #ffffff; padding: 25px; text-align: center; border-bottom: 4px solid #1B657C;">
            <img src="{LOGO_URL}" alt="SMC Logo" style="max-height: 70px; margin-bottom: 10px; display: block; margin-left: auto; margin-right: auto;">
            <h2 style="margin: 0; color: #3D405B; font-size: 22px;">INCIDENT ALERT SYSTEM</h2>
            <p style="margin: 5px 0 0 0; font-size: 14px; color: #1B657C; font-weight: bold;">SITE MANAGEMENT CLUSTER (oPT)</p>
            <p style="margin: 5px 0 0 0; font-size: 13px; color: #3D405B;">Report Date (as of): {report_date}</p>
        </div>

        <div style="padding: 30px;">
            <h3 style="color: #EC6B4D; margin-top: 0; font-size: 18px; margin-bottom: 20px; border-bottom: 1px solid #D4A373; padding-bottom: 10px;">{status_msg}</h3>
            
            {content_html}
            
            <p style="margin-top: 30px; font-size: 13px; color: #3D405B; line-height: 1.5; text-align: center;">
                This is an automated report generated at 06:15 PM Amman Time. It summarizes all new incidents submitted to the system over the past 24 hours (as of {report_date}). <br>
                <em>Two Excel files are attached: Internal Full Data and External Truncated Data.</em>
            </p>
        </div>

        <div style="background-color: #3D405B; color: #F5F3E8; padding: 15px; text-align: center; font-size: 12px;">
            © 2026 Site Management Cluster | Automated via Google Cloud
        </div>
    </div>
    """
    
    message.attach(MIMEText(html_template, 'html'))
    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    service.users().messages().send(userId='me', body={'raw': raw_message}).execute()
