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
API_URL = 'https://app.zitemanager.org/api/v2/reports-file/?report_id=9776&key=3XUnDvTu9hGTW3r2TIZYlBhocQA2671372018'
REPEAT_API_URL = 'https://app.zitemanager.org/api/v2/reports-file/?report_id=9776&key=3XUnDvTu9hGTW3r2TIZYlBhocQA2671372018&file_type=repeat'
SERVICE_ACCOUNT_EMAIL = 'incident-alert@incidentalert-490412.iam.gserviceaccount.com'

# PASTE YOUR GITHUB RAW LOGO URL HERE:
LOGO_URL = 'https://raw.githubusercontent.com/smcopt/incident_alert/main/CountryLogo_Palestine_V01.png'

# --- SORT SETTINGS ---
# Field used to order rows (matched by leaf name). The incident-date values are ISO
# 'YYYY-MM-DD', which sort chronologically as plain text. To order by the report date
# instead, set this to 'date_report'.
SORT_DATE_FIELD = 'incident_date'
SORT_NEWEST_FIRST = False   # True = most recent at the top; set False for oldest-first
SHEET_TAB_NAME = 'ALERT'

# Response items: (api_suffix, display label). The main form carries a requested
# quantity (qty_<suffix>), a delivered total (qty_delivered_<suffix>) and a remaining
# total (qty_remaining_<suffix>) for each. All are "number of HH needing <item>".
ITEM_FIELDS = [
    ("tents", "Tents"),
    ("tarpaulins", "Tarpaulins / plastic sheets"),
    ("sandbags", "Sandbags"),
    ("bedding", "Bedding kits"),
    ("nfi_kit", "NFI kits"),
    ("clothing", "Clothing"),
    ("rodent_control", "Rodent / pest control"),
    ("medical_hygiene", "Medical / hygiene"),
    ("food_parcels", "Food parcels"),
    ("cash", "Cash assistance"),
    ("latrines", "Latrines / toilet access"),
    ("water", "Water / water tanks"),
    ("engineering", "Engineering / drainage"),
    ("shelter_repair", "Shelter repair materials"),
    ("lighting", "Lighting"),
    ("fuel", "Fuel / heating"),
    ("pss", "PSS support"),
    ("other", "Other assistance"),
]


def _to_int(v):
    """Best-effort integer from an API value that may be '', None, '5' or 5."""
    try:
        s = str(v).strip()
        return int(float(s)) if s not in ('', 'nan', 'None') else 0
    except (ValueError, TypeError):
        return 0

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

        # 4. Fetch API Data (main form)
        response = requests.get(API_URL)
        api_data = response.json()

        # 4b. Fetch the RESPONSE repeat-group and index it by Case ID.
        # Each delivery entry links back to a main-form case via its 'Case ID'.
        repeat_map = defaultdict(list)
        try:
            repeat_resp = requests.get(REPEAT_API_URL)
            repeat_data = repeat_resp.json() or []
            for rrow in repeat_data:
                r_occ = build_occ(rrow)
                r_cid = get_case_id(rrow)
                if not r_cid:
                    continue
                repeat_map[r_cid].append({
                    "item": str(rget(rrow, r_occ, 'd_item', 0, '')).strip(),
                    "agency": str(rget(rrow, r_occ, 'd_agency', 0, '')).strip(),
                    "hh": str(rget(rrow, r_occ, 'd_hh', 0, '')).strip(),
                })
            print(f"Fetched {sum(len(v) for v in repeat_map.values())} response/delivery record(s) "
                  f"across {len(repeat_map)} case(s).")
        except Exception as rep_err:
            print(f"Warning: repeat-group fetch failed, continuing without deliveries: {rep_err}")

        new_records_for_sheet = []
        new_records_for_email = []
        new_deliveries = []      # flat repeat-group rows joined with parent core info
        new_raw_records = []     # raw API records (native new-form columns) for the internal Excel
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

                    # --- Handle "Other" Logic (event_type / event_type_other) ---
                    raw_main_incident = str(rget(item, occ, 'event_type', 0, '')).strip()
                    raw_other_incident = str(rget(item, occ, 'event_type_other', 0, '')).strip()
                    
                    if not raw_main_incident or raw_main_incident.lower() == 'other':
                        final_main_incident = raw_other_incident if raw_other_incident else 'N/A'
                    else:
                        final_main_incident = raw_main_incident

                    # Deliveries linked from the response repeat-group by Case ID
                    deliveries = repeat_map.get(case_id, [])
                    deliveries_text = "; ".join(
                        f"{d['agency'] or 'N/A'}: {d['item'] or 'N/A'}"
                        + (f" ({d['hh']} HH)" if d['hh'] else "")
                        for d in deliveries
                    ) if deliveries else ""

                    # Per-item ask vs delivered vs remaining (only items with any activity)
                    needs_breakdown = []
                    for suffix, label in ITEM_FIELDS:
                        ask = _to_int(rget(item, occ, f'qty_{suffix}', 0, ''))
                        delivered = _to_int(rget(item, occ, f'qty_delivered_{suffix}', 0, ''))
                        remaining = _to_int(rget(item, occ, f'qty_remaining_{suffix}', 0, ''))
                        if ask or delivered or remaining:
                            needs_breakdown.append({
                                "item": label, "ask": ask, "delivered": delivered, "remaining": remaining
                            })

                    governorate = rget(item, occ, 'Region Information/First Level Region Name', 0, 'N/A')
                    neighborhood = rget(item, occ, 'Region Information/Second Level Region Name', 0, 'N/A')
                    response_status = str(rget(item, occ, 'response_provided', 0, 'N/A')).strip() or 'N/A'

                    # Create a structured dictionary of the renamed fields for Email & External Excel
                    email_incident = {
                        "Site ID": rget(item, occ, 'Site ID', 0, 'N/A'),
                        "Site Name": rget(item, occ, 'Site Name', 0, 'N/A'),
                        "Site Name (Arabic)": rget(item, occ, 'Site Information/Site Name (Arabic)', 0, 'N/A'),
                        "Governorate": governorate,
                        "Neighborhood": neighborhood,
                        "Date of Incident": rget(item, occ, 'incident_date', 0, 'N/A'),
                        "Report Type": rget(item, occ, 'report_type', 0, ''),
                        "Agency Name": rget(item, occ, 'Agency_name', 0, 'N/A'),
                        "Site Type": rget(item, occ, 'Site Information/Site Type', 0, ''),
                        "Name of Reporter": rget(item, occ, 'NameReporter', 0, 'N/A'),
                        "Reporter Contact Information": rget(item, occ, 'Please_provide_the_r_we_need_to_follow_up', 0, 'N/A'),
                        "Main Incident": final_main_incident,
                        "Impact / Result": rget(item, occ, 'impacts', 0, 'N/A'),
                        "Details About the Incident": rget(item, occ, 'event_narrative', 0, 'N/A'),
                        "Individuals Affected": str(rget(item, occ, 'individuals', 0, '0')),
                        "Households Affected": str(rget(item, occ, 'households', 0, '0')),
                        "Shelters Completely Damaged": str(rget(item, occ, 'total_shelter_damage', 0, '0')),
                        "Shelters Partially Damaged": str(rget(item, occ, 'partially_damage', 0, '0')),
                        "HHs Sleeping Outside Shelter": str(rget(item, occ, 'outside', 0, '0')),
                        "Priority Needs": rget(item, occ, 'incident_needs', 0, 'N/A'),
                        # --- Response section ---
                        "Response Status": response_status,
                        "Has Remaining Need": str(rget(item, occ, 'has_remaining_need', 0, '')).strip(),
                        "Total Remaining (units)": str(rget(item, occ, 'total_remaining_hh', 0, '')).strip(),
                        "Response Deliveries": deliveries_text,
                        "_deliveries": deliveries,       # structured list for HTML rendering
                        "_needs": needs_breakdown,       # per-item ask/delivered/remaining
                        "URL": rget(item, occ, 'Url', 0, '#')
                    }
                    new_records_for_email.append(email_incident)
                    new_raw_records.append(item)   # native new-form columns for the internal Excel

                    # Flat repeat-group rows: each delivery joined with parent core info
                    for d in deliveries:
                        new_deliveries.append({
                            "Case ID": case_id,
                            "Site ID": email_incident["Site ID"],
                            "Site Name": email_incident["Site Name"],
                            "Date of Incident": email_incident["Date of Incident"],
                            "Governorate": governorate,
                            "Neighborhood": neighborhood,
                            "Main Incident": final_main_incident,
                            "Response Status": response_status,
                            "Delivered Item": d.get("item", ""),
                            "Delivering Agency": d.get("agency", ""),
                            "Households Reached": d.get("hh", ""),
                        })
                
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
            new_raw_records.sort(key=lambda it: str(rget(it, build_occ(it), 'incident_date', 0, '')),
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
            send_beautified_email(gmail_service, new_records_for_email, full_data=data_to_excel,
                                  headers=output_header, deliveries=new_deliveries,
                                  full_records=new_raw_records)
        else:
            send_beautified_email(gmail_service, None)

        return "Success", 200

    except Exception as e:
        print(f"Error: {e}")
        return f"Error: {e}", 500

def send_beautified_email(service, summary_data, full_data=None, headers=None, deliveries=None, full_records=None):
    # 1. Force the server to use Amman Timezone (UTC+3)
    amman_tz = timezone(timedelta(hours=3))
    
    # 2. Get the exact current time in Amman
    amman_time = datetime.now(amman_tz)

    # 3. Calculate today and yesterday based securely on Amman time
    current_date = amman_time.strftime("%d-%m-%Y")
    report_date = current_date

    # Columns for the "Response Deliveries" sheet (added to BOTH Excel files)
    delivery_cols = [
        "Case ID", "Site ID", "Site Name", "Date of Incident", "Governorate", "Neighborhood",
        "Main Incident", "Response Status", "Delivered Item", "Delivering Agency", "Households Reached"
    ]

    def _style_sheet(ws):
        """Bold header + auto width (max 50), shared by every sheet we write."""
        for cell in ws[1]:
            cell.font = Font(bold=True)
        for col in ws.columns:
            max_length = 0
            column_letter = col[0].column_letter
            for cell in col:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except Exception:
                    pass
            ws.column_dimensions[column_letter].width = min((max_length + 2), 50)

    message = MIMEMultipart()
    message['to'] = RECIPIENT_EMAIL
    message['from'] = SENDER_EMAIL
    message['subject'] = f"Daily Incident Summary - SM Cluster ({report_date})"
    


    # --- 1. ATTACH FULL INTERNAL EXCEL (raw new-form data + Response Deliveries sheet) ---
    # Dump the records with their own native columns (in form order) rather than forcing
    # them into the Google Sheet's header — that mismatch was the source of the "jumble".
    if full_records:
        # Column order = union of all record keys, preserving first-seen (form) order.
        incident_cols = []
        for rec in full_records:
            for k in rec.keys():
                if k not in incident_cols:
                    incident_cols.append(k)
        full_buffer = io.BytesIO()
        with pd.ExcelWriter(full_buffer, engine='openpyxl') as writer:
            pd.DataFrame(full_records, columns=incident_cols).to_excel(writer, index=False, sheet_name='Incidents')
            _style_sheet(writer.sheets['Incidents'])
            df_del = pd.DataFrame(deliveries or [], columns=delivery_cols)
            df_del.to_excel(writer, index=False, sheet_name='Response Deliveries')
            _style_sheet(writer.sheets['Response Deliveries'])
        full_buffer.seek(0)
        
        part_full = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        part_full.set_payload(full_buffer.read())
        encoders.encode_base64(part_full)
        part_full.add_header('Content-Disposition', f'attachment; filename="Internal_SMC Site Alert - {current_date}.xlsx"')
        message.attach(part_full)

    # --- 2. ATTACH TRUNCATED & FORMATTED EXTERNAL EXCEL (+ Response Deliveries sheet) ---
    if summary_data:
        # Define the exact columns for the external sheet (excludes 'URL')
        ext_cols = [
            "Site ID", "Site Name", "Site Name (Arabic)", "Governorate", "Neighborhood",
            "Date of Incident", "Report Type", "Agency Name", "Name of Reporter", "Reporter Contact Information",
            "Main Incident", "Impact / Result", "Details About the Incident", "Individuals Affected",
            "Households Affected", "Shelters Completely Damaged", "Shelters Partially Damaged",
            "HHs Sleeping Outside Shelter", "Priority Needs",
            "Response Status", "Has Remaining Need", "Total Remaining (units)", "Response Deliveries"
        ]
        
        df_ext = pd.DataFrame(summary_data, columns=ext_cols)
        ext_buffer = io.BytesIO()
        
        # Apply formatting to the external Excel file
        with pd.ExcelWriter(ext_buffer, engine='openpyxl') as writer:
            df_ext.to_excel(writer, index=False, sheet_name='Site Alerts')
            _style_sheet(writer.sheets['Site Alerts'])
            df_del = pd.DataFrame(deliveries or [], columns=delivery_cols)
            df_del.to_excel(writer, index=False, sheet_name='Response Deliveries')
            _style_sheet(writer.sheets['Response Deliveries'])

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
            # Response status badge colour (keeps existing palette)
            status = (r.get('Response Status') or 'N/A')
            sl = status.lower()
            if 'fully' in sl:
                badge_bg = '#1B657C'      # teal   = fully responded
            elif 'partial' in sl:
                badge_bg = '#D4A373'      # tan    = partially responded
            else:
                badge_bg = '#EC6B4D'      # orange = not yet / unknown
            # Linked deliveries (from the response repeat-group) as a table
            dels = r.get('_deliveries') or []
            if dels:
                _rows = "".join(
                    "<tr>"
                    f"<td style='padding:4px 8px; border-bottom:1px solid #EEE; color:#3D405B;'>{d.get('agency') or 'N/A'}</td>"
                    f"<td style='padding:4px 8px; border-bottom:1px solid #EEE; color:#3D405B;'>{d.get('item') or 'N/A'}</td>"
                    f"<td style='padding:4px 8px; border-bottom:1px solid #EEE; color:#3D405B; text-align:center;'>{d.get('hh') or '-'}</td>"
                    "</tr>" for d in dels
                )
                deliveries_html = (
                    "<table width='100%' cellpadding='0' cellspacing='0' style='margin-top:6px; border-collapse:collapse; font-size:12px;'>"
                    "<tr style='background-color:#F5F3E8;'>"
                    "<th style='padding:5px 8px; text-align:left; color:#1B657C; border-bottom:1px solid #D4A373;'>Agency</th>"
                    "<th style='padding:5px 8px; text-align:left; color:#1B657C; border-bottom:1px solid #D4A373;'>Item</th>"
                    "<th style='padding:5px 8px; text-align:center; color:#1B657C; border-bottom:1px solid #D4A373;'>HH Reached</th>"
                    f"</tr>{_rows}</table>"
                )
            else:
                deliveries_html = "<span style='font-size:11px; color:#3D405B;'>No deliveries recorded yet.</span>"
            # Per-item ask vs delivered vs remaining as a table
            needs = r.get('_needs') or []
            if needs:
                _nrows = "".join(
                    "<tr>"
                    f"<td style='padding:4px 8px; border-bottom:1px solid #EEE; color:#3D405B;'>{n['item']}</td>"
                    f"<td style='padding:4px 8px; border-bottom:1px solid #EEE; color:#3D405B; text-align:center;'>{n['ask']}</td>"
                    f"<td style='padding:4px 8px; border-bottom:1px solid #EEE; color:#3D405B; text-align:center;'>{n['delivered']}</td>"
                    f"<td style='padding:4px 8px; border-bottom:1px solid #EEE; color:#EC6B4D; text-align:center; font-weight:bold;'>{n['remaining']}</td>"
                    "</tr>" for n in needs
                )
                needs_html = (
                    "<table width='100%' cellpadding='0' cellspacing='0' style='margin-top:6px; border-collapse:collapse; font-size:12px;'>"
                    "<tr style='background-color:#F5F3E8;'>"
                    "<th style='padding:5px 8px; text-align:left; color:#1B657C; border-bottom:1px solid #D4A373;'>Item</th>"
                    "<th style='padding:5px 8px; text-align:center; color:#1B657C; border-bottom:1px solid #D4A373;'>Requested (HH)</th>"
                    "<th style='padding:5px 8px; text-align:center; color:#1B657C; border-bottom:1px solid #D4A373;'>Delivered (HH)</th>"
                    "<th style='padding:5px 8px; text-align:center; color:#1B657C; border-bottom:1px solid #D4A373;'>Remaining (HH)</th>"
                    f"</tr>{_nrows}</table>"
                )
            else:
                needs_html = "<span style='font-size:11px; color:#3D405B;'>No itemised needs recorded.</span>"
            content_html += f"""
            <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 20px; border: 1px solid #D4A373; border-radius: 8px; font-family: Arial, sans-serif; background-color: #ffffff; overflow: hidden;">
                <!-- Identity header -->
                <tr>
                    <td style="padding: 16px 18px; background-color: #F5F3E8; border-bottom: 1px solid #D4A373;">
                        <table width="100%" cellpadding="0" cellspacing="0">
                            <tr>
                                <td align="left" valign="top">
                                    <h3 style="margin: 0; color: #1B657C; font-size: 18px;">{r.get('Site ID')} &middot; {r.get('Site Name')}</h3>
                                    <p style="margin: 4px 0 0 0; font-size: 13px; color: #3D405B;">{r.get('Governorate')} &nbsp;&rsaquo;&nbsp; {r.get('Neighborhood')}</p>
                                    <p style="margin: 2px 0 0 0; font-size: 12px; color: #3D405B;">Incident Date: {r.get('Date of Incident')}{(' &nbsp;|&nbsp; via ' + r.get('Report Type')) if r.get('Report Type') else ''}</p>
                                </td>
                                <td align="right" valign="top">
                                    <span style="display: inline-block; padding: 6px 12px; background-color: #EC6B4D; color: #F5F3E8; border-radius: 4px; font-weight: bold; font-size: 12px;">{r.get('Main Incident')}</span>
                                    <div style="margin-top: 8px;"><span style="display: inline-block; padding: 5px 10px; background-color: {badge_bg}; color: #ffffff; border-radius: 4px; font-weight: bold; font-size: 11px;">{status}</span></div>
                                </td>
                            </tr>
                        </table>
                    </td>
                </tr>

                <!-- OVERVIEW -->
                <tr><td style="background-color: #1B657C; color: #F5F3E8; font-size: 12px; font-weight: bold; letter-spacing: 1.2px; padding: 7px 18px;">OVERVIEW</td></tr>
                <tr><td style="padding: 6px 12px;">
                    <table width="100%" cellpadding="7" cellspacing="0" style="font-size: 13px;">
                        <tr>
                            <td width="18%" style="color: #8a8a8a;">Agency</td><td width="32%" style="color: #3D405B;">{r.get('Agency Name')}</td>
                            <td width="18%" style="color: #8a8a8a;">Reporter</td><td width="32%" style="color: #3D405B;">{r.get('Name of Reporter')}</td>
                        </tr>
                        <tr>
                            <td style="color: #8a8a8a;">Contact</td><td style="color: #3D405B;">{r.get('Reporter Contact Information')}</td>
                            <td style="color: #8a8a8a;">Site Name (Ar)</td><td style="color: #3D405B;">{r.get('Site Name (Arabic)')}</td>
                        </tr>
                    </table>
                </td></tr>

                <!-- SITUATION & IMPACT -->
                <tr><td style="background-color: #1B657C; color: #F5F3E8; font-size: 12px; font-weight: bold; letter-spacing: 1.2px; padding: 7px 18px;">SITUATION &amp; IMPACT</td></tr>
                <tr><td style="padding: 6px 12px;">
                    <table width="100%" cellpadding="7" cellspacing="0" style="font-size: 13px;">
                        <tr>
                            <td width="25%" style="color: #8a8a8a;">Individuals Affected</td><td width="25%" style="color: #3D405B; font-weight: bold;">{r.get('Individuals Affected')}</td>
                            <td width="25%" style="color: #8a8a8a;">Households Affected</td><td width="25%" style="color: #3D405B; font-weight: bold;">{r.get('Households Affected')}</td>
                        </tr>
                        <tr>
                            <td style="color: #8a8a8a;">Shelters Destroyed</td><td style="color: #3D405B;">{r.get('Shelters Completely Damaged')}</td>
                            <td style="color: #8a8a8a;">Shelters Damaged</td><td style="color: #3D405B;">{r.get('Shelters Partially Damaged')}</td>
                        </tr>
                        <tr>
                            <td style="color: #8a8a8a;">HHs Sleeping Outside</td><td style="color: #3D405B;">{r.get('HHs Sleeping Outside Shelter')}</td>
                            <td style="color: #8a8a8a; vertical-align: top;">Priority Needs</td><td style="color: #3D405B;">{r.get('Priority Needs')}</td>
                        </tr>
                    </table>
                </td></tr>

                <!-- INCIDENT NARRATIVE -->
                <tr><td style="background-color: #1B657C; color: #F5F3E8; font-size: 12px; font-weight: bold; letter-spacing: 1.2px; padding: 7px 18px;">INCIDENT NARRATIVE</td></tr>
                <tr><td style="padding: 12px 18px; font-size: 13px; color: #3D405B; line-height: 1.55;">{r.get('Details About the Incident')}</td></tr>

                <!-- RESPONSE & GAP -->
                <tr><td style="background-color: #EC6B4D; color: #F5F3E8; font-size: 12px; font-weight: bold; letter-spacing: 1.2px; padding: 7px 18px;">RESPONSE &amp; GAP</td></tr>
                <tr><td style="padding: 12px 18px;">
                    <table width="100%" cellpadding="4" cellspacing="0" style="font-size: 13px; margin-bottom: 10px;">
                        <tr>
                            <td width="18%" style="color: #8a8a8a;">Status</td>
                            <td width="32%"><span style="display: inline-block; padding: 4px 10px; background-color: {badge_bg}; color: #ffffff; border-radius: 4px; font-weight: bold; font-size: 12px;">{status}</span></td>
                            <td width="22%" style="color: #8a8a8a;">Remaining Need</td>
                            <td width="28%" style="color: #3D405B; font-weight: bold;">{r.get('Has Remaining Need') or 'N/A'}{(' (' + r.get('Total Remaining (units)') + ' HH)') if r.get('Total Remaining (units)') else ''}</td>
                        </tr>
                    </table>
                    <div style="font-size: 11px; color: #8a8a8a; text-transform: uppercase; letter-spacing: 1px; margin: 4px 0 2px 0;">Ask vs Response</div>
                    {needs_html}
                    <div style="font-size: 11px; color: #8a8a8a; text-transform: uppercase; letter-spacing: 1px; margin: 14px 0 2px 0;">Deliveries Logged</div>
                    {deliveries_html}
                </td></tr>

                <!-- Footer -->
                <tr>
                    <td style="padding: 16px; border-top: 1px solid #D4A373; background-color: #F5F3E8;" align="center">
                        <a href="{r.get('URL')}" style="display: inline-block; padding: 10px 22px; background-color: #1B657C; color: #F5F3E8; text-decoration: none; border-radius: 4px; font-weight: bold; font-size: 14px;">Review Case</a>
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
