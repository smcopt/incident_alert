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


# --- CONFIGURATION ---
SPREADSHEET_ID = '15cGy5EhzuR330e6XmFaAXSaokoRsFxBUugzXybPqZkw'
SENDER_EMAIL = 'info@smcopt.org'
RECIPIENT_EMAIL = 'sujanpaudel@iom.int'
API_URL = 'https://app.zitemanager.org/api/v2/reports-file/?report_id=2137&key=7kq1bSino0AcI86hIFbmM6mmTU425121134211' 
SERVICE_ACCOUNT_EMAIL = 'incident-alert@incidentalert-490412.iam.gserviceaccount.com'

# PASTE YOUR GITHUB RAW LOGO URL HERE:
LOGO_URL = 'https://raw.githubusercontent.com/smcopt/incident_alert/main/CountryLogo_Palestine_V01.png'

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
        existing_ids = set([row[0] for row in result.get('values', []) if row])

        # 4. Fetch API Data
        response = requests.get(API_URL)
        api_data = response.json() 

        new_records_for_sheet = []
        new_records_for_email = []
        all_keys = []

        if api_data:
            # Dynamically grab all column headers for Excel/Sheets
            for item in api_data:
                for k in item.keys():
                    if k not in all_keys:
                        all_keys.append(k)
            
            # Force 'Case Id' to always be Column A
            if 'Case Id' in all_keys:
                all_keys.remove('Case Id')
                all_keys.insert(0, 'Case Id')

            if not existing_ids:
                new_records_for_sheet.append(all_keys)

            for item in api_data:
                case_id = str(item.get('Case Id', ''))
                
                if case_id and case_id not in existing_ids:
                    # Append ALL fields for Sheet/Excel
                    row_data = [str(item.get(key, '')) for key in all_keys]
                    new_records_for_sheet.append(row_data)

                    # --- Handle "Other" Logic ---
                    raw_main_incident = str(item.get('Event Information-What was the main incident? [Most Recent]', '')).strip()
                    raw_other_incident = str(item.get('Event Information-If other, please specify  [Most Recent]', '')).strip()
                    
                    if not raw_main_incident or raw_main_incident.lower() == 'other':
                        final_main_incident = raw_other_incident if raw_other_incident else 'N/A'
                    else:
                        final_main_incident = raw_main_incident

                    # Create a structured dictionary of the renamed fields for Email & External Excel
                    email_incident = {
                        "Site ID": item.get('Site ID', 'N/A'),
                        "Site Name": item.get('Site Name', 'N/A'),
                        "Site Name (Arabic)": item.get('Site Information/Site Name (Arabic)', 'N/A'),
                        "Governorate": item.get('Site Information/First Level Region Name', 'N/A'),
                        "Neighborhood": item.get('Site Information/Second Level Region Name', 'N/A'),
                        "Agency Name": item.get('Site Information/Site Type', 'N/A'),
                        "Name of Reporter": item.get('Details of Alert-Name of Person Completing the Form  [Most Recent]', 'N/A'),
                        "Reporter Contact Information": item.get("Details of Alert-Please provide the reporter's contact information in case we need to follow up.  [Most Recent]", 'N/A'),
                        "Main Incident": final_main_incident,
                        "Details About the Incident": item.get('Event Information-Details about the incident (as relevant)  [Most Recent]', 'N/A'),
                        "Individuals Affected": str(item.get('Impact of Incident-Individuals affected  [Most Recent]', '0')),
                        "Households Affected": str(item.get('Impact of Incident-Households affected  [Most Recent]', '0')),
                        "Shelters Completely Damaged": str(item.get('Impact of Incident-Number of Shelters Completely Damaged  [Most Recent]', '0')),
                        "Shelters Partially Damaged": str(item.get('Impact of Incident-Number of Shelters Partially Damaged:  [Most Recent]', '0')),
                        "HHs Sleeping Outside Shelter": str(item.get('Impact of Incident-Number of Households sleeping outside of shelter:  [Most Recent]', '0')),
                        "Quantities Required for Support": item.get('Top Needs-Quantities Required for Support  [Most Recent]', 'N/A'),
                        "URL": item.get('Url', '#')
                    }
                    new_records_for_email.append(email_incident)
                
        # 5. Update Sheet & Send Email
        if new_records_for_sheet:
            sheet_service.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID,
                range="ALERT!A1",
                valueInputOption="RAW",
                body={"values": new_records_for_sheet}
            ).execute()
            
            # Send the data to the email logic
            data_to_excel = new_records_for_sheet[1:] if not existing_ids else new_records_for_sheet
            send_beautified_email(gmail_service, new_records_for_email, full_data=data_to_excel, headers=all_keys)
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
    report_date = (amman_time - timedelta(days=1)).strftime("%d-%m-%Y")
    
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
            "Agency Name", "Name of Reporter", "Reporter Contact Information", 
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
        status_msg = "No incidents reported yesterday."
        content_html = "<p style='color: #3D405B; font-size: 16px; padding: 20px; text-align: center;'>All clear. No new submissions detected.</p>"
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
            <p style="margin: 5px 0 0 0; font-size: 13px; color: #3D405B;">Report Date: {report_date}</p>
        </div>

        <div style="padding: 30px;">
            <h3 style="color: #EC6B4D; margin-top: 0; font-size: 18px; margin-bottom: 20px; border-bottom: 1px solid #D4A373; padding-bottom: 10px;">{status_msg}</h3>
            
            {content_html}
            
            <p style="margin-top: 30px; font-size: 13px; color: #3D405B; line-height: 1.5; text-align: center;">
                This is an automated report generated at 06:00 AM Amman Time. It summarizes incidents from the previous day. <br>
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
