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

                    # Create a structured dictionary of the 17 fields for the Email Cards
                    email_incident = {
                        "Site ID": item.get('Site ID', 'N/A'),
                        "Site Name": item.get('Site Name', 'N/A'),
                        "Site Name (Arabic)": item.get('Site Information/Site Name (Arabic)', 'N/A'),
                        "Governorate": item.get('Site Information/First Level Region Name', 'N/A'),
                        "Neighborhood": item.get('Site Information/Second Level Region Name', 'N/A'),
                        "Agency Name": item.get('Site Information/Site Type', 'N/A'),
                        "Reporter": item.get('Details of Alert-Name of Person Completing the Form [Most Recent]', 'N/A'),
                        "Contact": item.get("Details of Alert-Please provide the reporter's contact information in case we need to follow up. [Most Recent]", 'N/A'),
                        "Incident": item.get('Event Information-What was the main incident? [Most Recent] ', 'N/A'),
                        "Details": item.get('Event Information-Details about the incident (as relevant)  [Most Recent]', 'N/A'),
                        "Ind_Affected": str(item.get('Impact of Incident-Individuals affected [Most Recent]', '0')),
                        "HH_Affected": str(item.get('Impact of Incident-Households affected [Most Recent]', '0')),
                        "Shelters_Destroyed": str(item.get('Impact of Incident-Number of Shelters Completely Damaged [Most Recent]', '0')),
                        "Shelters_Damaged": str(item.get('Impact of Incident-Number of Shelters Partially Damaged: [Most Recent]', '0')),
                        "Sleeping_Outside": str(item.get('Impact of Incident-Number of Households sleeping outside of shelter: [Most Recent]', '0')),
                        "Quantities": item.get('Top Needs-Quantities Required for Support [Most Recent]', 'N/A'),
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
    message = MIMEMultipart()
    message['to'] = RECIPIENT_EMAIL
    message['from'] = SENDER_EMAIL
    message['subject'] = "Daily Incident Summary - SM Cluster"

    # --- ATTACH EXCEL IF WE HAVE DATA ---
    if full_data and headers:
        df = pd.DataFrame(full_data, columns=headers)
        excel_buffer = io.BytesIO()
        df.to_excel(excel_buffer, index=False, engine='openpyxl')
        excel_buffer.seek(0)

        current_date = datetime.now().strftime("%d-%m-%Y")
        filename = f"SMC Site Alert - {current_date}.xlsx"
        
        part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        part.set_payload(excel_buffer.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
        message.attach(part)

    # --- BUILD BEAUTIFIED HTML EMAIL (CARD LAYOUT) ---
    if not summary_data:
        status_msg = "No incidents reported today."
        content_html = "<p style='color: #666; font-size: 16px; padding: 20px; text-align: center;'>Systems are clear. No new submissions detected.</p>"
    else:
        status_msg = f"Action Required: {len(summary_data)} New Incidents"
        content_html = ""
        
        # Build an HTML card for each incident
        for r in summary_data:
            content_html += f"""
            <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom: 20px; border: 1px solid #ddd; border-radius: 8px; font-family: Arial, sans-serif; background-color: #ffffff;">
                <tr>
                    <td style="padding: 15px; border-bottom: 1px solid #ddd; background-color: #f9f9f9; border-radius: 8px 8px 0 0;">
                        <table width="100%" cellpadding="0" cellspacing="0">
                            <tr>
                                <td align="left">
                                    <h3 style="margin: 0; color: #2b7a91; font-size: 18px;">{r.get('Site ID')} - {r.get('Site Name')}</h3>
                                    <p style="margin: 4px 0 0 0; font-size: 13px; color: #666;">{r.get('Governorate')} - {r.get('Neighborhood')}</p>
                                </td>
                                <td align="right" valign="top">
                                    <span style="display: inline-block; padding: 6px 10px; background-color: #ffeaea; color: #d9534f; border-radius: 4px; font-weight: bold; font-size: 12px;">{r.get('Incident')}</span>
                                </td>
                            </tr>
                        </table>
                    </td>
                </tr>
                <tr>
                    <td style="padding: 15px;">
                        <table width="100%" cellpadding="6" cellspacing="0" style="font-size: 13px;">
                            <tr>
                                <td width="25%" style="color: #666;"><strong>Site Name (Ar):</strong></td>
                                <td width="25%" style="color: #333;">{r.get('Site Name (Arabic)')}</td>
                                <td width="25%" style="color: #666;"><strong>Agency:</strong></td>
                                <td width="25%" style="color: #333;">{r.get('Agency Name')}</td>
                            </tr>
                            <tr>
                                <td style="color: #666;"><strong>Reporter:</strong></td>
                                <td style="color: #333;">{r.get('Reporter')}</td>
                                <td style="color: #666;"><strong>Contact:</strong></td>
                                <td style="color: #333;">{r.get('Contact')}</td>
                            </tr>
                            <tr>
                                <td style="color: #666;"><strong>Ind. Affected:</strong></td>
                                <td style="color: #333;">{r.get('Ind_Affected')}</td>
                                <td style="color: #666;"><strong>HH Affected:</strong></td>
                                <td style="color: #333;">{r.get('HH_Affected')}</td>
                            </tr>
                            <tr>
                                <td style="color: #666;"><strong>Shelters Destroyed:</strong></td>
                                <td style="color: #333;">{r.get('Shelters_Destroyed')}</td>
                                <td style="color: #666;"><strong>Shelters Damaged:</strong></td>
                                <td style="color: #333;">{r.get('Shelters_Damaged')}</td>
                            </tr>
                            <tr>
                                <td style="color: #666;"><strong>Sleeping Outside:</strong></td>
                                <td style="color: #333;">{r.get('Sleeping_Outside')}</td>
                                <td style="color: #666;"><strong>Needs:</strong></td>
                                <td style="color: #333;">{r.get('Quantities')}</td>
                            </tr>
                            <tr>
                                <td colspan="4" style="padding-top: 15px; border-top: 1px dashed #eee;">
                                    <strong style="color: #666;">Details:</strong><br>
                                    <span style="color: #333; line-height: 1.5; display: inline-block; margin-top: 5px;">{r.get('Details')}</span>
                                </td>
                            </tr>
                        </table>
                    </td>
                </tr>
                <tr>
                    <td style="padding: 15px; border-top: 1px solid #eee; background-color: #fafafa; border-radius: 0 0 8px 8px;" align="center">
                        <a href="{r.get('URL')}" style="display: inline-block; padding: 10px 20px; background-color: #2b7a91; color: #ffffff; text-decoration: none; border-radius: 4px; font-weight: bold; font-size: 14px;">Review Case</a>
                    </td>
                </tr>
            </table>
            """

    # Assemble the final email
    html_template = f"""
    <div style="max-width: 700px; margin: auto; border: 1px solid #e0e0e0; font-family: 'Segoe UI', Arial, sans-serif; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.05); background-color: #f4f6f8;">
        
        <div style="background-color: #ffffff; padding: 25px; text-align: center; border-bottom: 3px solid #2b7a91;">
            <img src="{LOGO_URL}" alt="SMC Logo" style="max-height: 70px; margin-bottom: 10px; display: block; margin-left: auto; margin-right: auto;">
            <h2 style="margin: 0; color: #333; font-size: 22px;">INCIDENT MANAGEMENT SYSTEM</h2>
            <p style="margin: 5px 0 0 0; font-size: 14px; color: #666; font-weight: bold;">SITE MANAGEMENT CLUSTER</p>
        </div>

        <div style="padding: 30px;">
            <h3 style="color: #d9534f; margin-top: 0; font-size: 18px; margin-bottom: 20px; border-bottom: 1px solid #ddd; padding-bottom: 10px;">{status_msg}</h3>
            
            {content_html}
            
            <p style="margin-top: 30px; font-size: 13px; color: #777; line-height: 1.5; text-align: center;">
                This is an automated report generated at 06:00 AM Amman Time.<br>
                <em>An Excel file containing the complete dataset for today's incidents is attached.</em>
            </p>
        </div>

        <div style="background-color: #333; color: #ccc; padding: 15px; text-align: center; font-size: 12px;">
            © 2026 Site Management Cluster | Automated via Google Cloud
        </div>
    </div>
    """
    
    message.attach(MIMEText(html_template, 'html'))
    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    service.users().messages().send(userId='me', body={'raw': raw_message}).execute()
