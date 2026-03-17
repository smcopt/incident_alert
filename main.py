import os
import json
import time
import base64
import requests
import google.auth
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from googleapiclient.discovery import build

# --- CONFIGURATION ---
SPREADSHEET_ID = '15cGy5EhzuR330e6XmFaAXSaokoRsFxBUugzXybPqZkw'
SENDER_EMAIL = 'info@smcopt.org'
RECIPIENT_EMAIL = 'sujanpaudel@iom.int' 
API_URL = 'https://app.zitemanager.org/api/v2/reports-file/?report_id=2137&key=7kq1bSino0AcI86hIFbmM6mmTU425121134211' 

SERVICE_ACCOUNT_EMAIL = 'incident-alert@incidentalert-490412.iam.gserviceaccount.com'

def run_workflow(request):
    try:
        # 1. Base Keyless Authentication for Google Sheets & IAM
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/cloud-platform' 
        ]
        creds, project = google.auth.default(scopes=scopes)
        creds.refresh(Request())
        sheet_service = build('sheets', 'v4', credentials=creds)

        # 2. Advanced Keyless Authentication for Gmail
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

        # 3. Get Existing Data
        result = sheet_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range="ALERT!A:A").execute()
        existing_ids = set([row[0] for row in result.get('values', []) if row])

        # 4. Fetch API Data
        response = requests.get(API_URL)
        api_data = response.json() 

        new_records_for_sheet = []
        new_records_for_email = []

        if api_data:
            # Dynamically grab all column headers from the API
            all_keys = list(api_data[0].keys())
            
            # Force 'Case Id' to always be Column A so our duplicate checker works
            if 'Case Id' in all_keys:
                all_keys.remove('Case Id')
                all_keys.insert(0, 'Case Id')

            # If the sheet is 100% empty, write the raw API headers as Row 1
            if not existing_ids:
                new_records_for_sheet.append(all_keys)

            for item in api_data:
                case_id = str(item.get('Case Id', ''))
                
                if case_id and case_id not in existing_ids:
                    # Append ALL fields to the Google Sheet based on column position
                    row_data = [str(item.get(key, '')) for key in all_keys]
                    new_records_for_sheet.append(row_data)

                    # Append only the 5 summary fields for the Email
                    email_date = item.get('Details of Alert-Date of the incident  [Most Recent]', 'N/A')
                    email_site = item.get('Site Name', 'N/A')
                    email_type = item.get('Event Information-What was the main incident? [Most Recent] ', 'N/A')
                    email_details = item.get('Event Information-Details about the incident (as relevant)  [Most Recent]', 'N/A')
                    new_records_for_email.append([case_id, email_date, email_site, email_type, email_details])
                
        # 5. Update Sheet & Send Email
        if new_records_for_sheet:
            sheet_service.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID,
                range="ALERT!A1",
                valueInputOption="RAW",
                body={"values": new_records_for_sheet}
            ).execute()
            
        if new_records_for_email:
            send_beautified_email(gmail_service, new_records_for_email)
        else:
            send_beautified_email(gmail_service, None)

        return "Success", 200

    except Exception as e:
        print(f"Error: {e}")
        return f"Error: {e}", 500

def send_beautified_email(service, new_rows):
    message = MIMEMultipart()
    message['to'] = RECIPIENT_EMAIL
    message['from'] = SENDER_EMAIL
    message['subject'] = "Daily Incident Summary - SM Cluster"

    if not new_rows:
        status_msg = "No incidents reported today."
        table_html = "<p style='color: #666;'>Systems are clear. No new submissions detected.</p>"
    else:
        status_msg = f"Action Required: {len(new_rows)} New Incidents"
        rows = ""
        for r in new_rows:
            rows += f"<tr><td style='padding:10px; border-bottom:1px solid #eee;'>{r[0]}</td><td style='padding:10px; border-bottom:1px solid #eee;'>{r[1]}</td><td style='padding:10px; border-bottom:1px solid #eee;'>{r[2]}</td><td style='padding:10px; border-bottom:1px solid #eee;'>{r[3]}</td><td style='padding:10px; border-bottom:1px solid #eee;'>{r[4]}</td></tr>"
        
        table_html = f"""
        <table style="width: 100%; border-collapse: collapse; margin-top: 20px; font-size: 14px;">
            <tr style="background-color: #f8f8f8; text-align: left;">
                <th style="padding: 10px;">Case ID</th>
                <th style="padding: 10px;">Date</th>
                <th style="padding: 10px;">Site</th>
                <th style="padding: 10px;">Type</th>
                <th style="padding: 10px;">Details</th>
            </tr>
            {rows}
        </table>"""

    html_content = f"""
    <div style="max-width: 600px; margin: auto; border: 1px solid #ddd; font-family: Arial, sans-serif;">
        <div style="background-color: #2b7a91; padding: 30px; text-align: center; color: white;">
            <h2 style="margin: 0;">INCIDENT MANAGEMENT SYSTEM</h2>
            <p style="margin: 5px 0 0 0; font-size: 14px; opacity: 0.9;">SITE MANAGEMENT CLUSTER</p>
        </div>
        <div style="padding: 40px 30px;">
            <h2 style="color: #2b7a91; margin-top: 0;">{status_msg}</h2>
            {table_html}
            <p style="margin-top: 30px;">This is an automated report generated at 06:00 AM Amman Time.</p>
        </div>
        <div style="background-color: #333; color: #ccc; padding: 20px; text-align: center; font-size: 12px;">
            © 2026 Site Management Cluster
        </div>
    </div>
    """
    message.attach(MIMEText(html_content, 'html'))
    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    service.users().messages().send(userId='me', body={'raw': raw_message}).execute()
