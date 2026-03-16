import os
import base64
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from googleapiclient.discovery import build
from google.oauth2 import service_account

# --- CONFIGURATION ---
SPREADSHEET_ID = 'YOUR_GOOGLE_SHEET_ID_HERE'
SENDER_EMAIL = 'info@smcopt.org'
API_URL = 'https://your-api-endpoint.com/data'
SERVICE_ACCOUNT_FILE = 'credentials.json'
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/gmail.send'
]

def run_workflow(request):
    # 1. Authenticate
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    # Important: To send as info@smcopt.org, the Service Account needs 
    # Domain-Wide Delegation, or simply share the sheet with it.
    sheet_service = build('sheets', 'v4', credentials=creds)
    gmail_service = build('gmail', 'v1', credentials=creds)

    # 2. Get Existing Data (Column A usually holds the Unique ID)
    result = sheet_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range="A:A").execute()
    existing_ids = [row[0] for row in result.get('values', []) if row]

    # 3. Fetch API Data
    response = requests.get(API_URL)
    api_data = response.json() 

    new_records = []
    for item in api_data:
        if str(item['id']) not in existing_ids:
            # Preparing row for Google Sheet (adjust keys based on your API)
            new_records.append([item['id'], item['date'], item['description'], item['status']])

    # 4. Update Sheet & Send Email
    if new_records:
        # Append to Sheet
        sheet_service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range="Sheet1!A1",
            valueInputOption="USER_ENTERED",
            body={"values": new_records}
        ).execute()
        
        send_email(gmail_service, new_records)
    else:
        send_email(gmail_service, None)

    return "Process Completed", 200

def send_email(service, new_rows):
    message = MIMEMultipart()
    message['to'] = 'recipient@smcopt.org' # Who gets the summary?
    message['from'] = SENDER_EMAIL
    message['subject'] = "Daily Incident Report - SM Cluster"

    # HTML Beautification logic (matching your screenshot style)
    if not new_rows:
        status_msg = "No incidents reported today."
        table_html = "<p style='color: #666;'>Systems are clear.</p>"
    else:
        status_msg = f"Action Required: {len(new_rows)} New Incidents"
        rows = "".join([f"<tr><td style='padding:8px; border-bottom:1px solid #eee;'>{r[0]}</td><td style='padding:8px; border-bottom:1px solid #eee;'>{r[2]}</td></tr>" for r in new_rows])
        table_html = f"<table style='width:100%; border-collapse:collapse;'>{rows}</table>"

    html_content = f"""
    <div style="font-family: Arial, sans-serif; border: 1px solid #ddd; max-width: 600px;">
        <div style="background-color: #2b7a91; color: white; padding: 20px; text-align: center;">
            <h2 style="margin:0;">INCIDENT MANAGEMENT SYSTEM</h2>
            <p style="margin:0; font-size: 12px;">SITE MANAGEMENT CLUSTER</p>
        </div>
        <div style="padding: 20px;">
            <h3 style="color: #2b7a91;">{status_msg}</h3>
            {table_html}
            <p>Reported at: 06:00 AM Amman Time</p>
        </div>
        <div style="background-color: #333; color: white; padding: 10px; text-align: center; font-size: 10px;">
            © 2026 SM Cluster
        </div>
    </div>
    """
    
    message.attach(MIMEText(html_content, 'html'))
    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    service.users().messages().send(userId='me', body={'raw': raw_message}).execute()
