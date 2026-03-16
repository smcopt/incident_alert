import os
import base64
import requests
import google.auth
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from googleapiclient.discovery import build

# --- CONFIGURATION ---
# Replace these with your actual details
SPREADSHEET_ID = '15cGy5EhzuR330e6XmFaAXSaokoRsFxBUugzXybPqZkw'
SENDER_EMAIL = 'info@smcopt.org'
RECIPIENT_EMAIL = 'sujanpaudel@iom.int' # or whoever should receive the summary
API_URL = 'https://api.smcopt.org/v1/incidents' # Replace with your real API

def run_workflow(request):
    """Main function triggered by Cloud Scheduler."""
    try:
        # 1. Identity-based Authentication
        # No credentials.json needed! The Cloud Function "is" the Service Account.
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/gmail.send'
        ]
        creds, project = google.auth.default(scopes=scopes)
        
        # To send as info@smcopt.org, we "impersonate" that user
        # This requires Domain-Wide Delegation set in Google Admin
        delegated_creds = creds.with_subject(SENDER_EMAIL)
        
        sheet_service = build('sheets', 'v4', credentials=creds)
        gmail_service = build('gmail', 'v1', credentials=delegated_creds)

        # 2. Get Existing Data (Column A) to prevent duplicates
        result = sheet_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range="Sheet1!A:A").execute()
        existing_ids = set([row[0] for row in result.get('values', []) if row])

        # 3. Fetch API Data
        response = requests.get(API_URL)
        api_data = response.json() 

        new_records = []
        for item in api_data:
            # Assuming your API uses 'id' as a unique field
            if str(item.get('id')) not in existing_ids:
                # Format for the Sheet: ID, Date, Description
                new_rows = [item.get('id'), item.get('date'), item.get('description')]
                new_records.append(new_rows)

        # 4. Update Sheet & Send Email
        if new_records:
            sheet_service.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID,
                range="Sheet1!A1",
                valueInputOption="RAW", # Hard Paste
                body={"values": new_records}
            ).execute()
            
            send_beautified_email(gmail_service, new_records)
        else:
            send_beautified_email(gmail_service, None)

        return "Success", 200

    except Exception as e:
        print(f"Error: {e}")
        return f"Error: {e}", 500

def send_beautified_email(service, new_rows):
    """Constructs the HTML email based on your preferred style."""
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
            rows += f"<tr><td style='padding:10px; border-bottom:1px solid #eee;'>{r[0]}</td><td style='padding:10px; border-bottom:1px solid #eee;'>{r[2]}</td></tr>"
        
        table_html = f"""
        <table style="width: 100%; border-collapse: collapse; margin-top: 20px;">
            <tr style="background-color: #f8f8f8; text-align: left;">
                <th style="padding: 10px;">ID</th>
                <th style="padding: 10px;">Description</th>
            </tr>
            {rows}
        </table>"""

    # The "Cluster" Styled Template
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
