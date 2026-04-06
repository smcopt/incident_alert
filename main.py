import os
import json
import time
import base64
import requests
import io
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg') # Required for Google Cloud Functions (no GUI)
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, timezone
import google.auth
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from googleapiclient.discovery import build
from openpyxl.styles import Font

# --- CONFIGURATION ---
SPREADSHEET_ID = '15cGy5EhzuR330e6XmFaAXSaokoRsFxBUugzXybPqZkw'
SENDER_EMAIL = 'info@smcopt.org'
RECIPIENT_EMAIL = 'sujanpaudel@iom.int'
API_URL = 'https://app.zitemanager.org/api/v2/reports-file/?report_id=2137&key=7kq1bSino0AcI86hIFbmM6mmTU425121134211' 
SERVICE_ACCOUNT_EMAIL = 'incident-alert@incidentalert-490412.iam.gserviceaccount.com'
LOGO_URL = 'https://raw.githubusercontent.com/smcopt/incident_alert/main/CountryLogo_Palestine_V01.png'
AMMAN_TZ = timezone(timedelta(hours=3))

# --- HELPERS ---
def fig_to_base64(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=100, bbox_inches='tight')
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode('utf-8')

def run_workflow(request):
    try:
        # 1. AUTH & SERVICES
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/cloud-platform']
        creds, _ = google.auth.default(scopes=scopes)
        creds.refresh(Request())
        sheet_service = build('sheets', 'v4', credentials=creds)

        # Gmail Delegation
        jwt_payload = json.dumps({
            "iss": SERVICE_ACCOUNT_EMAIL, "sub": SENDER_EMAIL,
            "scope": "https://www.googleapis.com/auth/gmail.send",
            "aud": "https://oauth2.googleapis.com/token",
            "iat": int(time.time()), "exp": int(time.time()) + 3600
        })
        iam_url = f"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{SERVICE_ACCOUNT_EMAIL}:signJwt"
        iam_resp = requests.post(iam_url, headers={"Authorization": f"Bearer {creds.token}"}, json={"payload": jwt_payload}).json()
        
        gmail_creds = Credentials(requests.post("https://oauth2.googleapis.com/token", data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": iam_resp['signedJwt']
        }).json()['access_token'])
        gmail_service = build('gmail', 'v1', credentials=gmail_creds)

        # 2. FETCH & CLEAN DATA
        existing_res = sheet_service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range="ALERT!A:A").execute()
        existing_ids = set([row[0] for row in existing_res.get('values', []) if row])
        
        api_data = requests.get(API_URL).json()
        df = pd.DataFrame(api_data)

        # Logic for "Other" incidents
        c_inc = 'Event Information-What was the main incident? [Most Recent] '
        c_oth = 'Event Information-If other, please specify  [Most Recent]'
        df[c_inc] = df[c_inc].fillna('')
        if c_oth in df.columns:
            df[c_oth] = df[c_oth].fillna('')
            mask = (df[c_inc].str.lower().str.strip() == 'other') | (df[c_inc] == '')
            df.loc[mask, c_inc] = df.loc[mask, c_oth]

        # Identify New Records for Sheet
        df_new = df[~df['Case Id'].astype(str).isin(existing_ids)].copy()

        if not df_new.empty:
            values = [df.columns.tolist()] if not existing_ids else []
            values.extend(df_new.astype(str).values.tolist())
            sheet_service.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID, range="ALERT!A1",
                valueInputOption="RAW", body={"values": values}
            ).execute()

        # 3. GENERATE VISUAL SUMMARY (HTML)
        summary_html = ""
        if not df.empty:
            # Simple KPIs
            total_alerts = len(df)
            unique_sites = df['Site Name'].nunique()
            
            # Create Chart
            fig, ax = plt.subplots(figsize=(6, 3))
            df[c_inc].value_counts().head(5).plot(kind='barh', color='#1B657C', ax=ax)
            ax.set_title("Recent Incident Trends", fontweight='bold')
            plt.tight_layout()
            chart_img = fig_to_base64(fig)

            summary_html = f"""
            <div style="border: 1px solid #1B657C; padding: 15px; border-radius: 8px; background: #ffffff;">
                <h3 style="color: #1B657C; margin-top: 0;">Summary Dashboard</h3>
                <p>Total Alerts: <b>{total_alerts}</b> | Sites Impacted: <b>{unique_sites}</b></p>
                <img src="data:image/png;base64,{chart_img}" style="width: 100%; border-radius: 4px;">
            </div>
            """

        # 4. SEND EMAIL
        send_final_email(gmail_service, df_new, summary_html)

        return "Success", 200
    except Exception as e:
        print(f"Error Log: {e}")
        return f"Error: {e}", 500

def send_final_email(service, df_new, summary_html):
    today = datetime.now(AMMAN_TZ).strftime("%d-%m-%Y")
    message = MIMEMultipart()
    message['to'] = RECIPIENT_EMAIL
    message['from'] = SENDER_EMAIL
    message['subject'] = f"Incident Alert Summary ({today})"

    # Attachment
    if not df_new.empty:
        buf = io.BytesIO()
        df_new.to_excel(buf, index=False, engine='openpyxl')
        part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        part.set_payload(buf.getvalue())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="New_Incidents_{today}.xlsx"')
        message.attach(part)

    # Email Body
    status_text = f"Found {len(df_new)} new incidents today." if not df_new.empty else "No new incidents recorded."
    body_content = f"""
    <html>
        <body style="font-family: sans-serif; color: #333; background: #F5F3E8; padding: 20px;">
            <div style="max-width: 600px; margin: auto; background: white; padding: 20px; border-radius: 10px;">
                <img src="{LOGO_URL}" height="50" style="display:block; margin: auto;">
                <h2 style="text-align: center;">SMC Alert System</h2>
                <p>{status_text}</p>
                {summary_html}
                <p style="font-size: 11px; color: #888; text-align: center; margin-top: 20px;">
                    This is an automated report from Google Cloud.
                </p>
            </div>
        </body>
    </html>
    """
    message.attach(MIMEText(body_content, 'html'))
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    service.users().messages().send(userId='me', body={'raw': raw}).execute()
