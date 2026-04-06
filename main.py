import os
import json
import time
import base64
import requests
import io
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg') # Required for headless environments
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

# Brand Palette
COLORS = {
    "blue_sapphire": "#1B657C", "baltic_sea": "#2C2C2C", "burnt_sienna": "#EC6B4D",
    "ecru_white": "#F5F3E8", "moonstone": "#6FC5BC", "steel_blue": "#4595AD"
}

# --- CHART HELPERS ---
def fig_to_base64(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format='png', dpi=120, bbox_inches='tight')
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode('utf-8')

def generate_visual_report(df_daily):
    """Generates the KPI-heavy HTML summary string."""
    if df_daily.empty:
        return None

    # Simplified KPI calculation
    total_alerts = len(df_daily)
    unique_sites = df_daily['Site Name'].nunique()
    
    # 1. Chart: Incident Types
    inc_counts = df_daily['Event Information-What was the main incident? [Most Recent] '].value_counts()
    fig, ax = plt.subplots(figsize=(6, 4))
    inc_counts.plot(kind='barh', color=COLORS["blue_sapphire"], ax=ax)
    ax.set_title("Alerts by Incident Type", color=COLORS["blue_sapphire"], fontweight='bold')
    plt.tight_layout()
    chart_base64 = fig_to_base64(fig)

    # 2. Build HTML Body (Partial snippet of your 'lovable' design)
    report_date = datetime.now(AMMAN_TZ).strftime("%d %b %Y")
    
    html_content = f"""
    <div style="font-family: sans-serif; background: #F5F3E8; padding: 20px; border-radius: 10px;">
        <div style="background: #1B657C; color: white; padding: 20px; border-radius: 8px 8px 0 0;">
            <h1 style="margin:0;">DAILY ANALYTICS SUMMARY</h1>
            <p style="margin:0; opacity:0.8;">{report_date} | Amman Timezone</p>
        </div>
        
        <div style="display: flex; gap: 10px; margin-top: 20px;">
            <div style="background: white; padding: 15px; border-radius: 8px; flex: 1; border-top: 4px solid #EC6B4D;">
                <small style="color: #6B7280;">TOTAL ALERTS</small>
                <h2 style="margin: 5px 0;">{total_alerts}</h2>
            </div>
            <div style="background: white; padding: 15px; border-radius: 8px; flex: 1; border-top: 4px solid #1B657C;">
                <small style="color: #6B7280;">SITES AFFECTED</small>
                <h2 style="margin: 5px 0;">{unique_sites}</h2>
            </div>
        </div>

        <div style="background: white; padding: 20px; margin-top: 20px; border-radius: 8px;">
            <h3 style="color: #1B657C;">Incident Distribution</h3>
            <img src="data:image/png;base64,{chart_base64}" style="width: 100%; max-width: 500px;">
        </div>
    </div>
    """
    return html_content

# --- MAIN WORKFLOW ---
def run_workflow(request):
    try:
        # 1. Authentication (Google Sheets & Gmail)
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/cloud-platform']
        creds, _ = google.auth.default(scopes=scopes)
        creds.refresh(Request())
        sheet_service = build('sheets', 'v4', credentials=creds)

        # Gmail Delegation (JWT Signing)
        jwt_payload = json.dumps({
            "iss": SERVICE_ACCOUNT_EMAIL, "sub": SENDER_EMAIL,
            "scope": "https://www.googleapis.com/auth/gmail.send",
            "aud": "https://oauth2.googleapis.com/token",
            "iat": int(time.time()), "exp": int(time.time()) + 3600
        })
        iam_url = f"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/{SERVICE_ACCOUNT_EMAIL}:signJwt"
        iam_response = requests.post(iam_url, headers={"Authorization": f"Bearer {creds.token}"}, json={"payload": jwt_payload}).json()
        
        gmail_creds = Credentials(requests.post("https://oauth2.googleapis.com/token", data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": iam_response['signedJwt']
        }).json()['access_token'])
        gmail_service = build('gmail', 'v1', credentials=gmail_creds)

        # 2. Fetch Data & Check Duplicates
        existing_res = sheet_service.spreadsheets().values().get(spreadsheetId=SPREADSHEET_ID, range="ALERT!A:A").execute()
        existing_ids = set([row[0] for row in existing_res.get('values', []) if row])
        
        response = requests.get(API_URL)
        api_data = response.json()
        
        df_api = pd.DataFrame(api_data)
        # Identify new records
        df_new = df_api[~df_api['Case Id'].astype(str).isin(existing_ids)] if not df_api.empty else pd.DataFrame()

        # 3. Update Sheets
        if not df_new.empty:
            values = [df_new.columns.tolist()] if not existing_ids else []
            values.extend(df_new.astype(str).values.tolist())
            sheet_service.spreadsheets().values().append(
                spreadsheetId=SPREADSHEET_ID, range="ALERT!A1",
                valueInputOption="RAW", body={"values": values}
            ).execute()

        # 4. Generate Visual Summary (HTML)
        # We use the whole dataset for the visualization script logic
        visual_summary_html = generate_visual_report(df_api)

        # 5. Send Email
        send_enhanced_email(gmail_service, df_new, visual_summary_html)

        return "Success", 200
    except Exception as e:
        print(f"Error: {e}")
        return f"Error: {e}", 500

def send_enhanced_email(service, df_new, summary_html):
    amman_now = datetime.now(AMMAN_TZ).strftime("%d-%m-%Y")
    message = MIMEMultipart()
    message['to'] = RECIPIENT_EMAIL
    message['from'] = SENDER_EMAIL
    message['subject'] = f"Daily Incident Summary - SM Cluster ({amman_now})"

    # Attachment: Full Data Excel
    if not df_new.empty:
        buf = io.BytesIO()
        df_new.to_excel(buf, index=False)
        part = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        part.set_payload(buf.getvalue())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="Incidents_{amman_now}.xlsx"')
        message.attach(part)

    # Email Body: Combine the Alert Cards with the Visual Analytics
    alert_status = f"Processed {len(df_new)} new alerts." if not df_new.empty else "No new alerts today."
    
    final_body = f"""
    <html>
        <body>
            <div style="text-align: center; padding: 10px;">
                <img src="{LOGO_URL}" height="60">
            </div>
            <h2 style="color: #1B657C;">Status: {alert_status}</h2>
            <hr>
            {summary_html if summary_html else "<p>No data available for visualization.</p>"}
            <br>
            <p style="font-size: 11px; color: grey;">Automated Report | Site Management Cluster</p>
        </body>
    </html>
    """
    message.attach(MIMEText(final_body, 'html'))
    
    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    service.users().messages().send(userId='me', body={'raw': raw}).execute()
