import os
import json
import time
import base64
import requests
import io
import pandas as pd
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

import matplotlib
matplotlib.use('Agg') # Crucial for background execution
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

# --- CONFIGURATION ---
SPREADSHEET_ID = '15cGy5EhzuR330e6XmFaAXSaokoRsFxBUugzXybPqZkw'
SENDER_EMAIL = 'info@smcopt.org'
RECIPIENT_EMAIL = 'coordination@smcopt.org'
API_URL = 'https://app.zitemanager.org/api/v2/reports-file/?report_id=2137&key=7kq1bSino0AcI86hIFbmM6mmTU425121134211' 
SERVICE_ACCOUNT_EMAIL = 'incident-alert@incidentalert-490412.iam.gserviceaccount.com'
LOGO_URL = 'https://raw.githubusercontent.com/smcopt/incident_alert/main/CountryLogo_Palestine_V01.png'

# --- CCCM BRAND PALETTE & ICONS ---
COLORS = {
    "blue_sapphire": "#1B657C",
    "baltic_sea": "#2C2C2C",
    "burnt_sienna": "#EC6B4D",
    "ecru_white": "#F5F3E8",
    "moss_green": "#BBDFBB",
    "moonstone": "#6FC5BC",
    "steel_blue": "#4595AD",
    "bg": "#F5F3E8",
    "card_bg": "#FFFFFF",
    "text": "#2C2C2C",
    "text_light": "#6B7280",
}
CHART_PALETTE = ["#1B657C", "#EC6B4D", "#4595AD", "#6FC5BC", "#BBDFBB", "#2C2C2C"]

ICON_ALERT = '''<svg xmlns="http://www.w3.org/2000/svg" width="36" height="36" viewBox="0 0 80 80" fill="none"><path d="M40 8L6 68h68L40 8z" fill="#EC6B4D" opacity="0.15" stroke="#EC6B4D" stroke-width="3" stroke-linejoin="round"/><line x1="40" y1="30" x2="40" y2="48" stroke="#EC6B4D" stroke-width="4" stroke-linecap="round"/><circle cx="40" cy="57" r="3" fill="#EC6B4D"/></svg>'''
ICON_SITE = '''<svg xmlns="http://www.w3.org/2000/svg" width="36" height="36" viewBox="0 0 80 80" fill="none"><path d="M40 6C28 6 18 16 18 28c0 16 22 44 22 44s22-28 22-44C62 16 52 6 40 6z" fill="#1B657C" opacity="0.85"/><circle cx="40" cy="28" r="10" fill="white" opacity="0.9"/></svg>'''
ICON_AFFECTED = '''<svg xmlns="http://www.w3.org/2000/svg" width="36" height="36" viewBox="0 0 80 80" fill="none"><circle cx="28" cy="22" r="10" fill="#1B657C"/><circle cx="52" cy="22" r="10" fill="#4595AD"/><path d="M8 62c0-11 9-20 20-20s20 9 20 20" fill="#1B657C" opacity="0.8"/><path d="M32 62c0-11 9-20 20-20s20 9 20 20" fill="#4595AD" opacity="0.8"/></svg>'''
ICON_DEATHS = '''<svg xmlns="http://www.w3.org/2000/svg" width="36" height="36" viewBox="0 0 80 80" fill="none"><path d="M40 8C25 8 14 20 14 34c0 18 26 38 26 38s26-20 26-38C66 20 55 8 40 8z" fill="#EC6B4D" opacity="0.9"/><path d="M40 24v16M34 32h12" stroke="white" stroke-width="3.5" stroke-linecap="round"/></svg>'''

def df_to_html_table(df):
    if df.empty:
        return "<p>No data available</p>"
    return df.to_html(index=False, classes='', border=0, justify='left')

def run_workflow(request):
    try:
        # Timezone configuration (Amman UTC+3)
        AMMAN_TZ = timezone(timedelta(hours=3))
        now = datetime.now(AMMAN_TZ)
        report_date = now.strftime("%d %B %Y")
        
        # 1. Base Keyless Authentication for Google APIs
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/gmail.send']
        credentials, project = google.auth.default(scopes=scopes)

        # 2. Fetch Data from API
        response = requests.get(API_URL)
        response.raise_for_status()
        csv_data = io.StringIO(response.text)
        df = pd.read_csv(csv_data)

        # 3. Process Data & Generate Lovable HTML
        if df.empty:
            status_msg = "No incidents reported in the 24h window."
            lovable_html_body = f"<div class='callout'><strong>ℹ Note:</strong> {status_msg}</div>"
        else:
            status_msg = "Incidents successfully processed."
            
            # KPI Calculations (Adjust column names to match your actual API data)
            total_alerts = len(df)
            sites_affected = df['site_name'].nunique() if 'site_name' in df.columns else 0
            individuals_affected = df['individuals'].sum() if 'individuals' in df.columns else 0
            deaths = df['deaths'].sum() if 'deaths' in df.columns else 0
            
            # Generate Chart: Incident Type Breakdown
            chart_base64 = ""
            if 'incident_type' in df.columns:
                type_counts = df['incident_type'].value_counts().head(5)
                fig, ax = plt.subplots(figsize=(6, 4))
                ax.barh(type_counts.index[::-1], type_counts.values[::-1], color=COLORS['blue_sapphire'])
                ax.spines['top'].set_visible(False)
                ax.spines['right'].set_visible(False)
                plt.tight_layout()
                
                buf = io.BytesIO()
                fig.savefig(buf, format='png', bbox_inches='tight', dpi=120)
                buf.seek(0)
                chart_base64 = base64.b64encode(buf.read()).decode('utf-8')
                plt.close(fig)

            # Build HTML Layout
            lovable_html_body = f"""
            <!DOCTYPE html>
            <html lang="en">
            <head>
            <meta charset="UTF-8">
            <style>
                body {{ font-family: 'Inter', -apple-system, sans-serif; background: #F5F3E8; color: #2C2C2C; line-height: 1.6; margin: 0; padding: 0; }}
                .container {{ max-width: 1100px; margin: 0 auto; padding: 30px; }}
                .report-header {{ background: #1B657C; color: white; margin-bottom: 20px; }}
                .header-top {{ display: flex; align-items: center; justify-content: space-between; padding: 20px; }}
                .header-title-bar {{ background: #EC6B4D; padding: 15px 20px; }}
                .header-title-bar h1 {{ margin: 0; font-size: 1.4em; }}
                .header-meta {{ background: #2C2C2C; padding: 10px 20px; font-size: 0.85em; color: #eee; }}
                table.kpi-table {{ width: 100%; border-collapse: separate; border-spacing: 10px; margin-bottom: 20px; }}
                .kpi-card {{ background: white; padding: 20px; border-top: 4px solid #1B657C; border-radius: 4px; text-align: left; }}
                .kpi-card.accent {{ border-top-color: #EC6B4D; }}
                .kpi-label {{ font-size: 0.7em; text-transform: uppercase; color: #6B7280; font-weight: bold; margin-top: 10px; }}
                .kpi-value {{ font-size: 1.8em; font-weight: bold; color: #2C2C2C; margin: 5px 0; }}
                .section {{ background: white; padding: 25px; border-radius: 6px; margin-bottom: 20px; }}
                .section-title {{ font-size: 1.1em; font-weight: bold; color: #1B657C; border-bottom: 2px solid #F5F3E8; padding-bottom: 10px; margin-bottom: 15px; }}
                .footer {{ text-align: center; padding: 20px; font-size: 0.8em; color: #6B7280; border-top: 2px solid #1B657C; }}
            </style>
            </head>
            <body>
                <div class="container">
                    <div class="report-header">
                        <div class="header-top">
                            <img src="{LOGO_URL}" alt="Logo" height="40">
                            <div style="text-align: right; font-size: 0.8em;">SITE MANAGEMENT CLUSTER<br>occupied Palestinian territory</div>
                        </div>
                        <div class="header-title-bar">
                            <h1>DAILY INCIDENT SUMMARY</h1>
                        </div>
                        <div class="header-meta">
                            Report Date: {report_date} | {total_alerts} alerts processed
                        </div>
                    </div>

                    <table class="kpi-table">
                        <tr>
                            <td class="kpi-card accent" width="25%">
                                {ICON_ALERT}
                                <div class="kpi-label">Total Alerts</div>
                                <div class="kpi-value">{total_alerts}</div>
                            </td>
                            <td class="kpi-card" width="25%">
                                {ICON_SITE}
                                <div class="kpi-label">Sites Affected</div>
                                <div class="kpi-value">{sites_affected}</div>
                            </td>
                            <td class="kpi-card" width="25%">
                                {ICON_AFFECTED}
                                <div class="kpi-label">Individuals Affected</div>
                                <div class="kpi-value">{individuals_affected:,}</div>
                            </td>
                            <td class="kpi-card accent" width="25%">
                                {ICON_DEATHS}
                                <div class="kpi-label">Deaths</div>
                                <div class="kpi-value">{deaths:,}</div>
                            </td>
                        </tr>
                    </table>

                    <div class="section">
                        <div class="section-title">Incident Type Breakdown</div>
                        {"<img src='data:image/png;base64," + chart_base64 + "' style='max-width: 100%;'>" if chart_base64 else "<p>No chart data</p>"}
                    </div>

                    <div class="footer">
                        <p style="color: #1B657C; font-weight: bold;">CCCM CLUSTER — SITE MANAGEMENT SUPPORT</p>
                        <p>Generated at {now.strftime("%I:%M %p")} Amman Time</p>
                    </div>
                </div>
            </body>
            </html>
            """

        # 4. Generate Excel Attachments (Internal vs External)
        # Create Internal file
        internal_file = '/tmp/Internal_Full_Data.xlsx'
        df.to_excel(internal_file, index=False)
        
        # Create External file (Example: drop sensitive columns)
        external_file = '/tmp/External_Truncated_Data.xlsx'
        cols_to_drop = ['reporter_name', 'reporter_phone'] # Adjust to your actual sensitive columns
        df_ext = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
        df_ext.to_excel(external_file, index=False)

        # 5. Email Sending Logic
        msg = MIMEMultipart()
        msg['Subject'] = f"Daily Incident Summary - {report_date}"
        msg['From'] = SENDER_EMAIL
        msg['To'] = RECIPIENT_EMAIL
        
        # Attach the Lovable HTML body
        msg.attach(MIMEText(lovable_html_body, 'html'))
        
        # Attach Files Function
        def attach_file(msg, filepath):
            with open(filepath, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename= {os.path.basename(filepath)}")
            msg.attach(part)

        attach_file(msg, internal_file)
        attach_file(msg, external_file)

        # Send via Gmail API
        gmail_service = build('gmail', 'v1', credentials=credentials)
        raw_msg = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        gmail_service.users().messages().send(userId='me', body={'raw': raw_msg}).execute()

        return "Success: Email Sent with Lovable Dashboard", 200

    except Exception as e:
        print(f"Error executing workflow: {e}")
        return f"Error: {e}", 500
