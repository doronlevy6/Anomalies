import os
import json
import base64
import re
import time
from datetime import datetime
from bs4 import BeautifulSoup
import boto3
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import pandas as pd
import traceback

# Reuse configuration paths
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 'https://www.googleapis.com/auth/gmail.modify']
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config', 'credentials.json')
TOKEN_PATH = os.path.join(os.path.dirname(__file__), 'config', 'token.json')

# --- Shared/Duplicated Helpers for Isolation ---

def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return json.load(f)

def get_gmail_service():
    # Duplicate auth logic to ensure independence
    creds = None
    config = load_config()
    
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # We assume token exists from the main workflow for now, 
            # or we re-implement the flow if needed. 
            # For simplicity, we assume the token is shared/valid.
            raise Exception("Credential token expired or invalid. Please run the main anomaly flow to authenticate first.")
            
    return build('gmail', 'v1', credentials=creds)

def get_bedrock_client():
    config = load_config()
    aws_conf = config['aws']
    return boto3.client(
        service_name='bedrock-runtime',
        region_name=aws_conf.get('region_name', 'us-east-1'),
        aws_access_key_id=aws_conf['access_key_id'],
        aws_secret_access_key=aws_conf['secret_access_key']
    )

def clean_html(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    for script in soup(["script", "style"]):
        script.extract()
    text = soup.get_text()
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '\n'.join(chunk for chunk in chunks if chunk)
    return text[:8000]

def extract_email_body(payload):
    body_text = ""
    body_html = ""
    parts = [payload]
    if 'parts' in payload:
        parts = payload['parts']
    
    for part in parts:
        mime_type = part.get('mimeType')
        body_data = part.get('body', {}).get('data', '')
        if not body_data:
            if 'parts' in part:
                 for subpart in part['parts']:
                    st = subpart.get('mimeType')
                    bd = subpart.get('body', {}).get('data', '')
                    if bd:
                        decoded = base64.urlsafe_b64decode(bd).decode('utf-8', errors='replace')
                        if st == 'text/plain': body_text += decoded
                        if st == 'text/html': body_html += decoded
            continue
            
        decoded = base64.urlsafe_b64decode(body_data).decode('utf-8', errors='replace')
        if mime_type == 'text/plain':
            body_text += decoded
        elif mime_type == 'text/html':
            body_html += decoded
            
    return body_text, body_html

def parse_llm_response(text):
    clean_text = re.sub(r'^```json\s*|\s*```$', '', text.strip(), flags=re.MULTILINE)
    try:
        return json.loads(clean_text)
    except json.JSONDecodeError:
        try:
             match_obj = re.search(r'\{[\s\S]*\}', clean_text)
             if match_obj: return json.loads(match_obj.group(0))
        except: pass
        return {}

def extract_account_id(text=""):
    if not text: return ""
    match = re.search(r'\b\d{3}[- ]?\d{3}[- ]?\d{3}[- ]?\d{3}\b', text)
    if match: return match.group(0).replace('-', '').replace(' ', '')
    match_simple = re.search(r'\b\d{12}\b', text)
    return match_simple.group(0) if match_simple else ""

def extract_original_metadata(body_text, current_headers):
    """
    Attempts to extract original sender, date, and subject from a forwarded email body.
    Returns a dict with updates if found, otherwise empty dict.
    Copied from anomalies_logic.py for consistency.
    """
    updates = {}
    
    # Common forwarded message patterns
    if "Forwarded message" in body_text[:2000] or "From:" in body_text[:1000]: 
        # Extract To (Special User Request: Map 'To' address to 'From' display)
        to_match = re.search(r'To:\s*(.*?)\n', body_text)
        if to_match:
            raw_to = to_match.group(1).strip()
            if '<' in raw_to:
                updates['fromName'] = raw_to.split('<')[0].strip(' "') 
                updates['fromAddress'] = raw_to.split('<')[1].strip('>')
            else:
                 updates['fromName'] = raw_to
                 updates['fromAddress'] = raw_to
        
        # Fallback to From if To is missing
        if 'fromName' not in updates:
            from_match = re.search(r'From:\s*(.*?)\n', body_text)
            if from_match:
                raw_from = from_match.group(1).strip()
                if '<' in raw_from:
                    updates['fromName'] = raw_from.split('<')[0].strip(' "')
                    updates['fromAddress'] = raw_from.split('<')[1].strip('>')
                else:
                    updates['fromName'] = raw_from
                    updates['fromAddress'] = raw_from
        
        # Extract Date or Sent
        date_match = re.search(r'(?:Date|Sent):\s*(.*?)\n', body_text)
        if date_match:
            updates['date'] = date_match.group(1).strip()
            
        # Extract Subject
        sub_match = re.search(r'Subject:\s*(.*?)\n', body_text)
        if sub_match:
            updates['subject'] = sub_match.group(1).strip()
            
    return updates

def get_or_create_health_label(service):
    """
    Finds or creates the 'health' label with orange color.
    """
    try:
        results = service.users().labels().list(userId='me').execute()
        labels = results.get('labels', [])
        
        for label in labels:
            if label['name'].lower() == 'health':
                return label['id']
                
        # Not found, create it
        label_body = {
            "name": "health",
            "messageListVisibility": "show",
            "labelListVisibility": "labelShow",
            "type": "user",
            "color": {
                "textColor": "#ffffff",
                "backgroundColor": "#ff8c00"  # Orange/Dark Orange
            }
        }
        created = service.users().labels().create(userId='me', body=label_body).execute()
        return created['id']
    except Exception as e:
        # ctx.log() is not available here, so we print to stdout which Flask captures
        print(f"Error managing 'health' label: {e}")
        return None

# --- Account Map (Loaded independently) ---
ACCOUNT_MAP = {}
def load_account_map_independent():
    global ACCOUNT_MAP
    try:
        excel_path = os.path.join(os.path.dirname(__file__), 'templates', 'mailsToFlow1.xlsx')
        if not os.path.exists(excel_path): return
        df = pd.read_excel(excel_path, header=1)
        for _, row in df.iterrows():
            acc_id = str(row.get('Account', '')).strip().replace('.0', '')
            if not acc_id or acc_id.lower() == 'nan': continue
            if len(acc_id) < 12: acc_id = acc_id.zfill(12)
            ACCOUNT_MAP[acc_id] = {
                "accountName": str(row.get('Account Name', '')),
                "operationsEmail": str(row.get('Operations Email', '')),
                "pocName": str(row.get('POC name', ''))
            }
            ACCOUNT_MAP[acc_id]["customer"] = ACCOUNT_MAP[acc_id]["accountName"]
    except Exception as e:
        print(f"Error loading account map in alerts: {e}")

load_account_map_independent()

def generate_alert_card(ctx, data, index):
    # Simplified version of generate_html_card for alerts
    urgency = data.get('urgency', 'low').lower()
    if urgency not in ['low', 'medium', 'high']: urgency = 'low'
    urg_class = f"urg-{urgency}"
    urg_label = {'low': 'נמוכה', 'medium': 'בינונית', 'high': 'גבוהה'}.get(urgency, 'נמוכה')
    
    action_req = data.get('action_required', False)
    action_label = "נדרשת פעולה" if action_req else "אין פעולה נדרשת"
    
    uid = f"alert_{index}_{int(time.time())}"
    
    # Meta
    acc_id = data.get('extracted_account_id', '')
    acc_name = data.get('account_name', '')
    customer = ACCOUNT_MAP.get(acc_id, {}).get('customer', '')
    
    meta_parts = []
    meta_parts.append(f"מאת: {data.get('fromName', '')}")
    if data.get('date'): meta_parts.append(f"תאריך: <span dir='ltr'>{data.get('date')}</span>")
    if customer: meta_parts.append(f"לקוח: {customer}")
    meta_parts.append(f"חשבון: <strong>{acc_id} ({acc_name})</strong>")
    
    total_impact = data.get('total_impact_usd', 'Unknown')
    if total_impact and total_impact != 'Unknown':
         meta_parts.append(f"<div style='margin-top:8px; font-size:1.1em; color:#d32f2f; font-weight:800; border:1px solid #ef4444; display:inline-block; padding:2px 8px; border-radius:4px; background:#fef2f2;'>Impact: {total_impact}</div>")
    
    meta_html = "<br />".join(meta_parts)
    
    # Links
    from urllib.parse import quote
    custom_subject = f"Abra - Alert: {data.get('subject', '')} - {acc_name}"
    cc_emails = "eyal.stoler@abra-it.com,Shaked.Gofer@abra-it.com,Snir.Gridish@abra-it.com"
    ops_email = ACCOUNT_MAP.get(acc_id, {}).get('operationsEmail', '')
    
    gmail_team = f"mailto:?subject={quote(custom_subject)}&cc={quote(cc_emails)}&body={quote(data.get('team_message_he', ''))}"
    gmail_client_he = f"mailto:{ops_email}?subject={quote(custom_subject)}&cc={quote(cc_emails)}&body={quote(data.get('client_message_he', ''))}"
    gmail_client_en = f"mailto:{ops_email}?subject={quote(custom_subject)}&cc={quote(cc_emails)}&body={quote(data.get('client_message_en', ''))}"
    
    card_html = f"""
    <div class="card" data-urgency="{urgency}">
        <div class="urg-container">
            <span class="badge {urg_class}">דחיפות: {urg_label}</span>
        </div>
        
        <div class="row">
            <div>
                <div class="subject">{data.get('subject', '')}</div>
                <div class="meta">{meta_html}</div>
            </div>
        </div>
        
        <div class="summary-box he" style="background:#f0f9ff; border:1px solid #bae6fd; border-radius:8px; padding:12px; margin:10px 0;">
            <div style="font-weight:600; color:#0369a1; margin-bottom:5px;">📋 תקציר (Alert):</div>
            <div style="color:#1e3a5f; line-height:1.5;">{data.get('summary_he', 'לא נמצא תקציר')}</div>
        </div>
        
        <div class="he">
            <div class="action-status">{action_label}</div>
            
            <div class="button-group">
                <button class="btn" onclick="toggleSection('{uid}-an', this)">פרטים (HE)</button>
                <button class="btn" onclick="toggleSection('{uid}-tm', this)">הודעה לצוות</button>
                <button class="btn" onclick="toggleSection('{uid}-ch', this)">לקוח (HE)</button>
                <button class="btn" onclick="toggleSection('{uid}-ce', this)">Client (EN)</button>
                <a class="btn" href="/api/email/{data.get('id')}" target="_blank" style="background:#f3f4f6; color:#374151; border-color:#d1d5db;">👁️ המייל המקורי</a>
            </div>
        </div>
        
        <div id="{uid}-an" class="section he" style="display:none;">
            <div class="section-title">פרטים טכניים:</div>
            <pre>{data.get('anomalies_he', '')}</pre>
        </div>
        
        <div id="{uid}-tm" class="section he" style="display:none;">
            <div class="section-title">הודעה לצוות:</div>
            <pre>{data.get('team_message_he', '')}</pre>
            <div class="next-action"><strong>הפעולה הבאה:</strong> {data.get('next_action_he', '')}</div>
            <div style="margin-top:10px;">
                <a class="btn" href="{gmail_team}" style="background:#e0f2fe; color:#0369a1; border-color:#bae6fd;">📧 פתח טיוטה לצוות</a>
            </div>
        </div>
        
        <div id="{uid}-ch" class="section he" style="display:none;">
            <div class="section-title">לקוח (עברית):</div>
            <pre>{data.get('client_message_he', '')}</pre>
            <div style="margin-top:10px;">
                <a class="btn" href="{gmail_client_he}" style="background:#dcfce7; color:#15803d; border-color:#86efac;">📧 פתח טיוטה ללקוח (HE)</a>
            </div>
        </div>
        
        <div id="{uid}-ce" class="section ltr" style="display:none;">
            <div class="section-title">Client (EN):</div>
            <pre>{data.get('client_message_en', '')}</pre>
             <div class="next-action"><strong>Next action:</strong> {data.get('next_action_en', '')}</div>
            <div style="margin-top:10px;">
                <a class="btn" href="{gmail_client_en}" style="background:#ecfccb; color:#3f6212; border-color:#bef264;">📧 Open Client Draft (EN)</a>
            </div>
        </div>
    </div>
    """
    return card_html


def run_alerts_workflow(ctx, limit=5):
    ctx.log("--- Starting General Alerts Analysis (Independent Flow) ---")
    
    if ctx.should_stop(): return []
    
    try:
        service = get_gmail_service()
        bedrock = get_bedrock_client()
        health_label_id = get_or_create_health_label(service)
        if health_label_id:
            ctx.log(f"Using label 'health' ID: {health_label_id}")
        else:
            ctx.log("Warning: Could not resolve 'health' label. Emails will NOT be tagged.")
            
    except Exception as e:
        ctx.log(f"Error authenticating or setting up labels: {e}")
        return []

    # Query: Inbox, Unread, Primary Category, NOT 'Cost anomaly', NOT 'health'
    query = 'in:inbox category:primary -label:health -subject:"Cost anomaly"'
    
    ctx.log(f"Searching for alerts with query: '{query}'...")
    results = service.users().messages().list(userId='me', q=query, maxResults=limit).execute()
    messages = results.get('messages', [])
    ctx.log(f"Found {len(messages)} alert messages.")
    
    cards = []
    
    for i, msg in enumerate(messages):
        if ctx.should_stop(): break
        ctx.log(f"\n--- Processing Alert {i+1}/{len(messages)} ---")
        
        m = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
        payload = m['payload']
        headers = {h['name'].lower(): h['value'] for h in payload.get('headers', [])}
        
        body_text, body_html = extract_email_body(payload)
        if not body_text and body_html: body_text = clean_html(body_html)
        elif not body_text and 'snippet' in m: body_text = m['snippet']
        
        email_data = {
            'id': m['id'],
            'fromName': headers.get('from', '').split('<')[0].strip(' "'),
            'fromAddress': headers.get('from', ''),
            'subject': headers.get('subject', 'No Subject'),
            'bodyText': body_text[:8000],
            'date': headers.get('date', '')
        }
        
        # Apply Metadata Extraction (Forwarded emails fix)
        updates = extract_original_metadata(body_text, email_data)
        if updates:
            ctx.log(f"  > Detected Forwarded Email. Updated metadata from body: {updates.keys()}")
            email_data.update(updates)
        
        # Account ID Extraction
        active_account_id = extract_account_id(email_data.get('bodyText', '')) or extract_account_id(email_data.get('subject', ''))
        
        account_info = ACCOUNT_MAP.get(active_account_id, {})
        account_name = account_info.get('accountName', 'Unknown')
        poc_name = account_info.get('pocName', 'Customer')
        
        email_data['extracted_account_id'] = active_account_id
        email_data['account_name'] = account_name
        email_data['poc_name'] = poc_name
        
        ctx.log(f"  > Identified Account: {active_account_id} ({account_name})")
        
        # AI Processing
        prompt = f"""You are an AWS FinOps Alert Analyst.
        
        Input Email:
        SUBJECT: {email_data['subject']}
        BODY: {email_data['bodyText']}
        
        Context:
        Account: {account_name} ({active_account_id})
        POC: {poc_name}

        Task:
        1. Classify Alert Type: 'Free Tier', 'Budget Alert', 'RI Utilization', 'SP Utilization', or 'Other'.
        2. Extract Details: 
           - For Free Tier: Service, Usage, Limit.
           - For Budget: Budget Name, Threshold, Actual/Forecast Amount.
           - For Utilization: RI/SP Name, dropped to %.
        3. Generate Outputs (Hebrew/English) mapped to the following JSON structure.
        
        JSON requirements:
        - summary_he: 2-3 line Hebrew summary. START with the Alert Type. (e.g. "התראת תקציב: חריגה בבצקציב X...").
        - anomalies_he: A clean list/text of the technical details (Service, Usage, Limits).
        - team_message_he: Technical note for the FinOps team.
        - client_message_he: A polite email draft to the customer {poc_name} explaining the alert.
        - client_message_en: English version of the client draft.
        - total_impact_usd: The relevant cost or "Free Tier".
        - urgency: low/medium/high (Free Tier = low, Budget = medium/high).
        - action_required: true/false.
        - next_action_he / en: What should happen next?
        
        Return ONLY valid JSON.
        {{
          "summary_he": "string",
          "anomalies_he": "string",
          "team_message_he": "string",
          "client_message_he": "string",
          "client_message_en": "string",
          "urgency": "low|medium|high",
          "action_required": true,
          "next_action_he": "string",
          "next_action_en": "string",
          "console_link": "",
          "total_impact_usd": "string"
        }}
        """

        try:
            body = json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 4000,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.0
            })
            # Use Haiku as it is confirmed working in the main workflow
            response = bedrock.invoke_model(body=body, modelId='anthropic.claude-3-haiku-20240307-v1:0')
            response_body = json.loads(response.get('body').read())
            llm_text = response_body['content'][0]['text']
            params = parse_llm_response(llm_text)
            email_data.update(params)
        except Exception as e:
            ctx.log(f"  > Bedrock Error: {e}")
            import traceback
            traceback.print_exc()
            email_data.update({'summary_he': f'Error processing AI: {e}'})

        # Generate Card
        card_html = generate_alert_card(ctx, email_data, i)
        cards.append(card_html)
        
        # Label
        if health_label_id:
            try:
                 service.users().messages().modify(userId='me', id=msg['id'], body={'addLabelIds': [health_label_id]}).execute()
                 ctx.log("  > Label 'health' applied.")
            except Exception as e:
                ctx.log(f"  > Warning: Failed to label: {e}")

    return cards
