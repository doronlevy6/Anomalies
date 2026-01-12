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

# --- Configuration ---
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 'https://www.googleapis.com/auth/gmail.modify']
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config', 'credentials.json')
TOKEN_PATH = os.path.join(os.path.dirname(__file__), 'config', 'token.json')

def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return json.load(f)

# --- Gmail Service ---
def get_gmail_service():
    creds = None
    config = load_config()
    
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # We must use the EXACT redirect URI registered in Google Console
            # Based on user input, it seems to be the n8n one
            redirect_uri = "http://localhost:5678/rest/oauth2-credential/callback"
            
            flow = InstalledAppFlow.from_client_config(config['gmail'], SCOPES)
            flow.redirect_uri = redirect_uri
            
            auth_url, _ = flow.authorization_url(prompt='consent')
            
            print(f"Opening browser for auth: {auth_url}")
            
            # Start a temporary server to listen for the callback
            import webbrowser
            from http.server import BaseHTTPRequestHandler, HTTPServer
            from urllib.parse import urlparse, parse_qs
            import threading

            auth_code = None
            
            class AuthHandler(BaseHTTPRequestHandler):
                def do_GET(self):
                    nonlocal auth_code
                    if self.path.startswith("/rest/oauth2-credential/callback"):
                        query = urlparse(self.path).query
                        params = parse_qs(query)
                        if 'code' in params:
                            auth_code = params['code'][0]
                            self.send_response(200)
                            self.send_header('Content-type', 'text/html')
                            self.end_headers()
                            self.wfile.write(b"Authentication successful! You can close this window.")
                            
                            # Spin off a thread to kill server to avoid deadlock in request handler
                            threading.Thread(target=server.shutdown).start()
                        else:
                             self.send_response(400)
                             self.wfile.write(b"No code found.")
                    else:
                        self.send_response(404)
            
            # Attempt to bind to 5678. If n8n is running, this might fail.
            try:
                HTTPServer.allow_reuse_address = True
                server = HTTPServer(('localhost', 5678), AuthHandler)
            except OSError:
                raise Exception("Port 5678 is in use. Please close n8n or any other app using this port to allow authentication.")

            webbrowser.open(auth_url)
            print("Listening on localhost:5678 for authentication...")
            server.serve_forever()
            server.server_close()
            
            if not auth_code:
                raise Exception("Failed to obtain auth code")

            flow.fetch_token(code=auth_code)
            creds = flow.credentials
        
        # Save the credentials for the next run
        with open(TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())

    return build('gmail', 'v1', credentials=creds)

# --- AWS Bedrock Service ---
def get_bedrock_client():
    config = load_config()
    aws_conf = config['aws']
    
    return boto3.client(
        service_name='bedrock-runtime',
        region_name=aws_conf.get('region_name', 'us-east-1'),
        aws_access_key_id=aws_conf['access_key_id'],
        aws_secret_access_key=aws_conf['secret_access_key']
    )

# --- Helpers ---
def clean_html(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    for script in soup(["script", "style"]):
        script.extract()
    text = soup.get_text()
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '\n'.join(chunk for chunk in chunks if chunk)
    return text[:8000] # Truncate as per original workflow

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
            if 'parts' in part: # Nested parts
                # extremely simplified recursion for this specific use case
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

def extract_original_metadata(body_text, current_headers):
    """
    Attempts to extract original sender, date, and subject from a forwarded email body.
    Returns a dict with updates if found, otherwise empty dict.
    """
    updates = {}
    
    # Common forwarded message patterns
    # ---------- Forwarded message ---------
    # From: Name <email>
    # Date: ...
    # Subject: ...
    
    if "Forwarded message" in body_text[:2000] or "From:" in body_text[:1000]: # Check first 2KB, loosened check
        # Extract To (Special User Request: Map 'To' address to 'From' display)
        to_match = re.search(r'To:\s*(.*?)\n', body_text)
        if to_match:
            raw_to = to_match.group(1).strip()
            if '<' in raw_to:
                updates['fromName'] = raw_to.split('<')[0].strip(' "') # Use alias name
                updates['fromAddress'] = raw_to.split('<')[1].strip('>')
            else:
                 updates['fromName'] = raw_to
                 updates['fromAddress'] = raw_to
        
        # Fallback: if no To found, try original From?
        # User said: "And cloudsolutions... I want inside From"
        # If 'To' is missing, maybe keep original behavior or try From.
        # Let's keep existing From logic as fallback if To is missing?
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

def split_reseller_email(body_text):
    """
    Deterministic regex-based splitter for Reseller emails.
    Extracts per-account blocks with shared row context.
    """
    # Pattern: "Member Account: <ID> (<Name>)"
    member_pattern = r'Member Account:\s*(\d{12})\s*\(([^)]+)\)'
    matches = list(re.finditer(member_pattern, body_text))
    
    if not matches:
        return []
    
    results = []
    
    # Identify row boundaries by "Start Date:"
    row_pattern = r'(Start Date:[^\n]+\n(?:Last Detected Date:[^\n]+\n)?(?:Duration:[^\n]+\n)?(?:Max Daily Impact:[^\n]+\n)?(?:Total Impact:[^\n]+\n)?)'
    row_matches = list(re.finditer(row_pattern, body_text))
    
    for i, match in enumerate(matches):
        account_id = match.group(1)
        account_name = match.group(2).strip()
        member_pos = match.start()
        
        # Find the row context (Start Date, Duration, etc.)
        row_context = ""
        row_start = 0
        for rm in row_matches:
            if rm.start() < member_pos:
                row_context = rm.group(1).strip()
                row_start = rm.start()
            else:
                break
        
        # Find the account-specific block:
        # Look backward for "AWS Service:" before this Member Account
        lookback_text = body_text[max(0, member_pos-200):member_pos]
        aws_service_match = re.search(r'AWS Service:\s*[^\n]+', lookback_text)
        if aws_service_match:
            block_start = max(0, member_pos-200) + aws_service_match.start()
        else:
            block_start = member_pos
        
        # Look forward for "Impact Contribution:" after this Member Account
        lookahead_text = body_text[member_pos:member_pos+400]
        impact_match = re.search(r'Impact Contribution:\s*\$[\d.,]+', lookahead_text)
        if impact_match:
            block_end = member_pos + impact_match.end()
        else:
            block_end = member_pos + 200
        
        # Extract account-specific block
        account_block = body_text[block_start:block_end].strip()
        
        # Find row end (next row start or reasonable limit)
        row_end = len(body_text)
        for rm in row_matches:
            if rm.start() > member_pos:
                row_end = rm.start()
                break
        
        # Find Monitor Details for this row
        row_text = body_text[row_start:row_end]
        monitor_match = re.search(r'Name:\s*([^\n]+)\n\s*Type:\s*([^\n]+)\n\s*Monitoring:\s*([^\n]+)', row_text)
        monitor_type = 'Unknown'
        monitor_info = ""
        if monitor_match:
            monitor_type = monitor_match.group(3).strip()
            monitor_info = f"\n\n--- MONITOR INFO ---\nName: {monitor_match.group(1).strip()}\nType: {monitor_match.group(2).strip()}\nMonitoring: {monitor_match.group(3).strip()}"
        
        # Combine: Row Context + Account Block + Monitor Info
        final_text = f"--- ANOMALY CONTEXT ---\n{row_context}\n\n--- ACCOUNT DATA ---\n{account_block}{monitor_info}"
        
        results.append({
            'account_id': account_id,
            'account_name': account_name,
            'text_block': final_text,
            'monitor_type': monitor_type
        })
    
    return results

def split_email_by_anomalies(body_text, account_id, account_name):
    """
    Splits ANY email (regular or reseller) by individual anomalies.
    Each anomaly starts with "Start Date:" and should be processed independently.
    This helps detect duplicates within the same email.
    """
    # Find all "Start Date" occurrences - each is a separate anomaly
    anomaly_pattern = r'Start Date:\s*\d{4}-\d{2}-\d{2}'
    anomaly_starts = [m.start() for m in re.finditer(anomaly_pattern, body_text)]
    
    if not anomaly_starts:
        # No anomalies found, return whole text
        return [{
            'account_id': account_id,
            'account_name': account_name,
            'text_block': body_text,
            'monitor_type': 'Unknown'
        }]
    
    results = []
    for i, start_pos in enumerate(anomaly_starts):
        # Find end position (next anomaly or end of text)
        if i + 1 < len(anomaly_starts):
            end_pos = anomaly_starts[i + 1]
        else:
            end_pos = len(body_text)
        
        # Extract this anomaly's text
        anomaly_text = body_text[start_pos:end_pos].strip()
        
        # Try to extract monitor type
        monitor_match = re.search(r'Monitoring:\s*([^\n]+)', anomaly_text)
        monitor_type = monitor_match.group(1).strip() if monitor_match else 'Unknown'
        
        results.append({
            'account_id': account_id,
            'account_name': account_name,
            'text_block': anomaly_text,
            'monitor_type': monitor_type
        })
    
    return results

def deduplicate_usage_types(split_results, body_text):
    """
    Removes duplicate usage types from split results.
    If same account_id + region + usage_type appears multiple times,
    keep only the one with the larger Impact Contribution.
    """
    # Extract usage type and region for each result
    enriched_results = []
    for item in split_results:
        text = item['text_block']
        account_id = item['account_id']
        
        # Extract Region
        region_match = re.search(r'Region:\s*([^\n]+)', text)
        region = region_match.group(1).strip() if region_match else ''
        
        # Extract Usage Type
        usage_match = re.search(r'Usage Type:\s*([^\n]+)', text)
        usage_type = usage_match.group(1).strip() if usage_match else ''
        
        # Extract Impact Contribution value
        impact_match = re.search(r'Impact Contribution:\s*\$([0-9.,]+)', text)
        impact = float(impact_match.group(1).replace(',', '')) if impact_match else 0.0
        
        enriched_results.append({
            **item,
            'region': region,
            'usage_type': usage_type,
            'impact_value': impact
        })
    
    # Group by account_id + region + usage_type
    groups = {}
    for item in enriched_results:
        key = f"{item['account_id']}|{item['region']}|{item['usage_type']}"
        if key not in groups:
            groups[key] = []
        groups[key].append(item)
    
    # Keep only the max impact from each group
    final_results = []
    for key, group in groups.items():
        if len(group) == 1:
            final_results.append(group[0])
        else:
            # Multiple items - keep the one with max impact
            max_item = max(group, key=lambda x: x['impact_value'])
            final_results.append(max_item)
    
    # Remove enrichment fields
    for item in final_results:
        item.pop('region', None)
        item.pop('usage_type', None)
        item.pop('impact_value', None)
    
    return final_results

def invoke_llm(bedrock, email_data):
    """
    Step 2: Standard Processor.
    Takes a single-account context (real or virtual) and generates the final formatted JSON.
    """
    model_id = "anthropic.claude-3-haiku-20240307-v1:0"
    
    # Context should already be enriched by the caller
    acc_name = email_data.get('account_name', 'Unknown')
    acc_id = email_data.get('extracted_account_id', 'Unknown')
    poc_name = email_data.get('poc_name', 'Customer')
    
    # Consistent Templates
    templates_block = f"""
    **CRITICAL GRAMMAR RULES - MUST FOLLOW**:
    
    1. DATES:
       - IF Start Date == End Date (Same Day): Write "בתאריך YYYY-MM-DD" (singular)
       - IF Start Date != End Date (Range): Write "בין התאריכים YYYY-MM-DD - YYYY-MM-DD" (range)
       - NEVER write "בין התאריכים" when dates are identical!
       - Compare dates FIRST before writing!
    
    2. SERVICES - COUNT THEM FIRST:
       - Count how many DISTINCT AWS services you found in the anomaly text
       - IF count == 1: Write "בשירות: <Service Name>" (singular)
         Example: "בשירות: Amazon Simple Storage Service"
         Example: "בשירות: Amazon Elastic Container Service"
       - IF count > 1: Write "במספר שירותים: <Service 1, Service 2>" (plural)
         Example: "במספר שירותים: Amazon S3, Amazon EC2"
       - DO NOT use plural "במספר שירותים" when you only detected ONE service!
    
    --- TEMPLATE: team_message_he (Internal) ---
    היי,
    זוהו מספר חריגות ב-Member Account {acc_name} ({acc_id})
    <Dates>, <Services Text>.

    Region(s): <Regions>.
    Usage Type: <Usage Type>.
    היקף משוער: $<Total Amount>.
    רק מוודא שניתן להודיע ללקוח וזה לא משהו בצד שלנו.

    --- TEMPLATE: client_message_he (Customer Hebrew) ---
    שלום {poc_name},

    חשבון: {acc_name} ({acc_id})

    <Description of the usage increase, mentioning specific services or components>

    <Dates>, זוהתה חריגה בהיקף משוער של <$Amount>.

    [INSTRUCTION FOR <Dates>]:
    * Example 1 (Same Day): Start=2025-05-10, End=2025-05-10 => Write: "בתאריך 2025-05-10"
    * Example 2 (Range): Start=2025-05-10, End=2025-05-12 => Write: "בין התאריכים 2025-05-10 - 2025-05-12"
    * CRITICAL: DO NOT write "בין התאריכים" if Start equals End. Always check dates first.

    השימוש התרכז ב:
    - אזור: <Region>
    - שירות: <Service>
    - Usage Type: <Usage Type>

    היינו רוצים לדעת האם אתם מודעים לעלייה בשימושים הללו,
    או שמדובר במשהו שדורש בדיקה מעמיקה יותר מצדנו.

    אנחנו כאן לשירותכם ולכל שאלה.

    Best regards,
    Abra-IT FinOps Team

    --- TEMPLATE: client_message_en (Customer English) ---
    Hello {poc_name},

    Account: {acc_name} ({acc_id})

    <Description of the usage increase, mentioning specific services or components>

    <Dates>, an anomaly was detected with an estimated scope of <$Amount>.

    [INSTRUCTION FOR <Dates>]:
    * Example 1 (Same Day): Start=2024-01-01, End=2024-01-01 => Write: "On 2024-01-01"
    * Example 2 (Range): Start=2024-01-01, End=2024-01-02 => Write: "Between 2024-01-01 - 2024-01-02"
    * CRITICAL: DO NOT write "Between" if Start equals End. Always check dates first.

    The usage was concentrated in:
    - Region: <Region>
    - Service: <Service>
    - Usage Type: <Usage Type>

    We would like to check whether you are already aware of this increase in usage,
    or if this is something that requires a deeper investigation on our side.

    We are here for any questions or clarifications.

    Best regards,
    Abra-IT FinOps Team
    """

    prompt = f"""You are an AWS Cost Anomaly Detection email processor for a reseller FinOps workflow.
    
    Input:
    FROM_NAME: {email_data.get('fromName', '')}
    FROM_ADDRESS: {email_data.get('fromAddress', '')}
    SUBJECT: {email_data.get('subject', '')}
    BODY_TEXT: {email_data.get('bodyText', '')}

    KNOWN CONTEXT:
    Member Account Name: {acc_name}
    Member Account ID: {acc_id}
    POC Name: {poc_name}

    Requirements:
    1) Extract anomalies ONLY from the email content.
    2) Output a Hebrew grouped anomaly list string.
    3) Build messages using these EXACT templates:
    
    {templates_block}

    ------------------------------------------------

    4) console_link: URL if present.
    5) urgency: low/medium/high
    6) action_required: true/false
    7) next_action_he / en: imperative step.
    8) total_impact_usd: "$Amount" (Extract the most relevant cost figure from the text).
    9) summary_he: A concise Hebrew summary with the following structured format:
       - חשבון: {acc_name} ({acc_id})
       - תקופה: <Dates in format matching the date rules above>
       - סכום: <$Amount>
       - שירות: <Service name(s)>
       - סוג שימוש: <Usage Type>
       
       Write this as a clean, readable summary that includes all these fields.

    Return ONLY valid JSON with EXACTLY these keys:
    {{
      "fromName":"string",
      "fromAddress":"string",
      "subject":"string",
      "summary_he":"string",
      "anomalies_he":"string",
      "active_member_account_id":"string",
      "team_message_he":"string",
      "client_message_he":"string",
      "client_message_en":"string",
      "urgency":"low|medium|high",
      "action_required":true,
      "next_action_he":"string",
      "next_action_en":"string",
      "console_link":"string",
      "total_impact_usd": "string"
    }}
    """
    
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4000,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.0
    })

    try:
        response = bedrock.invoke_model(body=body, modelId=model_id)
        response_body = json.loads(response.get('body').read())
        return response_body['content'][0]['text']
    except Exception as e:
        print(f"Error invoking Bedrock: {e}")
        return "{}"

def parse_llm_response(text):
    clean_text = re.sub(r'^```json\s*|\s*```$', '', text.strip(), flags=re.MULTILINE)
    try:
        return json.loads(clean_text)
    except json.JSONDecodeError:
        # Fallback regex extraction if JSON is malformed
        try:
             # Try to find a list first
             match_list = re.search(r'\[[\s\S]*\]', clean_text)
             if match_list:
                 return json.loads(match_list.group(0))
             
             # Try to find an object
             match_obj = re.search(r'\{[\s\S]*\}', clean_text)
             if match_obj:
                 return json.loads(match_obj.group(0))
        except:
            pass
        return {}

# --- Account Mapping ---
import pandas as pd

ACCOUNT_MAP = {}

def load_account_map():
    global ACCOUNT_MAP
    try:
        excel_path = os.path.join(os.path.dirname(__file__), 'templates', 'mailsToFlow1.xlsx')
        if not os.path.exists(excel_path):
            print(f"Warning: Account map file not found at {excel_path}")
            return

        # It seems the header is on the second row (index 1) based on analysis
        df = pd.read_excel(excel_path, header=1)
        
        # Normalize columns just in case
        # Expected: Account, Account Name, Operations Email, POC name
        
        for _, row in df.iterrows():
            # Handle potential NaNs
            acc_id = str(row.get('Account', '')).strip().replace('.0', '') # Remove decimal if read as float
            if not acc_id or acc_id.lower() == 'nan': continue
            
            # Ensure 12 digits
            if len(acc_id) < 12:
                 acc_id = acc_id.zfill(12)
            
            ACCOUNT_MAP[acc_id] = {
                "accountName": str(row.get('Account Name', '')),
                "operationsEmail": str(row.get('Operations Email', '')),
                "pocName": str(row.get('POC name', ''))
            }
            # Set customer to AccountName for now if not explicit
            ACCOUNT_MAP[acc_id]["customer"] = ACCOUNT_MAP[acc_id]["accountName"]
            
        print(f"Loaded {len(ACCOUNT_MAP)} accounts into Account Map.")
        return len(ACCOUNT_MAP)
        
    except Exception as e:
        print(f"Error loading account map: {e}")
        return 0

def get_account_map():
    return ACCOUNT_MAP

# Load on module start
load_account_map()
if ACCOUNT_MAP:
    print(f"DEBUG: First 3 Account IDs in Map: {list(ACCOUNT_MAP.keys())[:3]}")

def get_contacts_csv():
    import io
    import csv
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Header
    writer.writerow(['Account ID', 'Account Name', 'Customer', 'Operations Email', 'POC Name'])
    
    for acc_id, info in ACCOUNT_MAP.items():
        writer.writerow([
            acc_id,
            info.get('accountName', ''),
            info.get('customer', ''),
            info.get('operationsEmail', ''),
            info.get('pocName', '')
        ])
        
    return output.getvalue()


def extract_account_id(text=""):
    if not text: return ""
    # Allow for dashes or spaces in the 12-digit number (e.g. 123-456-789-012)
    match = re.search(r'\b\d{3}[- ]?\d{3}[- ]?\d{3}[- ]?\d{3}\b', text)
    if match:
        return match.group(0).replace('-', '').replace(' ', '')
    # Fallback to simple 12 digits
    match_simple = re.search(r'\b\d{12}\b', text)
    return match_simple.group(0) if match_simple else ""

def generate_html_card(ctx, data, index):
    urgency = data.get('urgency', 'low').lower()
    if urgency not in ['low', 'medium', 'high']: urgency = 'low'
    
    urg_class = f"urg-{urgency}"
    urg_label_map = {'low': 'נמוכה', 'medium': 'בינונית', 'high': 'גבוהה'}
    urg_label = urg_label_map.get(urgency, 'נמוכה')
    
    action_req = data.get('action_required', False)
    action_label = "נדרשת פעולה" if action_req else "אין פעולה נדרשת"
    
    uid = f"{index}_{int(time.time())}"
    
    # Enrichment (Already done in run_workflow, but fetching here for display)
    active_account_id = data.get('active_member_account_id', '')
    account_info = ACCOUNT_MAP.get(active_account_id, {})
    
    if not active_account_id:
         # Double check if not passed (should not happen with new flow)
         active_account_id = data.get('extracted_account_id', '')
         account_info = ACCOUNT_MAP.get(active_account_id, {})

    customer = account_info.get('customer', '')
    account_name = account_info.get('accountName', '')
    poc_name = account_info.get('pocName', '')
    ops_email = account_info.get('operationsEmail', '')
    
    # Extract dates from bodyText for export
    body_text = data.get('bodyText', '')
    start_date = ''
    end_date = ''
    
    # Try to extract Start Date
    start_match = re.search(r'Start Date:\s*(\d{4}-\d{2}-\d{2})', body_text)
    if start_match:
        start_date = start_match.group(1)
    
    # Try to extract Last Detected Date or End Date
    end_match = re.search(r'(?:Last Detected Date|End Date):\s*(\d{4}-\d{2}-\d{2})', body_text)
    if end_match:
        end_date = end_match.group(1)
    elif start_date:
        end_date = start_date  # Fallback to start if no end found
    
    # Extract service for export (First occurrence only - each anomaly has one service)
    service_match = re.search(r'AWS Service:\s*([^\n]+)', body_text)
    services = service_match.group(1).strip() if service_match else ''
    
    # Extract Region (First occurrence)
    region_match = re.search(r'Region:\s*([^\n]+)', body_text)
    region = region_match.group(1).strip() if region_match else ''
    
    # Extract Usage Type (First occurrence)
    usage_match = re.search(r'Usage Type:\s*([^\n]+)', body_text)
    usage_type = usage_match.group(1).strip() if usage_match else ''
    
    # Meta lines
    meta_parts = []
    # Shorten fromName - extract only part before @ if it's an email
    from_name = data.get('fromName', '')
    from_address = data.get('fromAddress', '')
    if '@' in from_address:
        display_name = from_address.split('@')[0]  # Just the username part
    else:
        display_name = from_name
    
    meta_parts.append(f"מאת: {display_name}")
    if data.get('date'):
        meta_parts.append(f"תאריך: <span dir='ltr'>{data.get('date')}</span>")
    
    if customer: meta_parts.append(f"לקוח: {customer}")
    
    # Enhanced Account Info in Meta
    acc_display = f"{active_account_id}"
    if account_name:
        acc_display += f" ({account_name})"
    if active_account_id:
        meta_parts.append(f"חשבון: <strong>{acc_display}</strong>")
        
    # Total Impact
    total_impact = data.get('total_impact_usd', 'Unknown')
    if total_impact and total_impact != 'Unknown':
         meta_parts.append(f"<div style='margin-top:8px; font-size:1.1em; color:#d32f2f; font-weight:800; border:1px solid #ef4444; display:inline-block; padding:2px 8px; border-radius:4px; background:#fef2f2;'>Total Impact: {total_impact}</div>")
    
    meta_html = "<br />".join(meta_parts)
    
    # Compose Links
    from urllib.parse import quote
    
    # Create custom subject: "Abra - anomaly alert <Account Name> (<Account ID>)"
    custom_subject = f"Abra - anomaly alert {account_name} ({active_account_id})"
    
    # CC recipients
    cc_emails = "eyal.stoler@abra-it.com,Shaked.Gofer@abra-it.com,Snir.Gridish@abra-it.com"
    
    # Email Bodies are now fully generated by LLM based on strict templates
    team_body = f"{data.get('team_message_he', '')}\\n\\nהפעולה הבאה (HE):\\n{data.get('next_action_he', '')}"
    client_he_body = f"{data.get('client_message_he', '')}"
    client_en_body = f"{data.get('client_message_en', '')}"
    
    gmail_team = f"mailto:?subject={quote(custom_subject)}&cc={quote(cc_emails)}&body={quote(team_body)}"
    gmail_client_he = f"mailto:{ops_email}?subject={quote(custom_subject)}&cc={quote(cc_emails)}&body={quote(client_he_body)}"
    gmail_client_en = f"mailto:{ops_email}?subject={quote(custom_subject)}&cc={quote(cc_emails)}&body={quote(client_en_body)}"
    
    card_html = f"""
    <div class="card" data-urgency="{urgency}">
        <div class="urg-container">
            <span class="badge {urg_class}">דחיפות: {urg_label}</span>
        </div>
        
        <div class="row">
            <div>
                <div class="subject">{custom_subject}</div>
                <div class="meta">{meta_html}</div>
            </div>
        </div>
        
        <div class="summary-box he" style="background:#f0f9ff; border:1px solid #bae6fd; border-radius:8px; padding:12px; margin:10px 0;">
            <div style="font-weight:600; color:#0369a1; margin-bottom:5px;">📋 תקציר:</div>
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
                {f'<a class="btn" href="{data.get("console_link")}" target="_blank">פתח בקונסולה</a>' if data.get('console_link') else ''}
                <button class="btn" onclick="exportAnomaly('{customer}', '{account_name}', '{active_account_id}', '{start_date}', '{end_date}', '{total_impact}', '{services.replace("'", "\\'")}', '{region}', '{usage_type.replace("'", "\\'")}')" style="background:#fef3c7; color:#92400e; border-color:#fde68a;">📊 ייצא לקובץ מעקב</button>
            </div>
        </div>
        
        <div id="{uid}-an" class="section he" style="display:none;">
            <div class="section-title">פרטים מלאים:</div>
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
                <a class="btn" href="{gmail_client_en}" style="background:#dcfce7; color:#15803d; border-color:#86efac;">📧 Open Client Draft (EN)</a>
            </div>
        </div>
    </div>
    """
    return card_html

import queue

class WorkflowContext:
    def __init__(self):
        self._stop_flag = False
        self.msg_queue = queue.Queue()
    
    def log(self, message):
        self.msg_queue.put({"type": "log", "message": message})
        print(message) # Keep printing to terminal for debugging
        
    def request_stop(self):
        self._stop_flag = True
        self.log("!!! Stop requested by user !!!")
        
    
    def should_stop(self):
        return self._stop_flag

def extract_console_link(html_content):
    """
    Extracts the AWS Console link from the HTML body.
    Look for specific anomaly detection URLs.
    """
    # Pattern for new anomaly link
    # https://console.aws.amazon.com/cost-management/home?region=...#/anomaly-detection/monitors/...
    link_pattern = r'https://console\.aws\.amazon\.com/cost-management/home\?[^"\s<]*'
    match = re.search(link_pattern, html_content)
    if match:
        return match.group(0).replace('&amp;', '&')
    
    return ""

# --- Main Logic ---
def run_anomalies_workflow(ctx: WorkflowContext):
    ctx.log("--- Starting Analysis ---")
    
    if ctx.should_stop(): return []
    
    ctx.log("Step 1: Authenticating with Gmail...")
    try:
        service = get_gmail_service()
    except Exception as e:
        ctx.log(f"Error authenticating Gmail: {e}")
        raise e
        
    if ctx.should_stop(): return []
    
    ctx.log("Step 2: Authenticating with AWS Bedrock...")
    try:
        bedrock = get_bedrock_client()
    except Exception as e:
        ctx.log(f"Error authenticating AWS: {e}")
        raise e
    
    # Node: Gmail Trigger
    ctx.log("--- Node: Gmail Trigger (Search) ---")
    query = 'in:inbox -label:fetched subject:"Cost anomaly"'
    ctx.log(f"Searching for emails with query: '{query}'...")
    results = service.users().messages().list(userId='me', q=query, maxResults=15).execute()
    messages = results.get('messages', [])
    ctx.log(f"Found {len(messages)} messages.")
    
    cards = []
    
    for i, msg in enumerate(messages):
        if ctx.should_stop():
            ctx.log("Execution stopped before processing all messages.")
            break
            
        ctx.log(f"\n--- Processing Message {i+1}/{len(messages)} ---")
        
        # Node: Fetch Content
        ctx.log("--- Node: Fetch Email Content ---")
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
        
        # Node: Metadata Extraction
        ctx.log("--- Node: Metadata Extraction ---")
        orig_meta = extract_original_metadata(body_text, headers)
        if orig_meta:
            ctx.log(f"  > Detected forwarded email. Overwriting metadata: {orig_meta}")
            email_data.update(orig_meta)
        
        if ctx.should_stop(): break
        
        # Node: Account Identification (Routing)
        ctx.log("--- Node: Account Identification & Routing ---")
        active_account_id = extract_account_id(email_data.get('subject', '')) or extract_account_id(email_data.get('bodyText', ''))
        
        # Look up in Account Map
        account_info = ACCOUNT_MAP.get(active_account_id, {})
        account_name = account_info.get('accountName', 'Unknown')
        poc_name = account_info.get('pocName', 'Customer')
        
        ctx.log(f"  > Identified Account: {active_account_id} ({account_name})")
        
        email_data['extracted_account_id'] = active_account_id
        email_data['account_name'] = account_name
        email_data['poc_name'] = poc_name
        
        # Determine Route
        is_reseller = (active_account_id == '262674733103')
        route_name = "RESELLER (Multi-Account)" if is_reseller else "STANDARD (Single Account)"
        ctx.log(f"  > Selected Route: {route_name}")
        
        # Build processing queue
        processing_queue = []
        
        if is_reseller:
            # Use deterministic regex splitter for Reseller (splits by Member Account)
            ctx.log("--- Node: Splitting Reseller Email ---")
            split_results = split_reseller_email(email_data.get('bodyText', ''))
            ctx.log(f"  > Found {len(split_results)} Member Accounts in email")
            
            # Deduplicate same usage types
            ctx.log("--- Node: Deduplicating Usage Types ---")
            split_results = deduplicate_usage_types(split_results, email_data.get('bodyText', ''))
            ctx.log(f"  > After deduplication: {len(split_results)} unique anomalies")
            
            for split_item in split_results:
                processing_queue.append({
                    'account_id': split_item['account_id'],
                    'account_name': split_item['account_name'],
                    'text_block': split_item['text_block']
                })
        else:
            # Standard: split by anomalies (Start Date markers) and deduplicate
            ctx.log("--- Node: Splitting Standard Email by Anomalies ---")
            split_results = split_email_by_anomalies(email_data.get('bodyText', ''), active_account_id, account_name)
            ctx.log(f"  > Found {len(split_results)} anomalies in email")
            
            # Deduplicate same usage types (in case of multiple monitors)
            if len(split_results) > 1:
                ctx.log("--- Node: Deduplicating Usage Types ---")
                split_results = deduplicate_usage_types(split_results, email_data.get('bodyText', ''))
                ctx.log(f"  > After deduplication: {len(split_results)} unique anomalies")
            
            for split_item in split_results:
                processing_queue.append({
                    'account_id': split_item['account_id'],
                    'account_name': split_item['account_name'],
                    'text_block': split_item['text_block']
                })
        
        if not processing_queue:
            ctx.log("  > Warning: No accounts found to process.")
            continue
        
        # Node: AI Processing (per item)
        ctx.log(f"--- Node: AI Processing ({len(processing_queue)} items) ---")
        
        for idx, queue_item in enumerate(processing_queue):
            if ctx.should_stop(): break
            
            target_id = queue_item['account_id']
            target_name = queue_item['account_name']
            target_text = queue_item['text_block']
            
            # Enrich from Account Map
            acc_info = ACCOUNT_MAP.get(target_id, {})
            enriched_name = acc_info.get('accountName')
            
            # Fallback: If not in map, try to use the name captured by regex
            if not enriched_name or enriched_name == 'Unknown':
                enriched_name = target_name # This comes from the regex capture group (Name)
            
            if not enriched_name: enriched_name = 'Unknown'
            
            enriched_poc = acc_info.get('pocName') or 'Customer'
            
            ctx.log(f"  > Processing {idx+1}/{len(processing_queue)}: {target_id} ({enriched_name})")
            
            # Build virtual email data for Standard LLM
            virtual_email = email_data.copy()
            virtual_email['extracted_account_id'] = target_id
            virtual_email['account_name'] = enriched_name
            virtual_email['poc_name'] = enriched_poc
            virtual_email['bodyText'] = target_text  # Override with focused block
            
            # Call Standard LLM
            llm_raw = invoke_llm(bedrock, virtual_email)
            llm_parsed = parse_llm_response(llm_raw)
            
            # Normalize to list
            if isinstance(llm_parsed, dict):
                items = [llm_parsed] if llm_parsed else []
            elif isinstance(llm_parsed, list):
                items = llm_parsed
            else:
                items = []
                
            if not items:
                ctx.log("  > Warning: No valid output from LLM.")
            
            # Extract Link from HTML (Hard fallback)
            extracted_link = extract_console_link(email_data.get('bodyHtml', '')) # We need to ensure bodyHtml is passed
                
            # Node: Card Generation
            ctx.log(f"--- Node: Card Generation ({len(items)} items) ---")
            for idx, item in enumerate(items):
                current_acc_id = item.get('active_member_account_id', active_account_id)
                combined_data = {**email_data, **item}
                combined_data['active_member_account_id'] = current_acc_id
                
                # If LLM didn't find link, or we want to ensure it works
                if not combined_data.get('console_link') and extracted_link:
                    combined_data['console_link'] = extracted_link
                
                # Re-enrich if different (Reseller flow)
                if current_acc_id != target_id:
                    acc_info_card = ACCOUNT_MAP.get(current_acc_id, {})
                    combined_data['account_name'] = acc_info_card.get('accountName', 'Unknown')
                    combined_data['poc_name'] = acc_info_card.get('pocName', 'Customer')
                
                ctx.log(f"    > Card generated for {current_acc_id}")
                cards.append(generate_html_card(ctx, combined_data, f"{i}_{idx}"))
        
        # Node: Apply Label
        ctx.log("--- Node: Apply Label ---")
        label_id = "Label_7552249412564502433"
        try:
             service.users().messages().modify(userId='me', id=msg['id'], body={'addLabelIds': [label_id]}).execute()
             ctx.log(f"  > Label applied: {label_id}")
        except Exception as e:
             ctx.log(f"  > Warning: Failed to label: {e}")

    ctx.log("--- Analysis Complete ---")
    return cards

def fetch_email_html(message_id):
    try:
        service = get_gmail_service()
        m = service.users().messages().get(userId='me', id=message_id, format='full').execute()
        payload = m['payload']
        _, body_html = extract_email_body(payload)
        
        if not body_html:
            # Fallback to text wrapped in html
            body_text, _ = extract_email_body(payload)
            body_html = f"<pre style='font-family:sans-serif; white-space:pre-wrap;'>{body_text}</pre>"
            
        return body_html
    except Exception as e:
        return f"<h1>Error loading email</h1><p>{str(e)}</p>"
