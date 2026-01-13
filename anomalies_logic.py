
# We must re-export everything app.py expects from anomalies_logic
# 1. WorkflowContext class
# 2. run_anomalies_workflow function
# 3. load_account_map function (for reload-map)
# 4. get_account_map (optional, but good practice)

import queue
from nodes.config import load_config
from nodes.services import get_gmail_service, get_bedrock_client, get_or_create_label, add_label_to_message
from nodes.email_processing import clean_html, extract_email_body, extract_original_metadata
from nodes.account_manager import load_account_map, get_account_map, extract_account_id, ACCOUNT_MAP
from nodes.splitting_logic import split_reseller_email, split_email_by_anomalies, deduplicate_usage_types
from nodes.llm_engine import invoke_llm, parse_llm_response
from nodes.ui_generator import generate_html_card

# --- Global Context ---
class WorkflowContext:
    def __init__(self):
        self.msg_queue = queue.Queue()
        self._stop_event = False
    
    def log(self, text):
        print(text)
        self.msg_queue.put({"type": "log", "text": text})

    def should_stop(self):
        return self._stop_event

    def request_stop(self):
        self._stop_event = True

# --- Main Workflow ---
def run_anomalies_workflow(ctx: WorkflowContext, limit=15):
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

    # Ensure label exists
    label_id = get_or_create_label(service, 'fetched')
    if label_id:
        ctx.log(f"Using label 'fetched' (ID: {label_id})")
    else:
        ctx.log("Warning: Could not create/find 'fetched' label. Tagging will be skipped.")
    
    # Node: Gmail Trigger
    ctx.log("--- Node: Gmail Trigger (Search) ---")
    query = 'in:inbox -label:fetched subject:"Cost anomaly"'
    ctx.log(f"Searching for emails with query: '{query}' (Limit: {limit})...")
    results = service.users().messages().list(userId='me', q=query, maxResults=limit).execute()
    messages = results.get('messages', [])
    ctx.log(f"Found {len(messages)} messages.")
    
    # DEBUG: Print all subjects found to help debug "Waller" issue
    if messages:
        ctx.log("\n--- DEBUG: Incoming Messages List ---")
        for i, m in enumerate(messages):
            try:
                msg_detail = service.users().messages().get(userId='me', id=m['id'], format='metadata').execute()
                headers = {h['name'].lower(): h['value'] for h in msg_detail['payload']['headers']}
                subject = headers.get('subject', 'No Subject')
                ctx.log(f"[{i+1}] ID: {m['id']} | Subject: {subject}")
            except Exception as e:
                ctx.log(f"[{i+1}] ID: {m['id']} | Error fetching metadata: {e}")
        ctx.log("-------------------------------------\n")
    
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
        
        # Iterate Queue and Invoke LLM for each
        for idx, item in enumerate(processing_queue):
            if ctx.should_stop(): break
            
            ctx.log(f"  > Processing Anomaly {idx+1}/{len(processing_queue)}: Account {item['account_id']}...")
            
            # Enrich context for LLM
            current_email_data = email_data.copy()
            current_email_data['bodyText'] = item['text_block']
            current_email_data['extracted_account_id'] = item['account_id']
            
            # Use specific name if available from split, else fallback to map or generic
            if item.get('account_name'):
                current_email_data['account_name'] = item.get('account_name')
            else:
                info = ACCOUNT_MAP.get(item['account_id'], {})
                current_email_data['account_name'] = info.get('accountName', 'Unknown')
                current_email_data['poc_name'] = info.get('pocName', 'Customer')

            # Node: LLM Analysis
            # ctx.log(f"--- Node: AI Analysis (Bedrock) for {current_email_data['extracted_account_id']} ---")
            llm_text = invoke_llm(bedrock, current_email_data)
            
            # Node: Parse JSON
            parsed_data = parse_llm_response(llm_text)
            
            # Merge with original email meta (ID, Date, Subject, etc)
            final_data = {**current_email_data, **parsed_data}
            
            # Node: Generate Card
            # ctx.log("--- Node: Generate HTML Card ---")
            html_card = generate_html_card(ctx, final_data, f"{i}_{idx}")
            
            cards.append(html_card)

        # Tag as fetched
        if label_id:
            add_label_to_message(service, 'me', msg['id'], label_id)
            ctx.log(f"  > Tagged message {msg['id']} as 'fetched'")
    
    ctx.log(f"--- Finished Analysis. Generated {len(cards)} cards. ---")
    return cards

# Load account map on module import (keeps existing behavior)
load_account_map()
