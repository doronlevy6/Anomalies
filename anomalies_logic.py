
# Unified Workflow Orchestrator - Handles Cost Anomaly AND Budget Alerts

import queue
from nodes.config import load_config
from nodes.services import get_gmail_service, get_bedrock_client, get_or_create_label, add_label_to_message
from nodes.email_processing import clean_html, extract_email_body, extract_original_metadata
from nodes.account_manager import load_account_map, get_account_map, extract_account_id, ACCOUNT_MAP
from nodes.splitting_logic import split_reseller_email, split_email_by_anomalies, deduplicate_usage_types
from nodes.llm_engine import invoke_llm, parse_llm_response
from nodes.budget_llm_engine import invoke_budget_llm, parse_budget_llm_response
from nodes.freetier_llm_engine import invoke_freetier_llm, parse_freetier_llm_response
from nodes.classifier import classify_email
from nodes.ui_generator import generate_html_card

# --- Global Context ---
class WorkflowContext:
    def __init__(self):
        self.msg_queue = queue.Queue()
        self._stop_event = False
    
    def log(self, text):
        print(text)
        self.msg_queue.put({"type": "log", "text": text})

    def emit_node(self, node_id, status, message=""):
        """Emit node status for visual workflow display.
        status: 'pending' | 'active' | 'done' | 'error'
        """
        self.msg_queue.put({
            "type": "node",
            "node_id": node_id,
            "status": status,
            "message": message
        })

    def should_stop(self):
        return self._stop_event

    def request_stop(self):
        self._stop_event = True

# --- Main Unified Workflow ---
def run_anomalies_workflow(ctx: WorkflowContext, limit=15):
    ctx.log("--- Starting Unified Analysis (Anomaly + Budget) ---")
    
    if ctx.should_stop(): return []
    
    ctx.emit_node("gmail", "active", "Connecting to Gmail...")
    ctx.log("Step 1: Authenticating with Gmail...")
    try:
        service = get_gmail_service()
    except Exception as e:
        ctx.emit_node("gmail", "error", str(e))
        ctx.log(f"Error authenticating Gmail: {e}")
        raise e
    ctx.emit_node("gmail", "done")
        
    if ctx.should_stop(): return []
    
    ctx.log("Step 2: Authenticating with AWS Bedrock...")
    try:
        bedrock = get_bedrock_client()
    except Exception as e:
        ctx.log(f"Error authenticating AWS: {e}")
        raise e

    # Ensure labels exist (using Gmail allowed colors)
    # See palette: #fb4c2f (red), #16a765 (green), #4986e7 (blue), #00bcd4 (cyan)
    fetched_label_id = get_or_create_label(service, 'fetched', '#16a765')  # Green (Requested)
    budget_label_id = get_or_create_label(service, 'budget', '#fb4c2f')    # Red
    freetier_label_id = get_or_create_label(service, 'freetier', '#4986e7')  # Blue (Swapped with old fetched color)
    
    if fetched_label_id:
        ctx.log(f"Label 'fetched' ready (ID: {fetched_label_id})")
    if budget_label_id:
        ctx.log(f"Label 'budget' ready (ID: {budget_label_id})")
    if freetier_label_id:
        ctx.log(f"Label 'freetier' ready (ID: {freetier_label_id})")
    
    # Node: Gmail Trigger - UNIFIED QUERY
    ctx.emit_node("fetch", "active", "Searching emails...")
    ctx.log("--- Node: Gmail Trigger (Unified Search) ---")
    query = 'in:inbox -label:fetched -label:budget -label:freetier (subject:"Cost anomaly" OR subject:"AWS Budgets" OR subject:"AWS Free Tier" OR from:budgets@costalerts.amazonaws.com OR from:freetier@costalerts.amazonaws.com)'
    ctx.log(f"Searching with query: '{query}' (Limit: {limit})...")
    results = service.users().messages().list(userId='me', q=query, maxResults=limit).execute()
    messages = results.get('messages', [])
    ctx.log(f"Found {len(messages)} messages.")
    ctx.emit_node("fetch", "done", f"Found {len(messages)} emails")
    
    # DEBUG: Print all subjects
    if messages:
        ctx.log("\n--- Incoming Messages ---")
        for i, m in enumerate(messages):
            try:
                msg_detail = service.users().messages().get(userId='me', id=m['id'], format='metadata').execute()
                headers = {h['name'].lower(): h['value'] for h in msg_detail['payload']['headers']}
                subject = headers.get('subject', 'No Subject')
                sender = headers.get('from', '')
                ctx.log(f"[{i+1}] {subject[:60]}...")
            except Exception as e:
                ctx.log(f"[{i+1}] Error: {e}")
        ctx.log("-------------------------\n")
    
    cards = []
    
    for i, msg in enumerate(messages):
        if ctx.should_stop():
            ctx.log("Execution stopped.")
            break
            
        ctx.log(f"\n--- Processing Message {i+1}/{len(messages)} ---")
        
        # Reset nodes for new message
        ctx.emit_node("fetch", "active", "Fetching content...")
        ctx.emit_node("classify", "pending", "")
        ctx.emit_node("account", "pending", "")
        ctx.emit_node("llm", "pending", "")
        ctx.emit_node("card", "pending", "")
        ctx.emit_node("tag", "pending", "")
        
        # Node: Fetch Content
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
        
        # Node: Classify Email Type
        ctx.emit_node("classify", "active", "Classifying...")
        classification = classify_email(email_data['fromAddress'], email_data['subject'])
        message_family = classification['family']
        target_label = classification['label']
        
        ctx.log(f"  > Classified as: {message_family} → label: {target_label}")
        email_data['message_family'] = message_family
        ctx.emit_node("classify", "done", message_family)
        
        # Skip unknown types
        if message_family == 'unknown':
            ctx.log(f"  > Skipping unknown email type")
            continue
        
        # Node: Metadata Extraction (for forwarded emails)
        orig_meta = extract_original_metadata(body_text, headers)
        if orig_meta:
            ctx.log(f"  > Detected forwarded email. Updating metadata.")
            email_data.update(orig_meta)
        
        if ctx.should_stop(): break
        
        # Node: Account Identification
        ctx.emit_node("account", "active", "Identifying account...")
        active_account_id = extract_account_id(email_data.get('subject', '')) or extract_account_id(email_data.get('bodyText', ''))
        account_info = ACCOUNT_MAP.get(active_account_id, {})
        account_name = account_info.get('accountName', 'Unknown')
        poc_name = account_info.get('pocName', 'Customer')
        
        ctx.log(f"  > Account: {active_account_id} ({account_name})")
        ctx.emit_node("account", "done", account_name)
        
        email_data['extracted_account_id'] = active_account_id
        email_data['account_name'] = account_name
        email_data['poc_name'] = poc_name
        
        # ============================================
        # BRANCH: Cost Anomaly vs Budget/RI
        # ============================================
        
        if message_family == 'cost_anomaly':
            # --- COST ANOMALY FLOW ---
            ctx.log("  > Route: COST ANOMALY")
            
            # Determine sub-route (Reseller vs Standard)
            is_reseller = (active_account_id == '262674733103')
            
            processing_queue = []
            
            if is_reseller:
                ctx.log("  > Splitting Reseller email...")
                split_results = split_reseller_email(email_data.get('bodyText', ''))
                split_results = deduplicate_usage_types(split_results, email_data.get('bodyText', ''))
                ctx.log(f"  > {len(split_results)} unique anomalies")
                
                for split_item in split_results:
                    processing_queue.append({
                        'account_id': split_item['account_id'],
                        'account_name': split_item['account_name'],
                        'text_block': split_item['text_block']
                    })
            else:
                ctx.log("  > Splitting Standard email...")
                split_results = split_email_by_anomalies(email_data.get('bodyText', ''), active_account_id, account_name)
                if len(split_results) > 1:
                    split_results = deduplicate_usage_types(split_results, email_data.get('bodyText', ''))
                ctx.log(f"  > {len(split_results)} unique anomalies")
                
                for split_item in split_results:
                    processing_queue.append({
                        'account_id': split_item['account_id'],
                        'account_name': split_item['account_name'],
                        'text_block': split_item['text_block']
                    })
            
            # Process each anomaly
            for idx, item in enumerate(processing_queue):
                if ctx.should_stop(): break
                
                ctx.log(f"  > Processing Anomaly {idx+1}/{len(processing_queue)}: {item['account_id']}...")
                
                # Reset item-specific nodes
                ctx.emit_node("llm", "pending", "")
                ctx.emit_node("card", "pending", "")
                
                current_email_data = email_data.copy()
                current_email_data['bodyText'] = item['text_block']
                current_email_data['extracted_account_id'] = item['account_id']
                
                if item.get('account_name'):
                    current_email_data['account_name'] = item.get('account_name')
                else:
                    info = ACCOUNT_MAP.get(item['account_id'], {})
                    current_email_data['account_name'] = info.get('accountName', 'Unknown')
                    current_email_data['poc_name'] = info.get('pocName', 'Customer')

                # LLM Analysis (Anomaly)
                ctx.emit_node("llm", "active", f"Analyzing {item['account_id']}...")
                llm_text = invoke_llm(bedrock, current_email_data)
                parsed_data = parse_llm_response(llm_text)
                ctx.emit_node("llm", "done")
                
                final_data = {**current_email_data, **parsed_data}
                ctx.emit_node("card", "active", "Generating card...")
                html_card = generate_html_card(ctx, final_data, f"{i}_{idx}")
                cards.append(html_card)
                ctx.emit_node("card", "done")
            
            # Tag with 'fetched'
            ctx.emit_node("tag", "active", "Tagging email...")
            if fetched_label_id:
                add_label_to_message(service, 'me', msg['id'], fetched_label_id)
                ctx.log(f"  > Tagged as 'fetched'")
            ctx.emit_node("tag", "done")
        
        elif message_family == 'free_tier':
            # --- FREE TIER FLOW ---
            ctx.log(f"  > Route: FREE TIER")
            
            # LLM Analysis (Free Tier)
            ctx.emit_node("llm", "active", "Analyzing Free Tier...")
            llm_text = invoke_freetier_llm(bedrock, email_data)
            parsed_data = parse_freetier_llm_response(llm_text)
            ctx.emit_node("llm", "done")
            
            final_data = {**email_data, **parsed_data}
            ctx.emit_node("card", "active", "Generating card...")
            html_card = generate_html_card(ctx, final_data, f"{i}_freetier")
            cards.append(html_card)
            ctx.emit_node("card", "done")
            
            # Tag with 'freetier'
            ctx.emit_node("tag", "active", "Tagging email...")
            if freetier_label_id:
                add_label_to_message(service, 'me', msg['id'], freetier_label_id)
                ctx.log(f"  > Tagged as 'freetier'")
            ctx.emit_node("tag", "done")

        else:
            # --- BUDGET / RI UTILIZATION FLOW ---
            ctx.log(f"  > Route: BUDGET ({message_family})")
            
            # No splitting needed for budget emails (single account per email)
            # LLM Analysis (Budget)
            ctx.emit_node("llm", "active", "Analyzing budget...")
            llm_text = invoke_budget_llm(bedrock, email_data)
            parsed_data = parse_budget_llm_response(llm_text)
            ctx.emit_node("llm", "done")
            
            final_data = {**email_data, **parsed_data}
            ctx.emit_node("card", "active", "Generating card...")
            html_card = generate_html_card(ctx, final_data, f"{i}_budget")
            cards.append(html_card)
            ctx.emit_node("card", "done")
            
            # Tag with 'budget'
            ctx.emit_node("tag", "active", "Tagging email...")
            if budget_label_id:
                add_label_to_message(service, 'me', msg['id'], budget_label_id)
                ctx.log(f"  > Tagged as 'budget'")
            ctx.emit_node("tag", "done")
    
    ctx.log(f"--- Finished. Generated {len(cards)} cards. ---")
    return cards

# Load account map on module import
load_account_map()

def fetch_email_html(message_id):
    """
    Fetches the raw HTML content of a specific email by ID.
    Used for the 'View Original Email' feature.
    """
    try:
        service = get_gmail_service()
        m = service.users().messages().get(userId='me', id=message_id, format='full').execute()
        
        # We reuse extract_email_body but just return the HTML part
        _, body_html = extract_email_body(m['payload'])
        
        # If no HTML found, fallback to text wrapping
        if not body_html:
            body_text, _ = extract_email_body(m['payload'])
            body_html = f"<pre style='white-space: pre-wrap; font-family: monospace;'>{body_text}</pre>"
            
        return body_html
    except Exception as e:
        return f"<h1>Error loading email</h1><p>{str(e)}</p>"
