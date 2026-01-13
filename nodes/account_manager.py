import os
import pandas as pd
import re

ACCOUNT_MAP = {}

def load_account_map():
    global ACCOUNT_MAP
    try:
        # Assuming we are in nodes/account_manager.py, and templates is in parent/templates
        PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
        excel_path = os.path.join(PROJECT_ROOT, 'templates', 'mailsToFlow1.xlsx')
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

def extract_account_id(text=""):
    if not text: return ""
    # Allow for dashes or spaces in the 12-digit number (e.g. 123-456-789-012)
    match = re.search(r'\b\d{3}[- ]?\d{3}[- ]?\d{3}[- ]?\d{3}\b', text)
    if match:
        return match.group(0).replace('-', '').replace(' ', '')
    # Fallback to simple 12 digits
    match_simple = re.search(r'\b\d{12}\b', text)
    return match_simple.group(0) if match_simple else ""

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
