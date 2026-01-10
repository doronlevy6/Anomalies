import os
import pandas as pd
from datetime import datetime

DAILY_FILE = os.path.join(os.path.dirname(__file__), 'daily_anomalies.xlsx')
MASTER_FILE = os.path.join(os.path.dirname(__file__), 'master_anomalies.xlsx')

COLUMNS = [
    'Timestamp', 'Company Name', 'Account Name', 'Account ID', 
    'Start Date', 'End Date', 'Region', 'Services', 'Usage Type', 'Total Impact', 'Status'
]

def load_excel(file_path):
    """Loads dataframe from excel or creates empty one if missing."""
    if os.path.exists(file_path):
        try:
            return pd.read_excel(file_path)
        except:
            return pd.DataFrame(columns=COLUMNS)
    return pd.DataFrame(columns=COLUMNS)

def check_duplicate(df, data):
    """
    Checks if anomaly exists in the dataframe.
    Key: Account ID, Region, Services, Usage Type, Start Date, Total Impact.
    """
    if df.empty:
        return None
        
    mask = (
        (df['Account ID'].astype(str) == str(data['account_id'])) &
        (df['Region'] == data['region']) &
        (df['Services'] == data['services']) & 
        (df['Usage Type'] == data['usage_type']) &
        (df['Start Date'] == data['start_date']) &
        (df['Total Impact'] == data['total_impact'])
    )
    
    matches = df[mask]
    if not matches.empty:
        # Return the Timestamp of the first match
        return matches.iloc[0]['Timestamp']
    return None

def export_anomaly(data, force_master=False):
    """
    Exports anomaly to Excel files with logic:
    1. Check Daily: If exists -> BLOCK.
    2. Check Master: If exists -> WARN (unless force_master=True).
    3. Add to both if checks pass.
    """
    daily_df = load_excel(DAILY_FILE)
    master_df = load_excel(MASTER_FILE)
    
    # Prepare row data
    row = {
        'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'Company Name': data.get('company_name'),
        'Account Name': data.get('account_name'),
        'Account ID': data.get('account_id'),
        'Start Date': data.get('start_date'),
        'End Date': data.get('end_date'),
        'Region': data.get('region', ''),
        'Services': data.get('services', ''),
        'Usage Type': data.get('usage_type', ''),
        'Total Impact': data.get('total_impact'),
        'Status': data.get('status', 'Not yet handled')
    }
    
    # 1. Check Daily
    if check_duplicate(daily_df, data):
        return {"status": "daily_duplicate"}
        
    # 2. Check Master
    existing_date = check_duplicate(master_df, data)
    if existing_date and not force_master:
        return {"status": "master_duplicate", "existing_date": existing_date}
        
    # 3. Add to files
    new_row_df = pd.DataFrame([row])
    
    # Append to Daily
    daily_df = pd.concat([daily_df, new_row_df], ignore_index=True)
    daily_df.to_excel(DAILY_FILE, index=False)
    
    # Append to Master
    master_df = pd.concat([master_df, new_row_df], ignore_index=True)
    master_df.to_excel(MASTER_FILE, index=False)
    
    return {"status": "success"}

def clear_daily_file():
    """Clears the daily file (recreates empty with headers)."""
    df = pd.DataFrame(columns=COLUMNS)
    df.to_excel(DAILY_FILE, index=False)
    return True
