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
    Key: Account ID, Service, Usage Type, Region, Start Date.
    Note: Total Impact is NOT part of duplicate detection.
    """
    if df.empty:
        return None
        
    mask = (
        (df['Account ID'].astype(str) == str(data['account_id'])) &
        (df['Region'] == data['region']) &
        (df['Services'] == data['services']) & 
        (df['Usage Type'] == data['usage_type']) &
        (df['Start Date'] == data['start_date'])
    )
    
    matches = df[mask]
    if not matches.empty:
        # Return the Timestamp of the first match as string
        return str(matches.iloc[0]['Timestamp'])
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
        'Status': data.get('status', 'Sent')
    }
    
    # 1. Check Daily
    dup_ts = check_duplicate(daily_df, data)
    if dup_ts:
        return {"status": "daily_duplicate", "timestamp": dup_ts}
        
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
    
    return {"status": "success", "timestamp": row['Timestamp']}

def delete_rows(file_type, timestamps):
    """Deletes rows by timestamp from the specified file."""
    file_path = DAILY_FILE if file_type == 'daily' else MASTER_FILE
    df = load_excel(file_path)
    
    if df.empty: return False
    
    # Filter out rows with matching timestamps
    # Convert both to string to be safe
    original_count = len(df)
    df = df[~df['Timestamp'].astype(str).isin([str(ts) for ts in timestamps])]
    
    if len(df) < original_count:
        df.to_excel(file_path, index=False)
        return True
    return False

def get_tracking_data():
    """Returns the contents of both tracking files."""
    daily_df = load_excel(DAILY_FILE)
    master_df = load_excel(MASTER_FILE)
    
    # Ensure Timestamp is string
    if 'Timestamp' in daily_df.columns:
        daily_df['Timestamp'] = daily_df['Timestamp'].astype(str)
    if 'Timestamp' in master_df.columns:
        master_df['Timestamp'] = master_df['Timestamp'].astype(str)
    
    # Replace NaN with None/Empty string for JSON serialization
    daily_df = daily_df.fillna('')
    master_df = master_df.fillna('')
    
    return {
        "daily": daily_df.to_dict(orient='records'),
        "master": master_df.to_dict(orient='records')
    }

def update_status(timestamp, new_status, file_type='master'):
    """Updates the Status field for a specific row in the specified tracking file."""
    file_path = MASTER_FILE if file_type == 'master' else DAILY_FILE
    df = load_excel(file_path)
    
    if df.empty:
        return False
    
    # Find the row with matching timestamp
    mask = df['Timestamp'].astype(str) == str(timestamp)
    
    if not mask.any():
        raise ValueError(f"Row with timestamp {timestamp} not found in {file_type}")
    
    # Update the Status field
    df.loc[mask, 'Status'] = new_status
    
    # Save back to Excel
    try:
        with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Anomalies')
            
            # Formats
            workbook = writer.book
            worksheet = writer.sheets['Anomalies']
            header_format = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3', 'border': 1})
            
            # Apply header format
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                
            return True
    except Exception as e:
        raise Exception(f"Error saving Excel file: {str(e)}")
