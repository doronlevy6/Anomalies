import re

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
