"""
Data Extractor Module - Extracts structured data from email bodies for clean display.
"""
import re
from typing import Dict, List, Optional


def extract_dates(body_text: str) -> Dict[str, Optional[str]]:
    """
    Extract start and end dates from email body.
    
    Returns:
        dict with 'start' and 'end' keys
    """
    dates = {"start": None, "end": None}
    
    # Try to extract Start Date
    start_match = re.search(r'Start Date:\s*(\d{4}-\d{2}-\d{2})', body_text)
    if start_match:
        dates["start"] = start_match.group(1)
    
    # Try to extract Last Detected Date or End Date
    end_match = re.search(r'(?:Last Detected Date|End Date):\s*(\d{4}-\d{2}-\d{2})', body_text)
    if end_match:
        dates["end"] = end_match.group(1)
    elif dates["start"]:
        dates["end"] = dates["start"]  # Fallback to start if no end found
    
    return dates


def extract_services(body_text: str) -> List[str]:
    """
    Extract AWS services from email body.
    
    Returns:
        list of service names
    """
    services = []
    
    # Pattern: "AWS Service: <service name>"
    service_matches = re.findall(r'AWS Service:\s*([^\n]+)', body_text)
    for service in service_matches:
        service = service.strip()
        if service and service not in services:
            services.append(service)
    
    # Also try "Service: <service name>"
    if not services:
        service_matches = re.findall(r'Service:\s*([^\n]+)', body_text)
        for service in service_matches:
            service = service.strip()
            if service and service not in services:
                services.append(service)
    
    return services


def extract_regions(body_text: str) -> List[str]:
    """
    Extract AWS regions from email body.
    
    Returns:
        list of region names
    """
    regions = []
    
    # Pattern: "Region: <region>"
    region_matches = re.findall(r'Region:\s*([^\n]+)', body_text)
    for region in region_matches:
        region = region.strip()
        if region and region not in regions:
            regions.append(region)
    
    return regions


def extract_usage_types(body_text: str) -> List[str]:
    """
    Extract Usage Types from email body.
    
    Returns:
        list of usage types
    """
    usage_types = []
    
    # Pattern: "Usage Type: <usage type>"
    usage_matches = re.findall(r'Usage Type:\s*([^\n]+)', body_text)
    for usage in usage_matches:
        usage = usage.strip()
        if usage and usage not in usage_types:
            usage_types.append(usage)
    
    return usage_types


def extract_amounts(body_text: str, message_family: str, parsed_data: Dict) -> Dict[str, Optional[str]]:
    """
    Extract monetary amounts from email body and parsed LLM data.
    
    Args:
        body_text: Raw email body text
        message_family: Type of email (cost_anomaly, budget_notification, etc.)
        parsed_data: Parsed data from LLM response
    
    Returns:
        dict with amount keys depending on message type
    """
    amounts = {}
    
    # Total Impact (from LLM or body)
    total_impact = parsed_data.get('total_impact_usd', '')
    if not total_impact or total_impact == 'Unknown':
        # Try to extract from body
        impact_match = re.search(r'Total Impact:\s*\$?([\d,]+\.?\d*)', body_text)
        if impact_match:
            total_impact = f"${impact_match.group(1)}"
    
    amounts['total_impact'] = total_impact if total_impact and total_impact != 'Unknown' else None
    
    # Budget-specific amounts
    if message_family in ['budget_notification', 'ri_utilization_alert']:
        budget_details = parsed_data.get('budget_details', {})
        amounts['budgeted'] = budget_details.get('budgeted_amount')
        amounts['actual'] = budget_details.get('actual_amount')
        amounts['threshold'] = budget_details.get('threshold')
        amounts['utilization_percent'] = budget_details.get('utilization_percent')
    
    return amounts


def extract_structured_data(email_data: Dict, parsed_data: Dict) -> Dict:
    """
    Main function to extract all structured data from email.
    
    Args:
        email_data: Raw email data with bodyText, subject, etc.
        parsed_data: Parsed LLM response data
    
    Returns:
        dict with structured fields for display
    """
    body_text = email_data.get('bodyText', '')
    message_family = email_data.get('message_family', 'cost_anomaly')
    
    # Extract account info
    account_id = email_data.get('extracted_account_id', '') or parsed_data.get('active_member_account_id', '')
    account_name = email_data.get('account_name', 'Unknown')
    
    # Extract all structured fields
    dates = extract_dates(body_text)
    services = extract_services(body_text)
    regions = extract_regions(body_text)
    usage_types = extract_usage_types(body_text)
    amounts = extract_amounts(body_text, message_family, parsed_data)
    
    # Build structured data object
    structured = {
        "account_id": account_id,
        "account_name": account_name,
        "dates": dates,
        "services": services,
        "regions": regions,
        "usage_types": usage_types,
        "amounts": amounts,
        "message_family": message_family
    }
    
    return structured


def generate_structured_html(structured_data: Dict) -> str:
    """
    Generate clean HTML table from structured data.
    
    Args:
        structured_data: Structured data dict from extract_structured_data()
    
    Returns:
        HTML string with formatted table
    """
    message_family = structured_data.get('message_family', 'cost_anomaly')
    
    # Table style
    table_style = """
        width: 100%; 
        border-collapse: collapse; 
        margin: 10px 0; 
        font-size: 0.95em;
        background: white;
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    """
    
    th_style = """
        background: #f3f4f6; 
        padding: 12px; 
        text-align: right; 
        font-weight: 600; 
        border-bottom: 2px solid #e5e7eb;
        color: #374151;
        width: 30%;
    """
    
    td_style = """
        padding: 12px; 
        text-align: right; 
        border-bottom: 1px solid #e5e7eb;
        color: #1f2937;
    """
    
    # Build rows based on message type
    rows = []
    
    # Account (always show)
    if structured_data.get('account_id'):
        account_display = f"{structured_data['account_name']} ({structured_data['account_id']})"
        rows.append(f"<tr><th style='{th_style}'>🏢 חשבון</th><td style='{td_style}'>{account_display}</td></tr>")
    
    # Dates
    dates = structured_data.get('dates', {})
    if dates.get('start') and dates.get('end'):
        if dates['start'] == dates['end']:
            date_display = f"בתאריך {dates['start']}"
        else:
            date_display = f"בין התאריכים {dates['start']} - {dates['end']}"
        rows.append(f"<tr><th style='{th_style}'>📅 תקופה</th><td style='{td_style}'>{date_display}</td></tr>")
    
    # Services
    services = structured_data.get('services', [])
    if services:
        services_display = "<br>".join(services)
        rows.append(f"<tr><th style='{th_style}'>⚙️ שירותים</th><td style='{td_style}'>{services_display}</td></tr>")
    
    # Regions
    regions = structured_data.get('regions', [])
    if regions:
        regions_display = ", ".join(regions)
        rows.append(f"<tr><th style='{th_style}'>🌍 אזורים</th><td style='{td_style}'>{regions_display}</td></tr>")
    
    # Usage Types
    usage_types = structured_data.get('usage_types', [])
    if usage_types:
        usage_display = "<br>".join(usage_types)
        rows.append(f"<tr><th style='{th_style}'>📊 סוג שימוש</th><td style='{td_style}'>{usage_display}</td></tr>")
    
    # Amounts
    amounts = structured_data.get('amounts', {})
    
    if message_family == 'cost_anomaly':
        # Cost Anomaly: Show Total Impact
        if amounts.get('total_impact'):
            rows.append(f"<tr><th style='{th_style}'>💰 סכום משוער</th><td style='{td_style}'><strong style='color:#dc2626; font-size:1.1em;'>{amounts['total_impact']}</strong></td></tr>")
    
    elif message_family in ['budget_notification', 'ri_utilization_alert']:
        # Budget: Show budgeted, actual, threshold
        if amounts.get('budgeted'):
            rows.append(f"<tr><th style='{th_style}'>💵 תקציב</th><td style='{td_style}'>{amounts['budgeted']}</td></tr>")
        if amounts.get('actual'):
            rows.append(f"<tr><th style='{th_style}'>💸 ניצול בפועל</th><td style='{td_style}'><strong style='color:#dc2626;'>{amounts['actual']}</strong></td></tr>")
        if amounts.get('threshold'):
            rows.append(f"<tr><th style='{th_style}'>⚠️ סף התראה</th><td style='{td_style}'>{amounts['threshold']}</td></tr>")
        if amounts.get('utilization_percent'):
            rows.append(f"<tr><th style='{th_style}'>📈 אחוז ניצול</th><td style='{td_style}'>{amounts['utilization_percent']}</td></tr>")
    
    elif message_family == 'free_tier':
        # Free Tier: Show total impact if available
        if amounts.get('total_impact'):
            rows.append(f"<tr><th style='{th_style}'>💰 סכום משוער</th><td style='{td_style}'><strong style='color:#dc2626;'>{amounts['total_impact']}</strong></td></tr>")
    
    # Build final HTML
    if not rows:
        return "<div style='padding:12px; color:#6b7280; text-align:center;'>אין נתונים מובנים זמינים</div>"
    
    html = f"<table style='{table_style}'>\n"
    html += "\n".join(rows)
    html += "\n</table>"
    
    return html
