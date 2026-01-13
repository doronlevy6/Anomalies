import base64
import re
from bs4 import BeautifulSoup

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
