"""
Free Tier LLM Engine - Prompts and parsing for AWS Free Tier limit alert emails.
"""
import json
import re

def invoke_freetier_llm(bedrock, email_data):
    """
    Processes Free Tier alert emails with a specialized prompt.
    """
    model_id = "anthropic.claude-3-haiku-20240307-v1:0"
    
    acc_name = email_data.get('account_name', 'Unknown')
    acc_id = email_data.get('extracted_account_id', 'Unknown')
    poc_name = email_data.get('poc_name', 'Customer')
    
    prompt = f"""You are an AWS FinOps Free Tier Alert Analyst.

Input Email:
FROM: {email_data.get('fromAddress', '')}
SUBJECT: {email_data.get('subject', '')}
BODY: {email_data.get('bodyText', '')}

Context:
Account: {acc_name} ({acc_id})
POC: {poc_name}

Extract the following information from this AWS Free Tier limit alert:
1. billing_month: Which month (e.g., "January")
2. threshold_percent: The alert threshold (e.g., 85)
3. products: Array of products with:
   - product_name (e.g., "AmazonCloudWatch")
   - current_usage_value (number)
   - current_usage_unit (e.g., "Requests")
   - limit_value (number)
   - limit_unit (same as usage_unit)
   - usage_percent (calculated or from email)

Generate customer-facing messages:

--- TEMPLATE: team_message_he (Internal) ---
היי,
התקבלה התראת Free Tier עבור חשבון {acc_name} ({acc_id}).
שירות: [product_name]
שימוש: [current_usage] / [limit] ([percent]%)
חודש: [billing_month]
נא לבדוק אם הלקוח צריך לדעת על זה.

--- TEMPLATE: client_message_he (Customer Hebrew) ---
שלום {poc_name},

התקבלה התראה מ-AWS על שימוש ב-Free Tier עבור חשבון {acc_name} ({acc_id}).

שירות: [product_name]
שימוש נוכחי: [current_usage] [unit]
מגבלת Free Tier: [limit] [unit]
אחוז ניצול: [percent]%

מומלץ לבדוק את השימוש ולשקול אם נדרשת פעולה.

Best regards,
Abra-IT FinOps Team

--- TEMPLATE: client_message_en (Customer English) ---
Hello {poc_name},

AWS Free Tier usage alert for account {acc_name} ({acc_id}).

Service: [product_name]
Current Usage: [current_usage] [unit]
Free Tier Limit: [limit] [unit]
Usage: [percent]%

We recommend reviewing the usage and considering if action is needed.

Best regards,
Abra-IT FinOps Team

Return ONLY valid JSON:
{{
  "summary_he": "string (2-3 lines summarizing the Free Tier alert)",
  "billing_month": "string",
  "threshold_percent": number,
  "products": [
    {{
      "product_name": "string",
      "current_usage_value": number,
      "current_usage_unit": "string",
      "limit_value": number,
      "usage_percent": number
    }}
  ],
  "team_message_he": "string",
  "client_message_he": "string",
  "client_message_en": "string",
  "urgency": "low|medium|high",
  "action_required": false,
  "next_action_he": "string",
  "next_action_en": "string",
  "total_impact_usd": "N/A"
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
        print(f"Error invoking Bedrock for free tier: {e}")
        return "{}"

def parse_freetier_llm_response(text):
    """Parses the LLM response JSON."""
    clean_text = re.sub(r'^```json\s*|\s*```$', '', text.strip(), flags=re.MULTILINE)
    try:
        return json.loads(clean_text)
    except json.JSONDecodeError:
        try:
            match = re.search(r'\{[\s\S]*\}', clean_text)
            if match:
                return json.loads(match.group(0))
        except:
            pass
        return {}
