"""
Budget LLM Engine - Prompts and parsing for AWS Budget and RI Utilization emails.
"""
import json
import re

def invoke_budget_llm(bedrock, email_data):
    """
    Processes Budget/RI Utilization emails with a specialized prompt.
    """
    model_id = "anthropic.claude-3-haiku-20240307-v1:0"
    
    acc_name = email_data.get('account_name', 'Unknown')
    acc_id = email_data.get('extracted_account_id', 'Unknown')
    poc_name = email_data.get('poc_name', 'Customer')
    message_family = email_data.get('message_family', 'budget_notification')
    
    # Different prompts based on message family
    if message_family == 'ri_utilization_alert':
        extraction_instructions = """
        This is an RI Utilization Alert. Extract:
        1. service_hint: Which AWS service (e.g., "Amazon RDS", "Amazon EC2")
        2. as_of_date: The date mentioned
        3. utilization_budgeted_percent: The budgeted utilization %
        4. utilization_threshold_percent: The threshold %
        5. utilization_actual_percent: The actual utilization %
        6. under_utilized_reservations: Top 3 under-utilized reservations with:
           - subscription_id, instance_type, availability_zone
           - current_utilization_percent, ri_hours_purchased, ri_hours_unused
           - cost_for_unused_ri_hours (USD)
        """
    else:
        extraction_instructions = """
        This is a Budget Cost Alert. Extract:
        1. budget_type: "Cost"
        2. alert_type: "ACTUAL" or "FORECAST"
        3. period_unit: "monthly" or "daily"
        4. budgeted_amount: The budget limit in USD
        5. threshold_amount: The threshold that triggered the alert
        6. actual_amount: The actual spend in USD
        7. reference_text: "for the current month" or "yesterday" etc.
        """
    
    prompt = f"""You are an AWS FinOps Budget Alert Analyst.

Input Email:
FROM: {email_data.get('fromAddress', '')}
SUBJECT: {email_data.get('subject', '')}
BODY: {email_data.get('bodyText', '')}

Context:
Account: {acc_name} ({acc_id})
POC: {poc_name}
Message Type: {message_family}

{extraction_instructions}

Generate outputs in Hebrew and English:

--- TEMPLATE: team_message_he (Internal) ---
היי,
התקבלה התראת {message_family.replace('_', ' ')} עבור חשבון {acc_name} ({acc_id}).
<Details extracted from email>
נא לבדוק ולהחליט אם צריך לעדכן את הלקוח.

--- TEMPLATE: client_message_he (Customer Hebrew) ---
שלום {poc_name},

חשבון: {acc_name} ({acc_id})

<Description of the budget alert or RI utilization issue>

<Specific amounts/percentages>

נשמח לדעת אם אתם מודעים לזה או שזה דורש בדיקה נוספת.

Best regards,
Abra-IT FinOps Team

--- TEMPLATE: client_message_en (Customer English) ---
Hello {poc_name},

Account: {acc_name} ({acc_id})

<Description of the budget alert or RI utilization issue>

<Specific amounts/percentages>

Please let us know if you are aware of this or if it requires further investigation.

Best regards,
Abra-IT FinOps Team

Return ONLY valid JSON:
{{
  "summary_he": "string (2-3 lines summarizing the alert)",
  "details_he": "string (extracted data in Hebrew)",
  "team_message_he": "string",
  "client_message_he": "string",
  "client_message_en": "string",
  "urgency": "low|medium|high",
  "action_required": true,
  "next_action_he": "string",
  "next_action_en": "string",
  "total_impact_usd": "string (e.g., '$150' or 'N/A')",
  "budget_details": {{
    "budget_type": "Cost|RI Utilization",
    "period": "monthly|daily|N/A",
    "budgeted_amount": "string",
    "actual_amount": "string",
    "threshold": "string",
    "utilization_percent": "string (for RI only)"
  }}
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
        print(f"Error invoking Bedrock for budget: {e}")
        return "{}"

def parse_budget_llm_response(text):
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
