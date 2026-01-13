import json
import re

def invoke_llm(bedrock, email_data):
    """
    Step 2: Standard Processor.
    Takes a single-account context (real or virtual) and generates the final formatted JSON.
    """
    model_id = "anthropic.claude-3-haiku-20240307-v1:0"
    
    # Context should already be enriched by the caller
    acc_name = email_data.get('account_name', 'Unknown')
    acc_id = email_data.get('extracted_account_id', 'Unknown')
    poc_name = email_data.get('poc_name', 'Customer')
    
    # Consistent Templates
    templates_block = f"""
    **CRITICAL GRAMMAR RULES - MUST FOLLOW**:
    
    1. DATES:
       - IF Start Date == End Date (Same Day): Write "בתאריך YYYY-MM-DD" (singular)
       - IF Start Date != End Date (Range): Write "בין התאריכים YYYY-MM-DD - YYYY-MM-DD" (range)
       - NEVER write "בין התאריכים" when dates are identical!
       - Compare dates FIRST before writing!
    
    2. SERVICES - COUNT THEM FIRST:
       - Count how many DISTINCT AWS services you found in the anomaly text
       - IF count == 1: Write "בשירות: <Service Name>" (singular)
         Example: "בשירות: Amazon Simple Storage Service"
         Example: "בשירות: Amazon Elastic Container Service"
       - IF count > 1: Write "במספר שירותים: <Service 1, Service 2>" (plural)
         Example: "במספר שירותים: Amazon S3, Amazon EC2"
       - DO NOT use plural "במספר שירותים" when you only detected ONE service!
    
    --- TEMPLATE: team_message_he (Internal) ---
    היי,
    זוהו מספר חריגות ב-Member Account {acc_name} ({acc_id})
    <Dates>, <Services Text>.

    Region(s): <Regions>.
    Usage Type: <Usage Type>.
    היקף משוער: $<Total Amount>.
    רק מוודא שניתן להודיע ללקוח וזה לא משהו בצד שלנו.

    --- TEMPLATE: client_message_he (Customer Hebrew) ---
    שלום {poc_name},

    חשבון: {acc_name} ({acc_id})

    <Description of the usage increase, mentioning specific services or components>

    <Dates>, זוהתה חריגה בהיקף משוער של <$Amount>.

    [INSTRUCTION FOR <Dates>]:
    * Example 1 (Same Day): Start=2025-05-10, End=2025-05-10 => Write: "בתאריך 2025-05-10"
    * Example 2 (Range): Start=2025-05-10, End=2025-05-12 => Write: "בין התאריכים 2025-05-10 - 2025-05-12"
    * CRITICAL: DO NOT write "בין התאריכים" if Start equals End. Always check dates first.

    השימוש התרכז ב:
    - אזור: <Region>
    - שירות: <Service>
    - Usage Type: <Usage Type>

    היינו רוצים לדעת האם אתם מודעים לעלייה בשימושים הללו,
    או שמדובר במשהו שדורש בדיקה מעמיקה יותר מצדנו.

    אנחנו כאן לשירותכם ולכל שאלה.

    Best regards,
    Abra-IT FinOps Team

    --- TEMPLATE: client_message_en (Customer English) ---
    Hello {poc_name},

    Account: {acc_name} ({acc_id})

    <Description of the usage increase, mentioning specific services or components>

    <Dates>, an anomaly was detected with an estimated scope of <$Amount>.

    [INSTRUCTION FOR <Dates>]:
    * Example 1 (Same Day): Start=2024-01-01, End=2024-01-01 => Write: "On 2024-01-01"
    * Example 2 (Range): Start=2024-01-01, End=2024-01-02 => Write: "Between 2024-01-01 - 2024-01-02"
    * CRITICAL: DO NOT write "Between" if Start equals End. Always check dates first.

    The usage was concentrated in:
    - Region: <Region>
    - Service: <Service>
    - Usage Type: <Usage Type>

    We would like to check whether you are already aware of this increase in usage,
    or if this is something that requires a deeper investigation on our side.

    We are here for any questions or clarifications.

    Best regards,
    Abra-IT FinOps Team
    """

    prompt = f"""You are an AWS Cost Anomaly Detection email processor for a reseller FinOps workflow.
    
    Input:
    FROM_NAME: {email_data.get('fromName', '')}
    FROM_ADDRESS: {email_data.get('fromAddress', '')}
    SUBJECT: {email_data.get('subject', '')}
    BODY_TEXT: {email_data.get('bodyText', '')}

    KNOWN CONTEXT:
    Member Account Name: {acc_name}
    Member Account ID: {acc_id}
    POC Name: {poc_name}

    Requirements:
    1) Extract anomalies ONLY from the email content.
    2) Output a Hebrew grouped anomaly list string.
    3) Build messages using these EXACT templates:
    
    {templates_block}

    ------------------------------------------------

    4) console_link: URL if present.
    5) urgency: low/medium/high
    6) action_required: true/false
    7) next_action_he / en: imperative step.
    8) total_impact_usd: "$Amount" (Extract the most relevant cost figure from the text).
    9) summary_he: A concise Hebrew summary with the following structured format:
       - חשבון: {acc_name} ({acc_id})
       - תקופה: <Dates in format matching the date rules above>
       - סכום: <$Amount>
       - שירות: <Service name(s)>
       - סוג שימוש: <Usage Type>
       
       Write this as a clean, readable summary that includes all these fields.

    Return ONLY valid JSON with EXACTLY these keys:
    {{
      "fromName":"string",
      "fromAddress":"string",
      "subject":"string",
      "summary_he":"string",
      "anomalies_he":"string",
      "active_member_account_id":"string",
      "team_message_he":"string",
      "client_message_he":"string",
      "client_message_en":"string",
      "urgency":"low|medium|high",
      "action_required":true,
      "next_action_he":"string",
      "next_action_en":"string",
      "console_link":"string",
      "total_impact_usd": "string"
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
        print(f"Error invoking Bedrock: {e}")
        return "{}"

def parse_llm_response(text):
    clean_text = re.sub(r'^```json\s*|\s*```$', '', text.strip(), flags=re.MULTILINE)
    try:
        return json.loads(clean_text)
    except json.JSONDecodeError:
        # Fallback regex extraction if JSON is malformed
        try:
             # Try to find a list first
             match_list = re.search(r'\[[\s\S]*\]', clean_text)
             if match_list:
                 return json.loads(match_list.group(0))
             
             # Try to find an object
             match_obj = re.search(r'\{[\s\S]*\}', clean_text)
             if match_obj:
                 return json.loads(match_obj.group(0))
        except:
            pass
        return {}
