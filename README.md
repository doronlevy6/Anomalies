# AWS Cost Anomaly Detection System

An automated workflow for processing AWS Cost Anomaly emails. It fetches emails, analyzes them using Bedrock (Claude 3.5 Sonnet), and generates actionable alerts for both the internal FinOps team and customers.

## 🚀 Quick Start

### 1. Prerequisites
*   Python 3.11+
*   Gmail API Credentials (`credentials.json`)
*   AWS Credentials (for Bedrock)

### 2. Start the Server
Run the following command in your terminal:
```bash
./venv/bin/python app.py
```
> **Note**: The first run will open a browser window to authenticate with Gmail.

### 3. Access the Dashboard
Open your web browser and navigate to:
[http://127.0.0.1:5001](http://127.0.0.1:5001)

---

## 🛠 Operation Guide

1.  **Fetch Emails**: Click **"Fetch Recent Emails"** to scan for unread emails with the subject "Cost anomaly".
2.  **Review Cards**: The system processes emails and displays them as cards:
    *   **Reseller Emails** (Account 262674733103) are automatically **split** into individual cards per Member Account.
    *   **Duplicates** (same account/region/usage in multiple monitors) are automatically filtered (keeping the highest impact).
3.  **Export Tracking**: Click **"📊 Export to Tracking File"** on any card to save details to `anomaly_exports.csv`.
    *   To reset the file, use the **"Clear Tracking File"** button at the top.
4.  **Send Notifications**:
    *   **Team Draft**: Creates an internal email with technical details and next steps.
    *   **Client Draft (HE/EN)**: Creates a customer-facing email (Hebrew or English) with formatted costs and dates.
5.  **View Console**: Click **"View Console"** to jump directly to the anomaly in AWS Cost Explorer.

---

## 📂 Project Structure

*   `app.py`: Flask web server entry point.
*   `workflow_logic.py`: **Core Logic**. Handles Gmail fetching, regex splitting, Bedrock LLM prompting, and card generation.
*   `export_helper.py`: Manages CSV export and clearing.
*   `templates/`: HTML templates (index.html).
*   `static/`: CSS styles.
*   `config/`: Credentials and tokens.
*   `anomaly_exports.csv`: The local database of exported anomalies.

## ✨ Key Features

*   **Intelligent Splitting**: Handles complex multi-account Reseller emails by isolating each member account's data.
*   **Smart Deduplication**: Detects when the same usage is reported by multiple monitors and keeps only the relevant one.
*   **Prompt Engineering**:
    *   Strict grammar rules for Hebrew services (Singular vs Plural).
    *   Smart date formatting (Single day vs Range).
*   **HTML Link Extraction**: Scrapes the email HTML to find deep links to the AWS Console if the LLM misses them.