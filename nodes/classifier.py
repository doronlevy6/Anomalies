"""
Classifier Node - Determines email type based on sender and subject.
"""

def classify_email(sender: str, subject: str) -> dict:
    """
    Classifies an email and returns metadata for routing.
    
    Returns:
        dict with keys:
            - family: 'cost_anomaly' | 'budget_notification' | 'ri_utilization_alert' | 'unknown'
            - label: 'fetched' | 'budget' | None
            - label_color: hex color for creating label
    """
    sender_lower = sender.lower() if sender else ""
    subject_lower = subject.lower() if subject else ""
    
    # Budget emails - check subject first (more reliable)
    if "aws budget" in subject_lower or "aws budgets" in subject_lower:
        if "ri utilization" in subject_lower:
            return {
                "family": "ri_utilization_alert",
                "label": "budget",
                "label_color": "#fb4934"  # Red
            }
        else:
            return {
                "family": "budget_notification",
                "label": "budget", 
                "label_color": "#fb4934"  # Red
            }
    
    # Fallback: Budget emails from AWS sender
    if "budgets@costalerts.amazonaws.com" in sender_lower:
        if "ri utilization" in subject_lower:
            return {
                "family": "ri_utilization_alert",
                "label": "budget",
                "label_color": "#fb4934"  # Red
            }
        else:
            return {
                "family": "budget_notification",
                "label": "budget", 
                "label_color": "#fb4934"  # Red
            }
    
    # Cost Anomaly emails
    if "cost anomaly" in subject_lower:
        return {
            "family": "cost_anomaly",
            "label": "fetched",
            "label_color": "#83a598"  # Gruvbox blue-gray
        }
    
    # Unknown type
    return {
        "family": "unknown",
        "label": None,
        "label_color": None
    }
