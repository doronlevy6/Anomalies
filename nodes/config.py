import os
import json

# --- Configuration ---
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 'https://www.googleapis.com/auth/gmail.modify']
# Paths are relative to the project root (assuming this runs from project root or handles __file__ correctly)
# We need to be careful with __file__ logic if this file is in nodes/
# Original: os.path.join(os.path.dirname(__file__), 'config', 'credentials.json')
# If __file__ is nodes/config.py, dirname is nodes/. Project root is ..
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(PROJECT_ROOT, 'config', 'credentials.json')
TOKEN_PATH = os.path.join(PROJECT_ROOT, 'config', 'token.json')

def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return json.load(f)
