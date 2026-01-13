import os
import boto3
import threading
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

from .config import load_config, SCOPES, TOKEN_PATH

# --- Gmail Service ---
def get_gmail_service():
    creds = None
    config = load_config()
    
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                print(f"Token refresh failed: {e}. Deleting invalid token and re-authenticating...")
                if os.path.exists(TOKEN_PATH):
                    os.remove(TOKEN_PATH)
                creds = None
        
        if not creds:
            # We must use the EXACT redirect URI registered in Google Console
            # Based on user input, it seems to be the n8n one
            redirect_uri = "http://localhost:5678/rest/oauth2-credential/callback"
            
            flow = InstalledAppFlow.from_client_config(config['gmail'], SCOPES)
            flow.redirect_uri = redirect_uri
            
            auth_url, _ = flow.authorization_url(prompt='consent')
            
            print(f"Opening browser for auth: {auth_url}")
            
            # Start a temporary server to listen for the callback
            
            auth_code = None
            
            class AuthHandler(BaseHTTPRequestHandler):
                def do_GET(self):
                    nonlocal auth_code
                    if self.path.startswith("/rest/oauth2-credential/callback"):
                        query = urlparse(self.path).query
                        params = parse_qs(query)
                        if 'code' in params:
                            auth_code = params['code'][0]
                            self.send_response(200)
                            self.send_header('Content-type', 'text/html')
                            self.end_headers()
                            self.wfile.write(b"Authentication successful! You can close this window.")
                            
                            # Spin off a thread to kill server to avoid deadlock in request handler
                            threading.Thread(target=server.shutdown).start()
                        else:
                             self.send_response(400)
                             self.wfile.write(b"No code found.")
                    else:
                        self.send_response(404)
            
            # Attempt to bind to 5678. If n8n is running, this might fail.
            try:
                HTTPServer.allow_reuse_address = True
                server = HTTPServer(('localhost', 5678), AuthHandler)
            except OSError:
                raise Exception("Port 5678 is in use. Please close n8n or any other app using this port to allow authentication.")

            webbrowser.open(auth_url)
            print("Listening on localhost:5678 for authentication...")
            server.serve_forever()
            server.server_close()
            
            if not auth_code:
                raise Exception("Failed to obtain auth code")

            flow.fetch_token(code=auth_code)
            creds = flow.credentials
        
        # Save the credentials for the next run
        with open(TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())

    return build('gmail', 'v1', credentials=creds)

def get_or_create_label(service, label_name):
    try:
        results = service.users().labels().list(userId='me').execute()
        labels = results.get('labels', [])
        for label in labels:
            if label['name'].lower() == label_name.lower():
                return label['id']
        
        # Create
        label_object = {'name': label_name, 'labelListVisibility': 'labelShow', 'messageListVisibility': 'show'}
        created_label = service.users().labels().create(userId='me', body=label_object).execute()
        return created_label['id']
    except Exception as e:
        print(f"Error getting/creating label: {e}")
        return None

def add_label_to_message(service, user_id, msg_id, label_id):
    try:
        body = {'addLabelIds': [label_id], 'removeLabelIds': []}
        service.users().messages().modify(userId=user_id, id=msg_id, body=body).execute()
        return True
    except Exception as e:
        print(f"Error labeling message {msg_id}: {e}")
        return False

# --- AWS Bedrock Service ---
def get_bedrock_client():
    config = load_config()
    aws_conf = config['aws']
    
    return boto3.client(
        service_name='bedrock-runtime',
        region_name=aws_conf.get('region_name', 'us-east-1'),
        aws_access_key_id=aws_conf['access_key_id'],
        aws_secret_access_key=aws_conf['secret_access_key']
    )
