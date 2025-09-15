import os
import webbrowser
import http.server
import socketserver
import json
from urllib.parse import urlparse, parse_qs
from xero_python.api_client import ApiClient
from xero_python.api_client.oauth2 import OAuth2Token
from requests_oauthlib import OAuth2Session

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')
TOKEN_FILE = 'xero_token.json'  # Using JSON for better readability and consistency

def get_authorization_url():
    """Generate the authorization URL for Xero OAuth2"""
    session = OAuth2Session(
        CLIENT_ID,
        redirect_uri=REDIRECT_URI,
        scope=['openid', 'profile', 'email', 'accounting.transactions', 'accounting.contacts', 'offline_access']
    )
    authorization_url, state = session.authorization_url(
        'https://login.xero.com/identity/connect/authorize'
    )
    return authorization_url

class OAuthCallbackHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # Parse the callback URL
        parsed_url = urlparse(self.path)
        if parsed_url.path == '/callback':
            query_params = parse_qs(parsed_url.query)
            if 'code' in query_params:
                code = query_params['code'][0]
                self.handle_oauth_callback(code)
            else:
                self.send_error(400, "No authorization code received")
        else:
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"<html><body><h1>Xero OAuth2 Callback</h1><p>You can close this window now.</p></body></html>")
    
    def handle_oauth_callback(self, code):
        """Exchange the authorization code for tokens"""
        try:
            session = OAuth2Session(CLIENT_ID, redirect_uri=REDIRECT_URI)
            token = session.fetch_token(
                'https://identity.xero.com/connect/token',
                code=code,
                client_secret=CLIENT_SECRET
            )
            
            # Save the token with required fields
            token_data = {
                'access_token': token['access_token'],
                'refresh_token': token.get('refresh_token', ''),
                'token_type': token.get('token_type', 'Bearer'),
                'expires_at': token.get('expires_at', 0),
                'expires_in': token.get('expires_in', 0),
                'scope': token.get('scope', ''),  # Keep as string, not list
                'id_token': token.get('id_token', '')
            }
            
            # Save the token as JSON
            with open(TOKEN_FILE, 'w') as f:
                json.dump(token_data, f)
            
            # Send success response
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"""
                <html>
                <body>
                    <h1>Authentication Successful!</h1>
                    <p>You can now close this window and return to your application.</p>
                </body>
                </html>
            """)
            
            # Shutdown the server after successful authentication
            import threading
            threading.Thread(target=self.server.shutdown, daemon=True).start()
            
        except Exception as e:
            self.send_error(500, f"Error during authentication: {str(e)}")

def main():
    # Start the local server
    with socketserver.TCPServer(("localhost", 5000), OAuthCallbackHandler) as httpd:
        print("Serving on port 5000...")
        
        # Get the authorization URL and open it in the default browser
        auth_url = get_authorization_url()
        print(f"Open {auth_url} in your browser to start the OAuth flow")
        webbrowser.open(auth_url)
        
        # Handle one request and then exit
        httpd.handle_request()
    
    print("Authentication flow completed. You can now run xero_exporter.py")

if __name__ == "__main__":
    main()
