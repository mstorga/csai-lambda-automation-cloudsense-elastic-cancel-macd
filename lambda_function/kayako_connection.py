import requests
from requests.auth import HTTPBasicAuth
import os
import base64
import time


class KayakoConnect:
    def __init__(self):
        self.base_url = f"https://central-supportdesk.kayako.com/api/v1/"
        self.shim_url = f"https://nzm4zzomsptqgmle2tzhmxd27i0bednl.lambda-url.us-east-1.on.aws/api/v2/"
        self.email = os.getenv('kayako_email')
        
        print(f"🔧 KayakoConnect initializing...")
        print(f"📧 Email: {self.email}")
        print(f"🌐 Base URL: {self.base_url}")
        
        # Get and decode password with error handling
        raw_password = os.getenv('kayako_password')
        if raw_password:
            try:
                self.password = base64.b64decode(raw_password).decode('utf-8')
                print(f"🔐 Password: Successfully decoded ({len(self.password)} chars)")
            except Exception as e:
                print(f"❌ Password decode error: {e}")
                self.password = None
        else:
            print(f"❌ No kayako_password found in environment variables")
            self.password = None
    
    def test_connection(self):
        """Test if the Kayako connection works with current credentials"""
        print(f"🧪 Testing Kayako connection...")
        
        if not self.email or not self.password:
            print(f"❌ Missing credentials - Email: {bool(self.email)}, Password: {bool(self.password)}")
            return False
        
        try:
            response = requests.get(
                self.base_url + "departments.json", 
                auth=HTTPBasicAuth(self.email, self.password),
                timeout=10
            )
            
            print(f"🌐 Connection test result: HTTP {response.status_code}")
            
            if response.status_code == 200:
                print(f"✅ Kayako connection successful!")
                return True
            elif response.status_code == 401:
                print(f"❌ Authentication failed (401) - Check credentials")
                return False
            else:
                print(f"⚠️ Unexpected response: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            return False
        
    def get(self, endpoint, is_shim, params=None):
        attempts = 0
        while attempts < 3:
            try:
                url_base = self.shim_url if is_shim else self.base_url
                response = requests.get(url_base + endpoint, auth=HTTPBasicAuth(self.email, self.password), params=params)
                print(f"🌐 API Call: GET {url_base + endpoint} -> HTTP {response.status_code}")
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    print('Rate limit reached, waiting 15 seconds before retrying...')
                    time.sleep(15)
                elif response.status_code == 401:
                    print(f'❌ Authentication failed for endpoint: {endpoint}')
                    return None
                elif response.status_code == 404:
                    print(f'❌ Resource not found (404) for endpoint: {endpoint}')
                    return None
                else:
                    print(f'ERROR: Failed to retrieve: {response.status_code} for endpoint: {endpoint}')
                    return None
            except requests.exceptions.ConnectionError as e:
                print(f'ERROR: Failed to establish connection: {str(e)}')
                break
            except requests.exceptions.RequestException as e:
                print(f'ERROR: Request failed: {str(e)}')
                break 
            attempts += 1

    def post(self, endpoint, is_shim, data=None):
        attempts = 0
        while attempts < 3:
            try:
                url_base = self.shim_url if is_shim else self.base_url
                response = requests.post(url_base + endpoint, auth=HTTPBasicAuth(self.email, self.password), json=data)
                if response.status_code in [201]:
                    return response.json()
                elif response.status_code == 429:
                    print('Rate limit reached, waiting 15 seconds before retrying...')
                    time.sleep(15)
                else:
                    print(f'ERROR: Failed to post data: {response.status_code} for endpoint: {endpoint}')
                    return None
            except requests.exceptions.ConnectionError as e:
                print(f'ERROR: Failed to establish connection: {str(e)}')
                break
            except requests.exceptions.RequestException as e:
                print(f'ERROR: Request failed: {str(e)}')
                break 
            attempts += 1

    def put(self, endpoint, is_shim, data=None):
        attempts = 0
        while attempts < 3:
            try:
                url_base = self.shim_url if is_shim else self.base_url
                response = requests.put(url_base + endpoint, auth=HTTPBasicAuth(self.email, self.password), json=data)
                if response.status_code in [200, 202]:
                    return response.json()
                elif response.status_code == 429:
                    print('Rate limit reached, waiting 15 seconds before retrying...')
                    time.sleep(15)
                else:
                    print(f'ERROR: Failed to update: {response.status_code} for endpoint: {endpoint}')
                    return None
            except requests.exceptions.ConnectionError as e:
                print(f'ERROR: Failed to establish connection: {str(e)}')
                break
            except requests.exceptions.RequestException as e:
                print(f'ERROR: Request failed: {str(e)}')
                break 
            attempts += 1

    def write_internal_note(self, ticket_id, text):
        data = {
            "tickets": [
                {
                    "id": ticket_id,
                    "comment": {
                        "body": text,
                        "public": False
                    }
                }
            ]
        }
        self.put(f"tickets/update_many", True, data)

    def delete_tags(self, ticket_id, tags):
        if not isinstance(tags, list):
            tags = [tags]
        all_tags = self.get(f"cases/{ticket_id}/tags.json", False).get("data")
        new_tag_list = [one.get("name") for one in all_tags if one.get("name") not in tags]
        data = {
            "tags": ",".join(new_tag_list)
        }
        self.put(f"cases/{ticket_id}/tags.json", False, data)

    def add_tags(self, ticket_id, tags):
        if not isinstance(tags, list):
            tags = [tags]
        data = {
            "tags": ", ".join(tags)
        }
        self.post(f"cases/{ticket_id}/tags.json", False, data)
