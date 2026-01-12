import requests
import json
import time
import threading
from datetime import datetime
from config import SERVER_HEALTH_URL, MIDDLEWARE_TOKEN, TALLY_URL

class BidirectionalSync:
    def __init__(self, company_id, tally_company_name, username="", password=""):
        self.company_id = company_id
        self.tally_company_name = tally_company_name
        self.username = username
        self.password = password
        
        self.backend_url = SERVER_HEALTH_URL.rstrip('/')
        self.tally_url = TALLY_URL
        
        self.running = False
        self.poll_thread = None
    
    def start_polling(self, interval=30):
        """Start polling backend for pending updates"""
        self.running = True
        self.poll_thread = threading.Thread(target=self._poll_loop, args=(interval,), daemon=True)
        self.poll_thread.start()
        print(f"✅ Started polling backend every {interval} seconds")
    
    def stop_polling(self):
        """Stop polling"""
        self.running = False
        if self.poll_thread:
            self.poll_thread.join(timeout=5)
        print("ℹ️ Stopped polling backend")
    
    def _poll_loop(self, interval):
        """Background polling loop"""
        while self.running:
            try:
                self._fetch_and_process_pending()
            except Exception as e:
                print(f"❌ Polling error: {e}")
            
            # Sleep in small chunks to allow quick shutdown
            for _ in range(interval):
                if not self.running:
                    break
                time.sleep(1)
    
    def _fetch_and_process_pending(self):
        try:
            print(f"\n🔍 Polling backend: {self.backend_url}/api/tally-sync/pending-for-middleware")
            print(f"   Company ID: {self.company_id}")
            print(f"   Token: {MIDDLEWARE_TOKEN[:10]}...")
            
            # ✅ FIX: Use correct header name for middleware authentication
            response = requests.get(
                f"{self.backend_url}/api/tally-sync/pending-for-middleware",
                headers={
                    'x-middleware-token': MIDDLEWARE_TOKEN  # ✅ Correct header name
                },
                params={
                    'companyId': self.company_id,
                    'limit': 20
                },
                timeout=10
            )
            
            print(f"   Response status: {response.status_code}")
            
            if response.status_code == 401:
                print(f"   ❌ Authentication failed!")
                print(f"   Response: {response.text}")
                print(f"   Expected token: {MIDDLEWARE_TOKEN[:10]}...")
                return
            
            if response.status_code != 200:
                print(f"   ⚠️ Backend returned status {response.status_code}")
                print(f"   Response: {response.text}")
                return
            
            data = response.json()
            items = data.get('items', [])
            
            if len(items) == 0:
                print(f"   ✅ No pending items")
                return
            
            print(f"   ✅ Found {len(items)} pending items")
            
            # Process each item
            for item in items:
                self._process_single_item(item)
            
        except requests.exceptions.RequestException as e:
            print(f"   ⚠️ Connection error: {e}")
        except Exception as e:
            print(f"   ❌ Processing error: {e}")
            import traceback
            traceback.print_exc()
    
    def _process_single_item(self, item):
        """Process a single queue item - push update to Tally"""
        queue_id = item['id']
        client_name = item['client_name']
        operation = item['operation']
        new_data = item['new_data']
        tally_guid = item['tally_guid']
        
        print(f"\n📤 Processing: {client_name} ({operation})")
        
        try:
            # Build Tally XML
            if operation == 'update_address' and 'address' in new_data:
                success, error, tally_response = self._push_address_to_tally(
                    tally_guid,
                    new_data['address']
                )
            else:
                success = False
                error = f"Unsupported operation: {operation}"
                tally_response = None
            
            # Report back to backend
            self._complete_item(queue_id, success, error, tally_response)
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            self._complete_item(queue_id, False, str(e), None)
    
    def _push_address_to_tally(self, tally_guid, address):
        """Push address update to Tally"""
        try:
            # Build XML
            login_block = ""
            if self.username and self.password:
                login_block = f"""
                    <LOGIN>
                        <USERNAME>{self._escape_xml(self.username)}</USERNAME>
                        <PASSWORD>{self._escape_xml(self.password)}</PASSWORD>
                    </LOGIN>
                """
            
            address_lines = address.split(",")
            update_xml = "<ADDRESS.LIST>\n"
            for line in address_lines:
                if line.strip():
                    update_xml += f"    <ADDRESS>{self._escape_xml(line.strip())}</ADDRESS>\n"
            update_xml += "</ADDRESS.LIST>"
            
            xml_payload = f"""
<ENVELOPE>
    <HEADER>
        <VERSION>1</VERSION>
        <TALLYREQUEST>Import Data</TALLYREQUEST>
        <TYPE>Data</TYPE>
        <ID>Ledger Update</ID>
    </HEADER>
    <BODY>
        <DESC>
            <STATICVARIABLES>
                <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
                <SVCURRENTCOMPANY>{self._escape_xml(self.tally_company_name)}</SVCURRENTCOMPANY>
                {login_block}
            </STATICVARIABLES>
        </DESC>
        <DATA>
            <TALLYMESSAGE>
                <LEDGER ACTION="Alter">
                    <GUID>{self._escape_xml(tally_guid)}</GUID>
                    {update_xml}
                </LEDGER>
            </TALLYMESSAGE>
        </DATA>
    </BODY>
</ENVELOPE>
            """.strip()
            
            # Send to Tally
            response = requests.post(
                self.tally_url,
                data=xml_payload.encode("utf-8"),
                headers={"Content-Type": "application/xml"},
                timeout=30
            )
            
            response_text = response.text.lower()
            
            if "error" in response_text or "failed" in response_text:
                return False, "Tally rejected the update", response.text[:500]
            
            if response.status_code == 200:
                print(f"   ✅ Successfully updated in Tally")
                return True, None, response.text[:300]
            
            return False, f"HTTP {response.status_code}", response.text[:500]
            
        except Exception as e:
            return False, str(e), None
    
    def _complete_item(self, queue_id, success, error, tally_response):
        """Report completion status back to backend"""
        try:
            response = requests.post(
                f"{self.backend_url}/api/tally-sync/complete-from-middleware/{queue_id}",
                headers={
                    'Content-Type': 'application/json',
                    'x-middleware-token': MIDDLEWARE_TOKEN  # ✅ Use correct header
                },
                json={
                    'success': success,
                    'error': error,
                    'tallyResponse': tally_response
                },
                timeout=10
            )
            
            if response.status_code == 200:
                status = "✅ Completed" if success else "❌ Failed"
                print(f"   {status} - reported to backend")
            else:
                print(f"   ⚠️ Failed to report status to backend: {response.status_code}")
                print(f"   Response: {response.text}")
                
        except Exception as e:
            print(f"   ⚠️ Failed to report to backend: {e}")
    
    @staticmethod
    def _escape_xml(text):
        """Escape XML special characters"""
        import html
        if text is None:
            return ""
        return html.escape(str(text))


# Example usage
if __name__ == "__main__":
    # Initialize
    sync = BidirectionalSync(
        company_id="your-company-uuid-here",
        tally_company_name="Your Company Name",
        username="",  # Optional
        password=""   # Optional
    )
    
    # Start polling
    sync.start_polling(interval=30)  # Poll every 30 seconds
    
    print("Press Ctrl+C to stop...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        sync.stop_polling()
        print("\n👋 Goodbye!")