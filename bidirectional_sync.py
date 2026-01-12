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
                import traceback
                traceback.print_exc()
            
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
            
            response = requests.get(
                f"{self.backend_url}/api/tally-sync/pending-for-middleware",
                headers={
                    'x-middleware-token': MIDDLEWARE_TOKEN
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
            
            # 🔍 DEBUG: Print all items
            for i, item in enumerate(items, 1):
                print(f"\n   📋 Item {i}:")
                print(f"      Queue ID: {item.get('id')}")
                print(f"      Client: {item.get('client_name')}")
                print(f"      Operation: {item.get('operation')}")
                print(f"      Tally GUID: {item.get('tally_guid')}")
                print(f"      New Data: {json.dumps(item.get('new_data'), indent=6)}")
            
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
        
        print(f"\n📤 Processing Queue Item #{queue_id}")
        print(f"   Client: {client_name}")
        print(f"   Operation: {operation}")
        print(f"   Tally GUID: {tally_guid}")
        
        try:
            # Build Tally XML
            if operation == 'update_address' and 'address' in new_data:
                print(f"   New Address: {new_data['address']}")
                
                success, error, tally_response = self._push_address_to_tally_safe(
                    tally_guid,
                    new_data['address'],
                    client_name  # ← ADD THIS - pass the ledger name
                )
                
                print(f"\n   📊 Tally Push Result:")
                print(f"      Success: {success}")
                print(f"      Error: {error}")
                print(f"      Tally Response: {tally_response[:200] if tally_response else 'None'}...")
                
            else:
                success = False
                error = f"Unsupported operation: {operation}"
                tally_response = None
                print(f"   ❌ {error}")
            
            # Report back to backend
            print(f"\n   📡 Reporting to backend...")
            self._complete_item(queue_id, success, error, tally_response)
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            import traceback
            traceback.print_exc()
            self._complete_item(queue_id, False, str(e), None)
    
    def _push_address_to_tally_safe(self, tally_guid, address, ledger_name):

        try:
            # Step 1: Fetch existing ledger
            print(f"\n   📥 Fetching existing ledger from Tally...")
            
            fetch_xml = f"""
    <ENVELOPE>
        <HEADER>
            <VERSION>1</VERSION>
            <TALLYREQUEST>Export Data</TALLYREQUEST>
            <TYPE>Data</TYPE>
            <ID>Ledger Export</ID>
        </HEADER>
        <BODY>
            <DESC>
                <STATICVARIABLES>
                    <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
                    <SVCURRENTCOMPANY>{self._escape_xml(self.tally_company_name)}</SVCURRENTCOMPANY>
                </STATICVARIABLES>
                <TDL>
                    <TDLMESSAGE>
                        <COLLECTION NAME="LedgerCollection" ISMODIFY="No" ISFIXED="No" ISINITIALIZE="No" ISOPTION="No" ISINTERNAL="No">
                            <TYPE>Ledger</TYPE>
                            <FETCH>*, ADDRESS.LIST</FETCH>
                            <FILTER>FilterByGUID</FILTER>
                        </COLLECTION>
                        <SYSTEM TYPE="Formulae" NAME="FilterByGUID">$$ISGUID:#GUID:{self._escape_xml(tally_guid)}</SYSTEM>
                    </TDLMESSAGE>
                </TDL>
            </DESC>
        </BODY>
    </ENVELOPE>
            """.strip()
            
            response = requests.post(
                self.tally_url,
                data=fetch_xml.encode("utf-8"),
                headers={"Content-Type": "application/xml"},
                timeout=30
            )
            
            if response.status_code != 200:
                return False, f"Failed to fetch ledger: HTTP {response.status_code}", None
            
            print(f"   ✅ Fetched ledger (length: {len(response.text)} bytes)")
            
            # Step 2: Now update with new address
            print(f"\n   🔧 Building update XML...")
            
            address_lines = [line.strip() for line in address.split(",") if line.strip()]
            update_xml = '<ADDRESS.LIST TYPE="String">\n'
            for line in address_lines:
                update_xml += f'    <ADDRESS TYPE="String">{self._escape_xml(line)}</ADDRESS>\n'
            update_xml += '</ADDRESS.LIST>'
            
            # Important: Use the exact ledger name
            alter_xml = f"""
    <ENVELOPE>
        <HEADER>
            <VERSION>1</VERSION>
            <TALLYREQUEST>Import Data</TALLYREQUEST>
            <TYPE>Data</TYPE>
            <ID>Ledger Alter</ID>
        </HEADER>
        <BODY>
            <DESC>
                <STATICVARIABLES>
                    <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
                    <SVCURRENTCOMPANY>{self._escape_xml(self.tally_company_name)}</SVCURRENTCOMPANY>
                </STATICVARIABLES>
            </DESC>
            <DATA>
                <TALLYMESSAGE xmlns:UDF="TallyUDF">
                    <LEDGER NAME="{self._escape_xml(ledger_name)}" ACTION="Alter">
                        <GUID>{self._escape_xml(tally_guid)}</GUID>
                        <ALTERID>1</ALTERID>
                        <NAME>{self._escape_xml(ledger_name)}</NAME>
                        {update_xml}
                    </LEDGER>
                </TALLYMESSAGE>
            </DATA>
        </BODY>
    </ENVELOPE>
            """.strip()
            
            print(f"\n   📤 Sending update to Tally...")
            print(f"   📄 Update XML:")
            print("   " + "="*60)
            for line in alter_xml.split('\n'):
                print(f"   {line}")
            print("   " + "="*60)
            
            response = requests.post(
                self.tally_url,
                data=alter_xml.encode("utf-8"),
                headers={"Content-Type": "application/xml"},
                timeout=30
            )
            
            print(f"\n   📨 Tally Response:")
            print(f"      Status Code: {response.status_code}")
            print(f"   📄 Response:")
            print("   " + "="*60)
            for line in response.text.split('\n'):
                print(f"   {line}")
            print("   " + "="*60)
            
            response_text = response.text.lower()
            
            # Check response
            if "error" in response_text or "<status>0</status>" not in response_text:
                return False, "Tally rejected the update", response.text[:500]
            
            if response.status_code == 200 and "<status>1</status>" in response_text:
                print(f"   ✅ Update successful!")
                return True, None, response.text[:300]
            
            return False, f"Unexpected response: HTTP {response.status_code}", response.text[:500]
            
        except Exception as e:
            print(f"   ❌ Exception: {e}")
            import traceback
            traceback.print_exc()
            return False, str(e), None
    
    def _complete_item(self, queue_id, success, error, tally_response):
        """Report completion status back to backend"""
        try:
            print(f"\n   📡 Sending completion to backend...")
            print(f"      Queue ID: {queue_id}")
            print(f"      Success: {success}")
            print(f"      Error: {error}")
            
            response = requests.post(
                f"{self.backend_url}/api/tally-sync/complete-from-middleware/{queue_id}",
                headers={
                    'Content-Type': 'application/json',
                    'x-middleware-token': MIDDLEWARE_TOKEN
                },
                json={
                    'success': success,
                    'error': error,
                    'tallyResponse': tally_response
                },
                timeout=10
            )
            
            print(f"      Backend Status: {response.status_code}")
            print(f"      Backend Response: {response.text[:200]}")
            
            if response.status_code == 200:
                status = "✅ Completed" if success else "❌ Failed"
                print(f"   {status} - reported to backend")
            else:
                print(f"   ⚠️ Failed to report status to backend: {response.status_code}")
                print(f"   Response: {response.text}")
                
        except Exception as e:
            print(f"   ⚠️ Failed to report to backend: {e}")
            import traceback
            traceback.print_exc()
    
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