import requests
import json
from config import MIDDLEWARE_TOKEN
from flask import Flask, request, jsonify


app = Flask(__name__)
TALLY_URL = "http://localhost:9000"


class BackendUploader:
    def __init__(self, backend_url, middleware_token):
        self.backend_url = backend_url.rstrip('/')
        self.middleware_token = middleware_token
        self.sync_endpoint = f"{self.backend_url}/api/sync/tally-clients"
    
    def test_connection(self):
        """Test if backend is reachable"""
        try:
            response = requests.get(self.backend_url, timeout=5)
            return response.status_code == 200
        except Exception as e:
            print(f"❌ Backend connection test failed: {e}")
            return False
    
    def upload_clients(self, clients_data, company_id, batch_size=100):
        """
        Upload clients to backend in batches
        
        Args:
            clients_data: List of client dictionaries
            company_id: UUID of the company
            batch_size: Number of clients per batch
        
        Returns:
            dict: Upload summary with counts
        """
        if not clients_data:
            return {
                'success': True,
                'message': 'No clients to upload',
                'summary': {
                    'total': 0,
                    'new': 0,
                    'updated': 0,
                    'failed': 0
                }
            }
        
        total_clients = len(clients_data)
        num_batches = (total_clients + batch_size - 1) // batch_size
        
        print(f"\n📤 Starting upload to backend...")
        print(f"   Total clients: {total_clients}")
        print(f"   Batch size: {batch_size}")
        print(f"   Number of batches: {num_batches}")
        print(f"   Company ID: {company_id}")
        
        overall_summary = {
            'new': 0,
            'updated': 0,
            'failed': 0,
            'geocodedDuringSync': 0
        }
        
        for batch_num in range(num_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, total_clients)
            batch = clients_data[start_idx:end_idx]
            
            print(f"\n📦 Batch {batch_num + 1}/{num_batches}: Uploading clients {start_idx + 1}-{end_idx}...")
            
            try:
                # ✅ FIX: Send company_id in the payload AND as header
                payload = {
                    'clients': batch,
                    'companyId': company_id  # ✅ Added this
                }
                
                headers = {
                    'Content-Type': 'application/json',
                    'x-middleware-token': self.middleware_token,
                    'x-company-id': company_id  # ✅ Added this header
                }
                
                response = requests.post(
                    self.sync_endpoint,
                    json=payload,
                    headers=headers,
                    timeout=60
                )
                
                print(f"   Status: {response.status_code}")
                
                if response.status_code == 200:
                    result = response.json()
                    summary = result.get('summary', {})
                    
                    overall_summary['new'] += summary.get('new', 0)
                    overall_summary['updated'] += summary.get('updated', 0)
                    overall_summary['failed'] += summary.get('failed', 0)
                    
                    geocoding_info = result.get('geocoding', {})
                    overall_summary['geocodedDuringSync'] += geocoding_info.get('geocodedDuringSync', 0)
                    
                    print(f"   ✅ Batch {batch_num + 1} success:")
                    print(f"      New: {summary.get('new', 0)}")
                    print(f"      Updated: {summary.get('updated', 0)}")
                    print(f"      Failed: {summary.get('failed', 0)}")
                    
                else:
                    error_msg = response.text
                    try:
                        error_json = response.json()
                        error_msg = json.dumps(error_json)
                    except:
                        pass
                    
                    print(f"   ❌ Batch {batch_num + 1} failed: {error_msg}")
                    raise Exception(f"Backend returned status {response.status_code} for batch {batch_num + 1}: {error_msg}")
                
            except requests.exceptions.Timeout:
                print(f"   ⏱️ Batch {batch_num + 1} timed out")
                overall_summary['failed'] += len(batch)
                raise Exception(f"Upload timeout for batch {batch_num + 1}")
            
            except requests.exceptions.ConnectionError as e:
                print(f"   🔌 Batch {batch_num + 1} connection error: {e}")
                overall_summary['failed'] += len(batch)
                raise Exception(f"Connection error for batch {batch_num + 1}: {str(e)}")
            
            except Exception as e:
                print(f"   ❌ Batch {batch_num + 1} error: {e}")
                overall_summary['failed'] += len(batch)
                raise Exception(f"Upload failed: {str(e)}")
        
        print(f"\n✅ Upload completed!")
        print(f"   New: {overall_summary['new']}")
        print(f"   Updated: {overall_summary['updated']}")
        print(f"   Failed: {overall_summary['failed']}")
        print(f"   Geocoded during sync: {overall_summary['geocodedDuringSync']}")
        
        return {
            'success': True,
            'summary': overall_summary
        }


def prepare_client_for_upload(client_dict):
    """
    Convert client dict to backend format
    
    Args:
        client_dict: Dictionary with client data
    
    Returns:
        dict: Formatted client data for backend
    """
    return {
        'tally_guid': client_dict.get('guid'),
        'name': client_dict.get('name'),
        'email': client_dict.get('email'),
        'phone': client_dict.get('phone'),
        'address': client_dict.get('address'),
        'pincode': client_dict.get('pincode'),
        'latitude': client_dict.get('latitude'),
        'longitude': client_dict.get('longitude'),
        'status': 'active',
        'notes': client_dict.get('notes'),
        'source': 'tally'
    }

@app.route('/api/tally/push-update', methods=['POST'])
def push_update_to_tally():
    """
    Push address / phone / email updates from backend to Tally
    """
    try:
        data = request.get_json(force=True)

        tally_guid = data.get('tallyGuid')
        tally_company_name = data.get('tallyCompanyName')
        username = data.get('username', '')
        password = data.get('password', '')
        operation = data.get('operation')
        update_data = data.get('data')

        if not all([tally_guid, tally_company_name, operation, update_data]):
            return jsonify({
                "success": False,
                "error": "Missing required fields"
            }), 400

        # Optional login block
        login_block = ""
        if username and password:
            login_block = f"""
                <LOGIN>
                    <USERNAME>{username}</USERNAME>
                    <PASSWORD>{password}</PASSWORD>
                </LOGIN>
            """

        # Build update XML
        update_xml = ""

        if operation == "update_address" and "address" in update_data:
            address_lines = update_data["address"].split(",")
            update_xml += "<ADDRESS.LIST>\n"
            for line in address_lines:
                if line.strip():
                    update_xml += f"    <ADDRESS>{line.strip()}</ADDRESS>\n"
            update_xml += "</ADDRESS.LIST>"

        elif operation == "update_phone" and "phone" in update_data:
            update_xml = f"<LEDGERPHONE>{update_data['phone']}</LEDGERPHONE>"

        elif operation == "update_email" and "email" in update_data:
            update_xml = f"<EMAIL>{update_data['email']}</EMAIL>"

        elif operation == "update_multiple":
            if "address" in update_data:
                address_lines = update_data["address"].split(",")
                update_xml += "<ADDRESS.LIST>\n"
                for line in address_lines:
                    if line.strip():
                        update_xml += f"    <ADDRESS>{line.strip()}</ADDRESS>\n"
                update_xml += "</ADDRESS.LIST>\n"

            if "phone" in update_data:
                update_xml += f"<LEDGERPHONE>{update_data['phone']}</LEDGERPHONE>\n"

            if "email" in update_data:
                update_xml += f"<EMAIL>{update_data['email']}</EMAIL>\n"

        else:
            return jsonify({
                "success": False,
                "error": f"Unsupported operation: {operation}"
            }), 400

        # Build final Tally XML
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
                <SVCURRENTCOMPANY>{tally_company_name}</SVCURRENTCOMPANY>
                {login_block}
            </STATICVARIABLES>
        </DESC>
        <DATA>
            <TALLYMESSAGE>
                <LEDGER ACTION="Alter">
                    <GUID>{tally_guid}</GUID>
                    {update_xml}
                </LEDGER>
            </TALLYMESSAGE>
        </DATA>
    </BODY>
</ENVELOPE>
        """.strip()

        response = requests.post(
            TALLY_URL,
            data=xml_payload.encode("utf-8"),
            headers={"Content-Type": "application/xml"},
            timeout=30
        )

        response_text = response.text.lower()

        if "error" in response_text or "failed" in response_text:
            return jsonify({
                "success": False,
                "error": "Tally rejected the update",
                "tallyResponse": response.text[:500]
            }), 400

        if response.status_code == 200:
            return jsonify({
                "success": True,
                "message": "Successfully updated in Tally",
                "tallyResponse": response.text[:300]
            })

        return jsonify({
            "success": False,
            "error": f"HTTP {response.status_code}",
            "tallyResponse": response.text[:500]
        }), 500

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


if __name__ == "__main__":
    app.run(port=5001, debug=True)