import requests
import json
from config import MIDDLEWARE_TOKEN

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