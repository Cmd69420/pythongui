import requests
import json
import html
from functools import wraps
from config import MIDDLEWARE_TOKEN
from flask import Flask, request, jsonify

app = Flask(__name__)
TALLY_URL = "http://localhost:9000"

# ✅ ADD TOKEN VALIDATION DECORATOR
def require_middleware_token(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('x-middleware-token')
        if not token or token != MIDDLEWARE_TOKEN:
            return jsonify({
                "success": False,
                "error": "Unauthorized - Invalid middleware token"
            }), 401
        return f(*args, **kwargs)
    return decorated_function

# ✅ ADD XML ESCAPING
def escape_xml(text):
    """Escape XML special characters"""
    if text is None:
        return ""
    return html.escape(str(text))

@app.route('/api/tally/push-update', methods=['POST'])
@require_middleware_token  # ✅ ADD THIS
def push_update_to_tally():
    """
    Push address updates from backend to Tally (ADDRESS ONLY)
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

        # ✅ ONLY SUPPORT ADDRESS UPDATES
        if operation != "update_address" or "address" not in update_data:
            return jsonify({
                "success": False,
                "error": f"Only 'update_address' operation is supported"
            }), 400

        # Optional login block
        login_block = ""
        if username and password:
            login_block = f"""
                <LOGIN>
                    <USERNAME>{escape_xml(username)}</USERNAME>
                    <PASSWORD>{escape_xml(password)}</PASSWORD>
                </LOGIN>
            """

        # ✅ BUILD ADDRESS XML WITH ESCAPING
        address_lines = update_data["address"].split(",")
        update_xml = "<ADDRESS.LIST>\n"
        for line in address_lines:
            if line.strip():
                update_xml += f"    <ADDRESS>{escape_xml(line.strip())}</ADDRESS>\n"
        update_xml += "</ADDRESS.LIST>"

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
                <SVCURRENTCOMPANY>{escape_xml(tally_company_name)}</SVCURRENTCOMPANY>
                {login_block}
            </STATICVARIABLES>
        </DESC>
        <DATA>
            <TALLYMESSAGE>
                <LEDGER ACTION="Alter">
                    <GUID>{escape_xml(tally_guid)}</GUID>
                    {update_xml}
                </LEDGER>
            </TALLYMESSAGE>
        </DATA>
    </BODY>
</ENVELOPE>
        """.strip()

        print(f"\n📤 Sending to Tally:")
        print(f"   GUID: {tally_guid}")
        print(f"   Company: {tally_company_name}")
        print(f"   Address: {update_data['address'][:50]}...")

        response = requests.post(
            TALLY_URL,
            data=xml_payload.encode("utf-8"),
            headers={"Content-Type": "application/xml"},
            timeout=30
        )

        response_text = response.text.lower()

        if "error" in response_text or "failed" in response_text:
            print(f"❌ Tally rejected: {response.text[:200]}")
            return jsonify({
                "success": False,
                "error": "Tally rejected the update",
                "tallyResponse": response.text[:500]
            }), 400

        if response.status_code == 200:
            print(f"✅ Tally accepted update")
            return jsonify({
                "success": True,
                "message": "Successfully updated address in Tally",
                "tallyResponse": response.text[:300]
            })

        return jsonify({
            "success": False,
            "error": f"HTTP {response.status_code}",
            "tallyResponse": response.text[:500]
        }), 500

    except Exception as e:
        print(f"❌ Exception: {e}")
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


if __name__ == "__main__":
    print("🚀 Starting Tally Middleware on port 5001...")
    print(f"🔑 Middleware Token: {MIDDLEWARE_TOKEN}")
    app.run(port=5001, debug=True)