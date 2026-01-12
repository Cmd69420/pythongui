import requests
from lxml import etree

TALLY_URL = "http://localhost:9000"
COMPANY_NAME = "Rajlaxmi Solutions Private Limited - (From 1-Apr-2016)"
LEDGER_GUID = "44acb1ff-18e7-40de-822e-c7f32ad504e4-00003960"

def fetch_ledger_by_guid(guid, company_name):
    """Fetch a specific ledger by GUID from Tally"""
    
    xml = f"""<ENVELOPE>
    <HEADER>
        <VERSION>1</VERSION>
        <TALLYREQUEST>Export</TALLYREQUEST>
        <TYPE>Data</TYPE>
        <ID>Ledger Details</ID>
    </HEADER>
    <BODY>
        <DESC>
            <STATICVARIABLES>
                <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
                <SVCURRENTCOMPANY>{company_name}</SVCURRENTCOMPANY>
                <GUID>{guid}</GUID>
            </STATICVARIABLES>
            <TDL>
                <TDLMESSAGE>
                    <REPORT NAME="Ledger Details">
                        <FORMS>Ledger Details</FORMS>
                    </REPORT>
                    <FORM NAME="Ledger Details">
                        <PARTS>Ledger Details</PARTS>
                    </FORM>
                    <PART NAME="Ledger Details">
                        <LINES>Ledger Details</LINES>
                    </PART>
                    <LINE NAME="Ledger Details">
                        <FIELDS>Name, GUID, Address</FIELDS>
                    </LINE>
                    <FIELD NAME="Name">
                        <SET>$Name</SET>
                    </FIELD>
                    <FIELD NAME="GUID">
                        <SET>$GUID</SET>
                    </FIELD>
                    <FIELD NAME="Address">
                        <SET>$$String:$Address</SET>
                    </FIELD>
                </TDLMESSAGE>
            </TDL>
        </DESC>
    </BODY>
</ENVELOPE>"""

    try:
        response = requests.post(
            TALLY_URL,
            data=xml.encode("utf-8"),
            headers={"Content-Type": "application/xml"},
            timeout=10
        )
        
        if response.status_code == 200:
            return response.text
        else:
            print(f"❌ Failed to fetch ledger: Status {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def fetch_ledger_simple(company_name, ledger_name):
    """Fetch ledger using simple collection export"""
    
    xml = f"""<ENVELOPE>
    <HEADER>
        <VERSION>1</VERSION>
        <TALLYREQUEST>Export</TALLYREQUEST>
        <TYPE>Collection</TYPE>
        <ID>Ledger Details</ID>
    </HEADER>
    <BODY>
        <DESC>
            <STATICVARIABLES>
                <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
                <SVCURRENTCOMPANY>{company_name}</SVCURRENTCOMPANY>
            </STATICVARIABLES>
            <TDL>
                <TDLMESSAGE>
                    <COLLECTION NAME="Ledger Details">
                        <TYPE>Ledger</TYPE>
                        <FETCH>NAME, GUID, ADDRESS.LIST, PARENT</FETCH>
                        <FILTER>LedgerFilter</FILTER>
                    </COLLECTION>
                    <SYSTEM TYPE="Formulae" NAME="LedgerFilter">$$IsEqual:$Name:"{ledger_name}"</SYSTEM>
                </TDLMESSAGE>
            </TDL>
        </DESC>
    </BODY>
</ENVELOPE>"""

    try:
        response = requests.post(
            TALLY_URL,
            data=xml.encode("utf-8"),
            headers={"Content-Type": "application/xml"},
            timeout=10
        )
        
        if response.status_code == 200:
            return response.text
        else:
            print(f"❌ Failed to fetch ledger: Status {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def parse_ledger_response(xml_text):
    """Parse the ledger XML response"""
    try:
        root = etree.fromstring(xml_text.encode())
        
        # Find ledger
        ledger = root.find(".//LEDGER")
        if ledger is None:
            print("❌ No ledger found in response")
            return None
        
        name = ledger.get("NAME", "")
        guid = ledger.findtext("GUID", "")
        
        # Get address
        address_list = ledger.find("ADDRESS.LIST")
        address_lines = []
        if address_list is not None:
            for addr in address_list.findall("ADDRESS"):
                if addr.text:
                    address_lines.append(addr.text.strip())
        
        return {
            "name": name,
            "guid": guid,
            "address": ", ".join(address_lines)
        }
        
    except Exception as e:
        print(f"❌ Error parsing: {e}")
        return None


if __name__ == "__main__":
    print("="*70)
    print("🔍 TALLY LEDGER VERIFICATION")
    print("="*70)
    print(f"\nCompany: {COMPANY_NAME}")
    print(f"GUID: {LEDGER_GUID}")
    
    print("\n📡 Fetching ledger from Tally...")
    
    # Try simple method
    xml_response = fetch_ledger_simple(
        COMPANY_NAME, 
        "Mitsubishi Heavy Industries Srk 18 Cs-S6 Split Ac (1.5"
    )
    
    if xml_response:
        print("\n✅ Received response from Tally")
        print(f"   Response length: {len(xml_response)} bytes")
        
        # Save raw response
        with open("tally_ledger_response.xml", "w", encoding="utf-8") as f:
            f.write(xml_response)
        print("   Saved to: tally_ledger_response.xml")
        
        # Parse and display
        ledger_data = parse_ledger_response(xml_response)
        
        if ledger_data:
            print("\n📋 LEDGER DETAILS:")
            print("="*70)
            print(f"Name: {ledger_data['name']}")
            print(f"GUID: {ledger_data['guid']}")
            print(f"\nCurrent Address in Tally:")
            print(f"{ledger_data['address']}")
            print("="*70)
            
            # Check if it matches expected
            expected = "Road no 22, Wagle Industrial Estate, Thane West, Thane, Maharashtra 400604, India"
            if expected in ledger_data['address']:
                print("\n✅ ADDRESS UPDATED SUCCESSFULLY!")
            else:
                print("\n⚠️ Address doesn't match expected value")
                print(f"\nExpected: {expected}")
        else:
            print("\n❌ Could not parse ledger data")
            print("\nRaw XML Response:")
            print(xml_response)
    else:
        print("\n❌ Failed to fetch ledger from Tally")
        print("\nPossible reasons:")
        print("  1. Tally is not running")
        print("  2. Wrong company name")
        print("  3. Ledger doesn't exist")
        print("  4. Tally port is not 9000")