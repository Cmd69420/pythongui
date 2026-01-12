import requests
from datetime import datetime

# Configuration
TALLY_URL = "http://localhost:9000"
COMPANY_NAME = "Rajlaxmi Solutions Private Limited - (From 1-Apr-2016)"

def test_tally_import_capability():
    """
    Test if Tally allows XML imports
    Creates a simple test ledger to verify import capability
    """
    
    print("="*70)
    print("🧪 TALLY IMPORT CAPABILITY TEST")
    print("="*70)
    print(f"Tally URL: {TALLY_URL}")
    print(f"Company: {COMPANY_NAME}")
    print(f"Test Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Test 1: Check if Tally is running
    print("\n📡 Test 1: Checking if Tally is accessible...")
    try:
        test_xml = """
<ENVELOPE>
    <HEADER>
        <VERSION>1</VERSION>
        <TALLYREQUEST>Export Data</TALLYREQUEST>
    </HEADER>
    <BODY>
        <DESC>
            <STATICVARIABLES>
                <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
            </STATICVARIABLES>
        </DESC>
    </BODY>
</ENVELOPE>
        """.strip()
        
        response = requests.post(
            TALLY_URL,
            data=test_xml.encode("utf-8"),
            headers={"Content-Type": "application/xml"},
            timeout=5
        )
        
        if response.status_code == 200:
            print("   ✅ Tally is running and responding")
        else:
            print(f"   ⚠️  Unexpected status: {response.status_code}")
            
    except Exception as e:
        print(f"   ❌ Cannot connect to Tally: {e}")
        return
    
    # Test 2: Try to create a simple test ledger
    print("\n📝 Test 2: Attempting to CREATE a test ledger...")
    print("   Creating ledger: '__IMPORT_TEST_LEDGER__'")
    
    test_ledger_name = f"__IMPORT_TEST_{datetime.now().strftime('%H%M%S')}__"
    
    create_xml = f"""
<ENVELOPE>
    <HEADER>
        <VERSION>1</VERSION>
        <TALLYREQUEST>Import Data</TALLYREQUEST>
        <TYPE>Data</TYPE>
        <ID>Test Import</ID>
    </HEADER>
    <BODY>
        <DESC>
            <STATICVARIABLES>
                <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
                <SVCURRENTCOMPANY>{COMPANY_NAME}</SVCURRENTCOMPANY>
            </STATICVARIABLES>
        </DESC>
        <DATA>
            <TALLYMESSAGE xmlns:UDF="TallyUDF">
                <LEDGER NAME="{test_ledger_name}" ACTION="Create">
                    <NAME>{test_ledger_name}</NAME>
                    <PARENT>Sundry Debtors</PARENT>
                    <ISBILLWISEON>No</ISBILLWISEON>
                    <ISCOSTCENTRESON>No</ISCOSTCENTRESON>
                </LEDGER>
            </TALLYMESSAGE>
        </DATA>
    </BODY>
</ENVELOPE>
    """.strip()
    
    print("\n   📤 Sending CREATE request...")
    print("   " + "-"*66)
    for line in create_xml.split('\n')[:15]:
        print(f"   {line}")
    print("   ...")
    print("   " + "-"*66)
    
    try:
        response = requests.post(
            TALLY_URL,
            data=create_xml.encode("utf-8"),
            headers={"Content-Type": "application/xml"},
            timeout=10
        )
        
        print(f"\n   📨 Response Status: {response.status_code}")
        print("   " + "-"*66)
        for line in response.text.split('\n'):
            print(f"   {line}")
        print("   " + "-"*66)
        
        # Parse response
        response_lower = response.text.lower()
        
        if "<status>1</status>" in response_lower:
            print("\n   ✅ SUCCESS! Ledger was created")
            print("   ✅ Tally ALLOWS imports - Educational Mode is NOT active")
            print("   💡 Your import code should work!")
            created_ledger = test_ledger_name
        elif "<status>0</status>" in response_lower:
            print("\n   ❌ FAILED! Tally rejected the import")
            print("   ❌ Educational Mode is BLOCKING imports")
            print("   💡 You need a licensed Tally version")
            created_ledger = None
        else:
            print("\n   ⚠️  Unexpected response format")
            created_ledger = None
            
    except Exception as e:
        print(f"\n   ❌ Error during import: {e}")
        import traceback
        traceback.print_exc()
        created_ledger = None
    
    # Test 3: If ledger was created, try to ALTER it
    if created_ledger:
        print(f"\n📝 Test 3: Attempting to ALTER the test ledger...")
        print(f"   Changing address of: {created_ledger}")
        
        alter_xml = f"""
<ENVELOPE>
    <HEADER>
        <VERSION>1</VERSION>
        <TALLYREQUEST>Import Data</TALLYREQUEST>
        <TYPE>Data</TYPE>
        <ID>Test Alter</ID>
    </HEADER>
    <BODY>
        <DESC>
            <STATICVARIABLES>
                <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
                <SVCURRENTCOMPANY>{COMPANY_NAME}</SVCURRENTCOMPANY>
            </STATICVARIABLES>
        </DESC>
        <DATA>
            <TALLYMESSAGE xmlns:UDF="TallyUDF">
                <LEDGER NAME="{created_ledger}" ACTION="Alter">
                    <NAME>{created_ledger}</NAME>
                    <ADDRESS.LIST TYPE="String">
                        <ADDRESS TYPE="String">Test Address Line 1</ADDRESS>
                        <ADDRESS TYPE="String">Test Address Line 2</ADDRESS>
                        <ADDRESS TYPE="String">Test City 400001</ADDRESS>
                    </ADDRESS.LIST>
                </LEDGER>
            </TALLYMESSAGE>
        </DATA>
    </BODY>
</ENVELOPE>
        """.strip()
        
        print("\n   📤 Sending ALTER request...")
        
        try:
            response = requests.post(
                TALLY_URL,
                data=alter_xml.encode("utf-8"),
                headers={"Content-Type": "application/xml"},
                timeout=10
            )
            
            print(f"\n   📨 Response Status: {response.status_code}")
            print("   " + "-"*66)
            for line in response.text.split('\n'):
                print(f"   {line}")
            print("   " + "-"*66)
            
            response_lower = response.text.lower()
            
            if "<status>1</status>" in response_lower:
                print("\n   ✅ SUCCESS! Address was updated")
                print("   ✅ ALTER operations work!")
            elif "<status>0</status>" in response_lower:
                print("\n   ❌ FAILED! Tally rejected the ALTER")
                print("   ⚠️  This is unexpected - CREATE worked but ALTER failed")
            else:
                print("\n   ⚠️  Unexpected response")
                
        except Exception as e:
            print(f"\n   ❌ Error during alter: {e}")
    
    else:
        print("\n⏭️  Test 3: SKIPPED (CREATE failed)")
    
    # Final Summary
    print("\n" + "="*70)
    print("📊 TEST SUMMARY")
    print("="*70)
    
    if created_ledger:
        print("✅ Tally ALLOWS imports - NOT in Educational Mode")
        print("✅ Your XML import code should work")
        print(f"✅ Test ledger created: {created_ledger}")
        print("\n💡 If your actual ledger updates are failing, the issue is:")
        print("   - Wrong GUID")
        print("   - Wrong ledger name")
        print("   - Wrong company name")
        print("   - Ledger is locked/in use")
    else:
        print("❌ Tally is BLOCKING imports - Educational Mode detected")
        print("❌ You need a licensed Tally version for XML imports")
        print("\n💡 Solutions:")
        print("   1. Purchase Tally Silver/Gold license")
        print("   2. Get Tally Developer license for testing")
        print("   3. Use manual entry as temporary workaround")
    
    print("="*70)


if __name__ == "__main__":
    test_tally_import_capability()