import requests
from lxml import etree

TALLY_URL = "http://localhost:9000"


class TallyClient:
    def __init__(self, url: str = TALLY_URL):
        self.url = url
        self.session = requests.Session()

    # --------------------------------------------------
    # CONNECTION TEST
    # --------------------------------------------------
    def test_connection(self) -> bool:
        xml = """
        <ENVELOPE>
            <HEADER>
                <VERSION>1</VERSION>
                <TALLYREQUEST>Export</TALLYREQUEST>
                <TYPE>Data</TYPE>
                <ID>TallyVersion</ID>
            </HEADER>
        </ENVELOPE>
        """
        try:
            r = self.session.post(
                self.url,
                data=xml.encode("utf-8"),
                headers={"Content-Type": "application/xml"},
                timeout=5
            )
            return r.status_code == 200
        except:
            return False

    # --------------------------------------------------
    # FETCH COMPANIES
    # --------------------------------------------------
    def fetch_companies(self):
        xml = """
        <ENVELOPE>
            <HEADER>
                <VERSION>1</VERSION>
                <TALLYREQUEST>Export</TALLYREQUEST>
                <TYPE>Collection</TYPE>
                <ID>List of Companies</ID>
            </HEADER>
            <BODY>
                <DESC>
                    <TDL>
                        <TDLMESSAGE>
                            <COLLECTION NAME="Companies">
                                <TYPE>Company</TYPE>
                                <FETCH>Name</FETCH>
                            </COLLECTION>
                        </TDLMESSAGE>
                    </TDL>
                </DESC>
            </BODY>
        </ENVELOPE>
        """

        r = self.session.post(
            self.url,
            data=xml.encode("utf-8"),
            headers={"Content-Type": "application/xml"},
            timeout=10
        )
        r.raise_for_status()

        root = etree.fromstring(r.content)

        return [
            c.find("NAME").text.strip()
            for c in root.xpath("//COMPANY")
            if c.find("NAME") is not None
        ]
    

    def get_companies(self):
        return self.fetch_companies()

    # --------------------------------------------------
    # CHECK IF COMPANY REQUIRES AUTHENTICATION
    # --------------------------------------------------
    def check_company_security(self, company_name: str) -> bool:
        """Check if a company requires username/password. Returns True if secured."""
        
        xml = f"""<ENVELOPE>
        <HEADER>
            <VERSION>1</VERSION>
            <TALLYREQUEST>Export</TALLYREQUEST>
            <TYPE>Collection</TYPE>
            <ID>Security Check</ID>
        </HEADER>
        <BODY>
            <DESC>
                <STATICVARIABLES>
                    <SVEXPORTFORMAT>$SysName:XML</SVEXPORTFORMAT>
                    <SVCURRENTCOMPANY>{company_name}</SVCURRENTCOMPANY>
                </STATICVARIABLES>
                <TDL>
                    <TDLMESSAGE>
                        <COLLECTION NAME="Security Check">
                            <TYPE>Company</TYPE>
                            <FETCH>NAME</FETCH>
                        </COLLECTION>
                    </TDLMESSAGE>
                </TDL>
            </DESC>
        </BODY>
    </ENVELOPE>"""

        try:
            r = self.session.post(
                self.url,
                data=xml.encode("utf-8"),
                headers={"Content-Type": "application/xml"},
                timeout=10
            )
            
            # Check response for security/authentication errors
            response_text = r.text.lower()
            
            # If response contains security/authentication keywords, company is secured
            if any(keyword in response_text for keyword in [
                'security', 'authentication', 'password', 'login', 'unauthorised', 'unauthorized'
            ]):
                return True
            
            # If we got a successful response, company is not secured
            if r.status_code == 200 and '<company' in response_text:
                return False
                
            return False
            
        except:
            # If request fails, assume not secured
            return False

    # --------------------------------------------------
    # FETCH ALL GROUPS (for filtering)
    # --------------------------------------------------
    def fetch_groups(self, company_name: str, username: str, password: str) -> list:
        """Fetch all group names from Tally"""
        
        login_block = f"""
            <LOGIN>
                <USERNAME>{username}</USERNAME>
                <PASSWORD>{password}</PASSWORD>
            </LOGIN>
        """

        xml = f"""<ENVELOPE>
        <HEADER>
            <VERSION>1</VERSION>
            <TALLYREQUEST>Export</TALLYREQUEST>
            <TYPE>Collection</TYPE>
            <ID>All Groups</ID>
        </HEADER>
        <BODY>
            <DESC>
                <STATICVARIABLES>
                    <SVEXPORTFORMAT>$SysName:XML</SVEXPORTFORMAT>
                    <SVCURRENTCOMPANY>{company_name}</SVCURRENTCOMPANY>
                    {login_block}
                </STATICVARIABLES>
                <TDL>
                    <TDLMESSAGE>
                        <COLLECTION NAME="All Groups">
                            <TYPE>Group</TYPE>
                            <FETCH>NAME</FETCH>
                        </COLLECTION>
                    </TDLMESSAGE>
                </TDL>
            </DESC>
        </BODY>
    </ENVELOPE>"""

        r = self.session.post(
            self.url,
            data=xml.encode("utf-8"),
            headers={"Content-Type": "application/xml"},
            timeout=30
        )

        if r.status_code != 200:
            raise Exception("Failed to fetch groups from Tally")

        root = etree.fromstring(r.content)
        groups = [g.get("NAME", "").strip() for g in root.findall(".//GROUP") if g.get("NAME")]
        
        return sorted(groups)

    # --------------------------------------------------
    # FETCH LEDGERS (Original method - credentials optional)
    # --------------------------------------------------
    def fetch_ledgers(self, company_name: str, username: str = "", password: str = "") -> str:
        # Only add login block if credentials are provided
        login_block = ""
        if username and password:
            login_block = f"""
            <LOGIN>
                <USERNAME>{username}</USERNAME>
                <PASSWORD>{password}</PASSWORD>
            </LOGIN>
            """

        xml = f"""<ENVELOPE>
        <HEADER>
            <VERSION>1</VERSION>
            <TALLYREQUEST>Export</TALLYREQUEST>
            <TYPE>Collection</TYPE>
            <ID>Ledger Dump</ID>
        </HEADER>
        <BODY>
            <DESC>
                <STATICVARIABLES>
                    <SVEXPORTFORMAT>$SysName:XML</SVEXPORTFORMAT>
                    <SVCURRENTCOMPANY>{company_name}</SVCURRENTCOMPANY>
                    {login_block}
                </STATICVARIABLES>
                <TDL>
                    <TDLMESSAGE>
                        <COLLECTION NAME="Ledger Dump">
                            <TYPE>Ledger</TYPE>
                            <FETCH>
                                NAME,
                                GUID,
                                PARENT,
                                ADDRESS.LIST,
                                PINCODE,
                                STATENAME,
                                COUNTRYNAME,
                                MOBILE,
                                LEDGERPHONE,
                                PHONENUMBER,
                                EMAIL,
                                LEDGEREMAIL,
                                OPENINGBALANCE,
                                CLOSINGBALANCE
                            </FETCH>
                        </COLLECTION>
                    </TDLMESSAGE>
                </TDL>
            </DESC>
        </BODY>
    </ENVELOPE>"""

        r = self.session.post(
            self.url,
            data=xml.encode("utf-8"),
            headers={"Content-Type": "application/xml"},
            timeout=180
        )

        if r.status_code != 200:
            raise Exception("Failed to fetch ledgers from Tally")

        return r.text

    # --------------------------------------------------
    # FETCH LEDGERS WITH FILTER (FIXED)
    # --------------------------------------------------
    def fetch_ledgers_filtered(self, company_name: str, username: str, password: str, parent_groups: list) -> str:
        """Fetch all ledgers and filter in parser - Tally TDL filters are complex"""
        
        # Just fetch all ledgers - we'll filter in the parser
        # This is more reliable than TDL filters which have complex syntax
        return self.fetch_ledgers(company_name, username, password)

    # --------------------------------------------------
    # FETCH GENERIC MASTERS (credentials optional)
    # --------------------------------------------------
    def fetch_masters(self, company_name: str, username: str = "", password: str = "", master_type: str = "") -> str:
        """Fetch any master type (Group, Stock Item, etc.)"""
        
        # Only add login block if credentials are provided
        login_block = ""
        if username and password:
            login_block = f"""
            <LOGIN>
                <USERNAME>{username}</USERNAME>
                <PASSWORD>{password}</PASSWORD>
            </LOGIN>
            """

        # Map master type to Tally collection type
        type_mapping = {
            "Group": "Group",
            "Stock Item": "StockItem",
            "Stock Group": "StockGroup",
            "Stock Category": "StockCategory",
            "Unit": "Unit",
            "Godown": "Godown",
            "Cost Centre": "CostCentre",
            "Voucher Type": "VoucherType"
        }

        tally_type = type_mapping.get(master_type, master_type.replace(" ", ""))

        xml = f"""<ENVELOPE>
        <HEADER>
            <VERSION>1</VERSION>
            <TALLYREQUEST>Export</TALLYREQUEST>
            <TYPE>Collection</TYPE>
            <ID>{master_type} Collection</ID>
        </HEADER>
        <BODY>
            <DESC>
                <STATICVARIABLES>
                    <SVEXPORTFORMAT>$SysName:XML</SVEXPORTFORMAT>
                    <SVCURRENTCOMPANY>{company_name}</SVCURRENTCOMPANY>
                    {login_block}
                </STATICVARIABLES>
                <TDL>
                    <TDLMESSAGE>
                        <COLLECTION NAME="{master_type} Collection">
                            <TYPE>{tally_type}</TYPE>
                            <FETCH>NAME, GUID, PARENT</FETCH>
                        </COLLECTION>
                    </TDLMESSAGE>
                </TDL>
            </DESC>
        </BODY>
    </ENVELOPE>"""

        r = self.session.post(
            self.url,
            data=xml.encode("utf-8"),
            headers={"Content-Type": "application/xml"},
            timeout=180
        )

        if r.status_code != 200:
            raise Exception(f"Failed to fetch {master_type} from Tally")

        return r.text