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
    

    def get_companies(self):          # ✅ ADD HERE
        return self.fetch_companies()

    # --------------------------------------------------
    # FETCH LEDGERS
    # --------------------------------------------------
    def fetch_ledgers(self, company_name: str, username: str, password: str) -> str:
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
                    <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
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
