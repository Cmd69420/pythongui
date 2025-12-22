import re
import pandas as pd
from lxml import etree

# REMOVED THE HARDCODED FILTER - Now accepts all ledgers
# Filtering will be done in the app based on user selection

# ----------------------------
# Helpers
# ----------------------------

def sanitize_xml(xml: str) -> str:
    return re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", xml)


def _text(elem, tag):
    c = elem.find(tag)
    return c.text.strip() if c is not None and c.text else ""


def _float(elem, tag):
    t = _text(elem, tag)
    try:
        return float(re.sub(r"[^\d.-]", "", t)) if t else 0.0
    except:
        return 0.0


# ----------------------------
# Address Cleaning Logic
# ----------------------------

def clean_address(address: str) -> str:
    if not address:
        return ""

    addr = address.lower().strip()

    # Remove "address :" / "addr :" etc
    addr = re.sub(r"^\s*address\s*[:\-]\s*", "", addr, flags=re.I)

    # Remove leading symbols
    addr = re.sub(r"^[=\-:]+", "", addr).strip()

    # Remove leading name like "Mr Name,"
    addr = re.sub(
        r"^\s*(mr|mrs|ms|dr|shri|smt|miss)\.?\s+[a-z\s\.]+?,\s*",
        "",
        addr,
        flags=re.I
    )

    # Remove everything after pincode
    addr = re.sub(r"(\b\d{6}\b).*", r"\1", addr)

    # Remove phone/email words
    addr = re.sub(
        r"(mobile|cell|phone|email)\s*[:\-].*$",
        "",
        addr,
        flags=re.I
    )

    # Normalize spaces & commas
    addr = re.sub(r"\s+", " ", addr)
    addr = re.sub(r",\s*,", ",", addr)
    addr = addr.strip(" ,")

    return addr.title()


# ----------------------------
# Main Parser
# ----------------------------

def parse_ledgers(xml_text: str) -> pd.DataFrame:
    xml_text = sanitize_xml(xml_text)

    parser = etree.XMLParser(recover=True, huge_tree=True)
    root = etree.fromstring(xml_text.encode(), parser)

    rows = []

    for ledger in root.findall(".//LEDGER"):
        parent = _text(ledger, "PARENT").strip()

        # NO FILTERING HERE - Accept all ledgers
        # The app will filter based on user selection

        name = ledger.get("NAME", "").strip()

        # Address
        parts = []
        addr_list = ledger.find("ADDRESS.LIST")
        if addr_list is not None:
            for a in addr_list.findall("ADDRESS"):
                if a.text:
                    parts.append(a.text.strip())

        raw_address = ", ".join(parts)
        address = clean_address(raw_address)

        # Contact
        phone = (
            _text(ledger, "LEDGERPHONE")
            or _text(ledger, "PHONENUMBER")
            or _text(ledger, "MOBILE")
        )
        phone = re.sub(r"[^\d+]", "", phone)

        email = (
            _text(ledger, "EMAIL")
            or _text(ledger, "LEDGEREMAIL")
        ).lower().strip()

        rows.append({
            "guid": _text(ledger, "GUID"),
            "name": name,
            "parent": parent.title() if parent else "",
            "address": address,
            "phone": phone,
            "email": email,
            "opening_balance": _float(ledger, "OPENINGBALANCE"),
            "closing_balance": _float(ledger, "CLOSINGBALANCE"),
        })

    df = pd.DataFrame(rows)
    return df