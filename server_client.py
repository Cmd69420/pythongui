import requests

def server_health_check(url: str) -> bool:
    try:
        r = requests.get(url, timeout=5)
        return r.status_code == 200
    except:
        return False
