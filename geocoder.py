import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import re

GOOGLE_API_KEY = "AIzaSyCwGkrq4Onpvj9Yu5His9row-fIg5v6N0I"
GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
PLACES_NEARBY_URL = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
PLACES_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
PLACE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"


def extract_pincode_from_components(address_components: list) -> str:
    """Extract pincode from Google's address_components"""
    for component in address_components:
        if 'postal_code' in component.get('types', []):
            return component.get('long_name', '')
    return None


def extract_pincode_from_address(address: str) -> str:
    """Extract 6-digit pincode from address string (fallback)"""
    match = re.search(r'\b(\d{6})\b', address)
    return match.group(1) if match else None


def extract_address_components(address: str) -> dict:
    """Extract city, state, pincode from address string"""
    components = {
        'city': None,
        'state': None,
        'pincode': None,
        'country': 'India'
    }
    
    # Extract pincode (6 digits in India)
    pincode_match = re.search(r'\b(\d{6})\b', address)
    if pincode_match:
        components['pincode'] = pincode_match.group(1)
    
    # Common Indian states
    states = ['Maharashtra', 'Gujarat', 'Karnataka', 'Tamil Nadu', 'Delhi', 
              'Uttar Pradesh', 'Rajasthan', 'West Bengal', 'Kerala', 'Punjab',
              'Madhya Pradesh', 'Andhra Pradesh', 'Telangana', 'Bihar', 'Odisha']
    
    address_lower = address.lower()
    for state in states:
        if state.lower() in address_lower:
            components['state'] = state
            break
    
    return components


def geocode_address(address: str):
    """
    Standard geocoding for addresses
    Returns: (formatted_address, lat, lng, pincode)
    """
    if not address or address.lower() == "nan" or not address.strip():
        return None, None, None, None
    
    try:
        params = {
            "address": address,
            "key": GOOGLE_API_KEY,
            "region": "in"  # Bias towards India
        }
        r = requests.get(GEOCODE_URL, params=params, timeout=10)
        data = r.json()

        if data["status"] == "OK" and len(data["results"]) > 0:
            result = data["results"][0]
            loc = result["geometry"]["location"]
            formatted_addr = result.get("formatted_address", address)
            
            # ✅ EXTRACT PINCODE from address_components
            pincode = extract_pincode_from_components(result.get("address_components", []))
            
            # Fallback: Extract from formatted address
            if not pincode:
                pincode = extract_pincode_from_address(formatted_addr)
            
            return formatted_addr, loc["lat"], loc["lng"], pincode
        else:
            print(f"  ⚠ Geocoding failed with status: {data.get('status', 'UNKNOWN')}")
            return None, None, None, None
    
    except Exception as e:
        print(f"  ✗ Error geocoding address: {e}")
        return None, None, None, None


def search_business_nearby(business_name: str, lat: float, lng: float, radius: int = 500):
    """Search for business within radius of coordinates"""
    try:
        params = {
            "location": f"{lat},{lng}",
            "radius": radius,
            "keyword": business_name,
            "key": GOOGLE_API_KEY
        }
        
        r = requests.get(PLACES_NEARBY_URL, params=params, timeout=10)
        data = r.json()
        
        if data["status"] == "OK" and len(data["results"]) > 0:
            place = data["results"][0]
            
            # Verify name similarity
            place_name = place.get("name", "").lower()
            search_name = business_name.lower()
            
            if any(word in place_name for word in search_name.split()) or \
               any(word in search_name for word in place_name.split()):
                
                place_id = place.get("place_id")
                if place_id:
                    return get_place_details(place_id)
        
        return None, None, None, None, None
    
    except Exception as e:
        print(f"  ✗ Error searching nearby: {e}")
        return None, None, None, None, None


def get_place_details(place_id: str):
    """
    Get detailed place information
    Returns: (name, address, lat, lng, pincode)
    """
    try:
        params = {
            "place_id": place_id,
            "fields": "name,formatted_address,geometry,rating,types,address_components",
            "key": GOOGLE_API_KEY
        }
        
        r = requests.get(PLACE_DETAILS_URL, params=params, timeout=10)
        data = r.json()
        
        if data["status"] == "OK":
            result = data["result"]
            name = result.get("name", "")
            address = result.get("formatted_address", "")
            geometry = result.get("geometry", {}).get("location", {})
            
            lat = geometry.get("lat")
            lng = geometry.get("lng")
            
            # ✅ EXTRACT PINCODE
            pincode = extract_pincode_from_components(result.get("address_components", []))
            if not pincode:
                pincode = extract_pincode_from_address(address)
            
            return name, address, lat, lng, pincode
        
        return None, None, None, None, None
    
    except Exception as e:
        print(f"  ✗ Error getting place details: {e}")
        return None, None, None, None, None


def search_business_with_context(business_name: str, address: str):
    """
    Context-aware text search using address components
    Returns: (address, lat, lng, pincode)
    """
    try:
        components = extract_address_components(address)
        
        # Build search query
        search_parts = [business_name]
        if components['pincode']:
            search_parts.append(components['pincode'])
        if components['city']:
            search_parts.append(components['city'])
        if components['state']:
            search_parts.append(components['state'])
        
        search_query = " ".join(search_parts)
        
        params = {
            "input": search_query,
            "inputtype": "textquery",
            "fields": "place_id,name,formatted_address,geometry,types",
            "key": GOOGLE_API_KEY
        }
        
        r = requests.get(PLACES_SEARCH_URL, params=params, timeout=10)
        data = r.json()
        
        if data["status"] == "OK" and len(data["candidates"]) > 0:
            candidate = data["candidates"][0]
            
            # Validate it's a business
            place_types = candidate.get("types", [])
            business_types = ["point_of_interest", "establishment", "store", 
                            "business", "finance", "bank", "restaurant", "shop"]
            
            if not any(t in place_types for t in business_types):
                return None, None, None, None
            
            # Validate pincode match
            result_address = candidate.get("formatted_address", "").lower()
            if components['pincode'] and components['pincode'] not in result_address:
                return None, None, None, None
            
            address = candidate.get("formatted_address", "")
            geometry = candidate.get("geometry", {}).get("location", {})
            lat = geometry.get("lat")
            lng = geometry.get("lng")
            
            # ✅ EXTRACT PINCODE
            pincode = extract_pincode_from_address(address)
            
            if address and lat and lng:
                return address, lat, lng, pincode
        
        return None, None, None, None
    
    except Exception as e:
        print(f"  ✗ Error in context search: {e}")
        return None, None, None, None


def is_likely_business_name(name: str) -> bool:
    """Determine if name is likely a business"""
    name_lower = name.lower().strip()
    
    if len(name_lower) < 3:
        return False
    
    personal_titles = ['mr.', 'mrs.', 'ms.', 'miss', 'dr.', 'prof.',
                      'shri', 'smt.', 'kumari', 'master', 'baby']
    
    for title in personal_titles:
        if name_lower.startswith(title):
            return False
    
    business_keywords = [
        'pvt', 'ltd', 'limited', 'llp', 'inc', 'corp', 'corporation',
        'company', 'co.', 'enterprises', 'industries', 'traders',
        'associates', 'partners', 'solutions', 'services', 'systems',
        'technologies', 'tech', 'software', 'motors', 'auto',
        'construction', 'builders', 'developers', 'consultants',
        'agency', 'agencies', 'store', 'shop', 'mart', 'mall',
        'hospital', 'clinic', 'pharmacy', 'medical', 'healthcare',
        'hotel', 'restaurant', 'cafe', 'foods', 'caterers',
        'bank', 'finance', 'insurance', 'exports', 'imports',
        'textile', 'fabrics', 'garments', 'apparels',
        'steel', 'metal', 'engineering', 'manufacturing',
        'group', 'holding', 'trust', 'foundation', 'institute',
        '&', ' and ', ' n '
    ]
    
    for keyword in business_keywords:
        if keyword in name_lower:
            return True
    
    words = name_lower.split()
    if len(words) <= 3 and not any(kw in name_lower for kw in business_keywords):
        if all(len(w) <= 10 for w in words):
            return False
    
    if len(words) > 3:
        return True
    
    return False


# ========== THREE PROCESSING MODES ==========

def process_single_row_basic(row, address_col="address"):
    """
    BASIC MODE: Only geocode the address from Tally (original method)
    Returns: dict with address, latitude, longitude, pincode, location_source
    """
    original_address = str(row.get(address_col, "")).strip()
    
    result = {
        "address": original_address,
        "latitude": None,
        "longitude": None,
        "pincode": None,  # ✅ ADDED
        "location_source": "not_found"
    }
    
    # Skip empty addresses
    if not original_address or original_address.lower() == "nan":
        return result
    
    print(f"[BASIC] Geocoding: {original_address[:50]}...")
    geocoded_addr, geo_lat, geo_lng, geo_pincode = geocode_address(original_address)
    
    if geocoded_addr and geo_lat and geo_lng:
        result["address"] = geocoded_addr
        result["latitude"] = geo_lat
        result["longitude"] = geo_lng
        result["pincode"] = geo_pincode  # ✅ ADDED
        result["location_source"] = "geocoded"
        print(f"  ✓ Success: {geocoded_addr[:50]}... (Pincode: {geo_pincode or 'N/A'})")
        return result
    
    print(f"  ✗ Failed to geocode")
    return result


def process_single_row_enhanced(row, address_col="address", name_col="name"):
    """
    ENHANCED MODE: Google Places search first, then fallback to address geocoding
    Returns: dict with address, latitude, longitude, pincode, location_source, match_confidence
    """
    business_name = str(row.get(name_col, "")).strip()
    original_address = str(row.get(address_col, "")).strip()
    
    result = {
        "address": original_address,
        "latitude": None,
        "longitude": None,
        "pincode": None,  # ✅ ADDED
        "location_source": "not_found",
        "match_confidence": "none"
    }
    
    # Skip empty
    if not business_name or business_name.lower() == "nan":
        return result
    
    # Skip personal names
    if not is_likely_business_name(business_name):
        print(f"[ENHANCED] Skipping personal name: {business_name}")
        
        # Still geocode the address
        if original_address and original_address.lower() != "nan":
            geocoded_addr, geo_lat, geo_lng, geo_pincode = geocode_address(original_address)
            if geocoded_addr and geo_lat and geo_lng:
                result["address"] = geocoded_addr
                result["latitude"] = geo_lat
                result["longitude"] = geo_lng
                result["pincode"] = geo_pincode  # ✅ ADDED
                result["location_source"] = "geocoded"
                result["match_confidence"] = "address_only"
        
        return result
    
    print(f"\n[ENHANCED] Processing: {business_name}")
    
    # STRATEGY 1: Geocode address to get approximate location
    if original_address and original_address.lower() != "nan":
        print(f"  Step 1: Geocoding address...")
        geocoded_addr, geo_lat, geo_lng, geo_pincode = geocode_address(original_address)
        
        if geocoded_addr and geo_lat and geo_lng:
            # STRATEGY 2: Search for business nearby (500m)
            print(f"  Step 2: Searching nearby (500m)...")
            place_name, place_addr, place_lat, place_lng, place_pincode = search_business_nearby(
                business_name, geo_lat, geo_lng, radius=500
            )
            
            if place_addr and place_lat and place_lng:
                result["address"] = place_addr
                result["latitude"] = place_lat
                result["longitude"] = place_lng
                result["pincode"] = place_pincode  # ✅ ADDED
                result["location_source"] = "google_places_nearby"
                result["match_confidence"] = "high"
                print(f"  ✓ Found nearby: {place_name} (Pincode: {place_pincode or 'N/A'})")
                return result
            
            # STRATEGY 3: Wider search (2km)
            print(f"  Step 3: Expanding to 2km...")
            place_name, place_addr, place_lat, place_lng, place_pincode = search_business_nearby(
                business_name, geo_lat, geo_lng, radius=2000
            )
            
            if place_addr and place_lat and place_lng:
                result["address"] = place_addr
                result["latitude"] = place_lat
                result["longitude"] = place_lng
                result["pincode"] = place_pincode  # ✅ ADDED
                result["location_source"] = "google_places_nearby"
                result["match_confidence"] = "medium"
                print(f"  ✓ Found nearby (2km): {place_name} (Pincode: {place_pincode or 'N/A'})")
                return result
    
    # STRATEGY 4: Context-aware text search
    if original_address and original_address.lower() != "nan":
        print(f"  Step 4: Context search...")
        place_addr, place_lat, place_lng, place_pincode = search_business_with_context(
            business_name, original_address
        )
        
        if place_addr and place_lat and place_lng:
            result["address"] = place_addr
            result["latitude"] = place_lat
            result["longitude"] = place_lng
            result["pincode"] = place_pincode  # ✅ ADDED
            result["location_source"] = "google_places_context"
            result["match_confidence"] = "medium"
            print(f"  ✓ Found via context (Pincode: {place_pincode or 'N/A'})")
            return result
    
    # STRATEGY 5: Fallback to address geocoding
    if original_address and original_address.lower() != "nan":
        print(f"  Step 5: Fallback to address...")
        geocoded_addr, geo_lat, geo_lng, geo_pincode = geocode_address(original_address)
        
        if geocoded_addr and geo_lat and geo_lng:
            result["address"] = geocoded_addr
            result["latitude"] = geo_lat
            result["longitude"] = geo_lng
            result["pincode"] = geo_pincode  # ✅ ADDED
            result["location_source"] = "geocoded"
            result["match_confidence"] = "low"
            print(f"  ✓ Using address only (Pincode: {geo_pincode or 'N/A'})")
            return result
    
    print(f"  ✗ Could not find location")
    return result


def geocode_dataframe(df: pd.DataFrame, address_col="address", name_col="name", 
                     max_workers=4, use_enhanced=True):
    """
    Main geocoding function with two modes:
    - use_enhanced=True: Enhanced mode (Google Places + Geocoding)
    - use_enhanced=False: Basic mode (Address Geocoding only)
    
    Returns DataFrame with: latitude, longitude, pincode, location_source
    """
    
    # Initialize columns
    df["latitude"] = None
    df["longitude"] = None
    df["pincode"] = None  # ✅ ADDED
    df["location_source"] = "not_processed"
    
    if use_enhanced:
        df["match_confidence"] = "none"
        method_name = "ENHANCED (Google Places + Address)"
    else:
        method_name = "BASIC (Address Geocoding Only)"
    
    print(f"\n{'='*70}")
    print(f"Starting {method_name} for {len(df)} records...")
    print(f"{'='*70}\n")
    
    tasks = {}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for idx, row in df.iterrows():
            if use_enhanced:
                task = executor.submit(process_single_row_enhanced, row, address_col, name_col)
            else:
                task = executor.submit(process_single_row_basic, row, address_col)
            
            tasks[task] = idx
        
        completed = 0
        for future in as_completed(tasks):
            idx = tasks[future]
            completed += 1
            
            try:
                result = future.result()
                
                # Update DataFrame
                if result["address"]:
                    df.at[idx, address_col] = result["address"]
                df.at[idx, "latitude"] = result["latitude"]
                df.at[idx, "longitude"] = result["longitude"]
                df.at[idx, "pincode"] = result["pincode"]  # ✅ ADDED
                df.at[idx, "location_source"] = result["location_source"]
                
                if use_enhanced and "match_confidence" in result:
                    df.at[idx, "match_confidence"] = result["match_confidence"]
                
                # Progress update
                if completed % 5 == 0:
                    print(f"\n📊 Progress: {completed}/{len(df)} records processed")
                
            except Exception as e:
                print(f"❌ Error processing row {idx}: {e}")
                df.at[idx, "location_source"] = "error"
            
            # Rate limiting
            time.sleep(0.12)
    
    # Summary statistics
    print(f"\n{'='*70}")
    print(f"📊 GEOCODING SUMMARY ({method_name}):")
    print(f"{'='*70}")
    
    sources = df["location_source"].value_counts()
    for source, count in sources.items():
        percentage = (count / len(df)) * 100
        emoji = "✅" if source in ["geocoded", "google_places_nearby", "google_places_context"] else "❌"
        print(f"{emoji} {source}: {count} ({percentage:.1f}%)")
    
    # ✅ PINCODE STATISTICS
    pincode_found = df["pincode"].notna().sum()
    pincode_percentage = (pincode_found / len(df)) * 100
    print(f"\n📍 Pincode Extraction:")
    print(f"   Found: {pincode_found}/{len(df)} ({pincode_percentage:.1f}%)")
    
    if use_enhanced and "match_confidence" in df.columns:
        print(f"\n🎯 Match Confidence Breakdown:")
        confidence = df["match_confidence"].value_counts()
        for conf, count in confidence.items():
            percentage = (count / len(df)) * 100
            print(f"   {conf}: {count} ({percentage:.1f}%)")
    
    print(f"{'='*70}\n")
    
    return df