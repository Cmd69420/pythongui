import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

GOOGLE_API_KEY = "AIzaSyCwGkrq4Onpvj9Yu5His9row-fIg5v6N0I"
GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"
PLACES_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
PLACE_DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"


def search_business_on_google(business_name: str, location_hint: str = "India"):
    """
    Search for a business on Google Places.
    Returns (address, lat, lng) if found, else (None, None, None)
    """
    try:
        # Step 1: Find Place from Text
        params = {
            "input": f"{business_name} {location_hint}",
            "inputtype": "textquery",
            "fields": "place_id,name,formatted_address,geometry",
            "key": GOOGLE_API_KEY
        }
        
        r = requests.get(PLACES_SEARCH_URL, params=params, timeout=10)
        data = r.json()
        
        if data["status"] == "OK" and len(data["candidates"]) > 0:
            candidate = data["candidates"][0]
            
            # Extract data
            address = candidate.get("formatted_address", "")
            geometry = candidate.get("geometry", {}).get("location", {})
            lat = geometry.get("lat")
            lng = geometry.get("lng")
            
            if address and lat and lng:
                return address, lat, lng
        
        return None, None, None
    
    except Exception as e:
        print(f"Error searching business '{business_name}': {e}")
        return None, None, None


def geocode_address(address: str):
    """
    Standard geocoding for addresses.
    """
    try:
        params = {
            "address": address,
            "key": GOOGLE_API_KEY
        }
        r = requests.get(GEOCODE_URL, params=params, timeout=10)
        data = r.json()

        if data["status"] == "OK":
            loc = data["results"][0]["geometry"]["location"]
            formatted_addr = data["results"][0].get("formatted_address", address)
            return formatted_addr, loc["lat"], loc["lng"]

        return None, None, None
    
    except Exception as e:
        print(f"Error geocoding address '{address}': {e}")
        return None, None, None


def is_likely_business_name(name: str) -> bool:
    """
    Determine if a name is likely a business rather than a person.
    Returns True if it's likely a business, False if it's likely a personal name.
    """
    name_lower = name.lower().strip()
    
    # Empty or very short names - skip
    if len(name_lower) < 3:
        return False
    
    # Personal name indicators (titles)
    personal_titles = [
        'mr.', 'mrs.', 'ms.', 'miss', 'dr.', 'prof.',
        'shri', 'smt.', 'kumari', 'master', 'baby'
    ]
    
    for title in personal_titles:
        if name_lower.startswith(title):
            return False
    
    # Business indicators (keywords)
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
        '&', ' and ', ' n '  # Common in business names
    ]
    
    for keyword in business_keywords:
        if keyword in name_lower:
            return True
    
    # Check if name has only 2-3 words without business keywords (likely personal)
    words = name_lower.split()
    if len(words) <= 3 and not any(kw in name_lower for kw in business_keywords):
        # Could be "John Doe" or "Rajendra K Shah" - likely personal
        # Check if all words start with capital (common in Indian personal names)
        if all(len(w) <= 10 for w in words):  # Personal names usually short words
            return False
    
    # If it has more than 3 words, it might be a business
    if len(words) > 3:
        return True
    
    # Default: treat as personal name (safer to not search)
    return False


def process_single_row_enhanced(row, address_col="address", name_col="name"):
    """
    ENHANCED METHOD: Process a single row with Google Places first, then geocoding fallback.
    Only searches Google Places if the name appears to be a business.
    Returns dict with updated address, latitude, longitude, and source.
    """
    business_name = str(row.get(name_col, "")).strip()
    original_address = str(row.get(address_col, "")).strip()
    
    result = {
        "address": original_address,
        "latitude": None,
        "longitude": None,
        "location_source": "not_found"
    }
    
    # Skip empty names/addresses
    if not business_name or business_name.lower() == "nan":
        return result
    
    # Step 1: Check if this looks like a business name (not personal)
    if is_likely_business_name(business_name):
        # Try Google Places search with business name
        print(f"[ENHANCED] Searching Google Places for: {business_name}")
        places_addr, places_lat, places_lng = search_business_on_google(business_name)
        
        if places_addr and places_lat and places_lng:
            result["address"] = places_addr
            result["latitude"] = places_lat
            result["longitude"] = places_lng
            result["location_source"] = "google_places"
            print(f"  ✓ Found via Google Places: {places_addr[:50]}...")
            return result
        else:
            print(f"  → Business not found on Google Places, trying address...")
    else:
        print(f"[ENHANCED] Skipping Places (personal name): {business_name}")
    
    # Step 2: Fallback to geocoding original address
    if original_address and original_address.lower() != "nan":
        print(f"  → Geocoding address: {original_address[:50]}...")
        geocoded_addr, geo_lat, geo_lng = geocode_address(original_address)
        
        if geocoded_addr and geo_lat and geo_lng:
            result["address"] = geocoded_addr
            result["latitude"] = geo_lat
            result["longitude"] = geo_lng
            result["location_source"] = "geocoded"
            print(f"  ✓ Geocoded: {geocoded_addr[:50]}...")
            return result
    
    print(f"  ✗ Could not find location for: {business_name}")
    return result


def process_single_row_basic(row, address_col="address"):
    """
    BASIC METHOD: Only use address geocoding (no Google Places search).
    Returns dict with updated address, latitude, longitude, and source.
    """
    original_address = str(row.get(address_col, "")).strip()
    
    result = {
        "address": original_address,
        "latitude": None,
        "longitude": None,
        "location_source": "not_found"
    }
    
    # Skip empty addresses
    if not original_address or original_address.lower() == "nan":
        return result
    
    # Only geocode the address
    print(f"[BASIC] Geocoding address: {original_address[:50]}...")
    geocoded_addr, geo_lat, geo_lng = geocode_address(original_address)
    
    if geocoded_addr and geo_lat and geo_lng:
        result["address"] = geocoded_addr
        result["latitude"] = geo_lat
        result["longitude"] = geo_lng
        result["location_source"] = "geocoded"
        print(f"  ✓ Geocoded: {geocoded_addr[:50]}...")
        return result
    
    print(f"  ✗ Could not geocode: {original_address[:30]}...")
    return result


def geocode_dataframe(df: pd.DataFrame, address_col="address", name_col="name", 
                     max_workers=4, use_enhanced=True):
    """
    Geocode DataFrame with option to use enhanced (Places + Geocoding) or basic (Geocoding only).
    
    Args:
        df: DataFrame with business data
        address_col: Column name containing addresses
        name_col: Column name containing business names (only used if use_enhanced=True)
        max_workers: Number of parallel workers
        use_enhanced: If True, uses Google Places + Geocoding. If False, only uses Geocoding.
    
    Returns:
        DataFrame with updated address, latitude, longitude, and location_source columns
    """
    # Initialize new columns
    df["latitude"] = None
    df["longitude"] = None
    df["location_source"] = "not_processed"
    
    method_name = "ENHANCED (Google Places + Geocoding)" if use_enhanced else "BASIC (Geocoding Only)"
    
    print(f"\n{'='*60}")
    print(f"Starting {method_name} for {len(df)} records...")
    print(f"{'='*60}\n")
    
    tasks = {}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for idx, row in df.iterrows():
            if use_enhanced:
                # Use enhanced method with Google Places
                tasks[executor.submit(process_single_row_enhanced, row, address_col, name_col)] = idx
            else:
                # Use basic method with only geocoding
                tasks[executor.submit(process_single_row_basic, row, address_col)] = idx
        
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
                df.at[idx, "location_source"] = result["location_source"]
                
                # Progress update
                if completed % 10 == 0:
                    print(f"\nProgress: {completed}/{len(df)} records processed")
                
            except Exception as e:
                print(f"Error processing row {idx}: {e}")
                df.at[idx, "location_source"] = "error"
            
            # Small delay to avoid hitting API rate limits
            time.sleep(0.1)
    
    # Summary statistics
    print(f"\n{'='*60}")
    print(f"GEOCODING SUMMARY ({method_name}):")
    print(f"{'='*60}")
    
    sources = df["location_source"].value_counts()
    for source, count in sources.items():
        percentage = (count / len(df)) * 100
        print(f"{source}: {count} ({percentage:.1f}%)")
    
    print(f"{'='*60}\n")
    
    return df