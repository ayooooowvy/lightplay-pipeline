#!/usr/bin/env python3
"""
LightPlay Heartbeat Pipeline
Runs daily to fetch eBay data using Bootstrap Mode tiered refresh
Stays under 5,000 API calls/day until partner approval
"""

import os
import time
import requests
import psycopg2
from datetime import datetime, timedelta
from supabase import create_client
import schedule

# Initialize Supabase
supabase = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_SERVICE_ROLE_KEY')
)

# eBay API credentials
EBAY_APP_ID = os.getenv('EBAY_APP_ID')
EBAY_CERT_ID = os.getenv('EBAY_CERT_ID')
EBAY_TOKEN = os.getenv('EBAY_USER_TOKEN')

# API call counter (track to stay under 5k/day)
api_calls_today = 0
MAX_CALLS_PER_DAY = 4800  # Leave 200 buffer

def log(message):
    """Print timestamped log message"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}")

def get_ebay_auth_token():
    """Get eBay OAuth token (cached for 2 hours)"""
    # For Bootstrap Mode, use User Token from eBay Developer account
    # Later: implement OAuth flow for production
    return EBAY_TOKEN

def fetch_ebay_sold_comps(card_name, card_set, limit=10):
    """
    Fetch sold comps for a card from eBay Browse API
    Returns list of sold listings
    """
    global api_calls_today
    
    if api_calls_today >= MAX_CALLS_PER_DAY:
        log(f"⚠️  Hit daily API limit ({MAX_CALLS_PER_DAY}), skipping remaining cards")
        return []
    
    # Build search query
    query = f"{card_name} {card_set} Pokemon"
    
    # eBay Browse API endpoint
    url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
    
    headers = {
        'Authorization': f'Bearer {get_ebay_auth_token()}',
        'X-EBAY-C-MARKETPLACE-ID': 'EBAY_US'
    }
    
    params = {
        'q': query,
        'filter': 'buyingOptions:{AUCTION|FIXED_PRICE},itemEndDate:[..],priceCurrency:USD',
        'sort': 'itemEndDate',
        'limit': limit
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        api_calls_today += 1
        
        if response.status_code == 200:
            data = response.json()
            return data.get('itemSummaries', [])
        else:
            log(f"❌ eBay API error {response.status_code}: {response.text[:100]}")
            return []
            
    except Exception as e:
        log(f"❌ Error fetching eBay data: {e}")
        return []

def parse_condition_from_title(title):
    """Extract condition from listing title"""
    title_lower = title.lower()
    
    if 'near mint' in title_lower or 'nm' in title_lower:
        return 'NM'
    elif 'lightly played' in title_lower or 'lp' in title_lower:
        return 'LP'
    elif 'moderately played' in title_lower or 'mp' in title_lower:
        return 'MP'
    elif 'heavily played' in title_lower or 'hp' in title_lower:
        return 'HP'
    elif 'damaged' in title_lower:
        return 'DM'
    else:
        return 'NM'  # Default assumption

def parse_grade_from_title(title):
    """Extract PSA/BGS grade from title"""
    title_lower = title.lower()
    
    if 'psa 10' in title_lower:
        return True, 'PSA 10'
    elif 'psa 9' in title_lower:
        return True, 'PSA 9'
    elif 'psa 8' in title_lower:
        return True, 'PSA 8'
    elif 'bgs 9.5' in title_lower:
        return True, 'BGS 9.5'
    elif 'cgc' in title_lower:
        return True, 'CGC'
    else:
        return False, None

def get_cards_needing_refresh():
    """
    Get cards that need eBay data refresh based on tiered schedule
    Bootstrap Mode: Track 7,500 cards ($5+) with tiered refresh
    """
    now = datetime.now()
    
    cards_to_fetch = []
    
    # Tier 1: ≥$500 or high liquidity - Daily refresh
    tier1 = supabase.table('prices')\
        .select('card_id, cards(name, set_name)')\
        .eq('refresh_tier', 1)\
        .or_(f'last_ebay_fetch.is.null,last_ebay_fetch.lt.{(now - timedelta(hours=24)).isoformat()}')\
        .limit(400)\
        .execute()
    
    cards_to_fetch.extend(tier1.data if tier1.data else [])
    
    # Tier 2: $100-499 - Daily refresh
    tier2 = supabase.table('prices')\
        .select('card_id, cards(name, set_name)')\
        .eq('refresh_tier', 2)\
        .or_(f'last_ebay_fetch.is.null,last_ebay_fetch.lt.{(now - timedelta(hours=24)).isoformat()}')\
        .limit(1100)\
        .execute()
    
    cards_to_fetch.extend(tier2.data if tier2.data else [])
    
    # Tier 3: $25-99 - Daily refresh
    tier3 = supabase.table('prices')\
        .select('card_id, cards(name, set_name)')\
        .eq('refresh_tier', 3)\
        .or_(f'last_ebay_fetch.is.null,last_ebay_fetch.lt.{(now - timedelta(hours=24)).isoformat()}')\
        .limit(1800)\
        .execute()
    
    cards_to_fetch.extend(tier3.data if tier3.data else [])
    
    # Tier 4: $10-24 - Every 2 days (fetch 50% daily)
    tier4 = supabase.table('prices')\
        .select('card_id, cards(name, set_name)')\
        .eq('refresh_tier', 4)\
        .or_(f'last_ebay_fetch.is.null,last_ebay_fetch.lt.{(now - timedelta(days=2)).isoformat()}')\
        .limit(1400)\
        .execute()
    
    cards_to_fetch.extend(tier4.data if tier4.data else [])
    
    # Tier 5: $5-9.99 - Weekly (fetch ~14% daily)
    tier5 = supabase.table('prices')\
        .select('card_id, cards(name, set_name)')\
        .eq('refresh_tier', 5)\
        .or_(f'last_ebay_fetch.is.null,last_ebay_fetch.lt.{(now - timedelta(days=7)).isoformat()}')\
        .limit(200)\
        .execute()
    
    cards_to_fetch.extend(tier5.data if tier5.data else [])
    
    log(f"📋 Found {len(cards_to_fetch)} cards needing refresh")
    return cards_to_fetch

def process_ebay_data():
    """
    Main function: Fetch eBay data for cards needing refresh
    """
    global api_calls_today
    api_calls_today = 0
    
    log("🔄 Starting eBay data fetch...")
    
    # Get cards needing refresh
    cards = get_cards_needing_refresh()
    
    if not cards:
        log("✅ No cards need refresh")
        return
    
    processed = 0
    skipped = 0
    
    for card in cards:
        # Check API limit
        if api_calls_today >= MAX_CALLS_PER_DAY:
            log(f"⚠️  Hit daily limit, stopping. Processed: {processed}, Skipped: {len(cards) - processed}")
            break
        
        card_id = card['card_id']
        card_name = card['cards']['name']
        card_set = card['cards']['set_name']
        
        # Fetch eBay sold comps
        listings = fetch_ebay_sold_comps(card_name, card_set, limit=10)
        
        if not listings:
            skipped += 1
            continue
        
        # Process each listing
        for listing in listings:
            condition = parse_condition_from_title(listing.get('title', ''))
            graded, grade = parse_grade_from_title(listing.get('title', ''))
            
            # Insert into ebay_sold_comps
            try:
                supabase.table('ebay_sold_comps').upsert({
                    'card_id': card_id,
                    'ebay_item_id': listing.get('itemId'),
                    'title': listing.get('title'),
                    'sold_price': float(listing.get('price', {}).get('value', 0)),
                    'sold_date': listing.get('itemEndDate'),
                    'condition': condition,
                    'graded': graded,
                    'grade': grade,
                    'listing_url': listing.get('itemWebUrl'),
                    'image_url': listing.get('image', {}).get('imageUrl')
                }, on_conflict='ebay_item_id').execute()
            except Exception as e:
                log(f"❌ Error inserting sold comp: {e}")
        
        # Update last_ebay_fetch timestamp
        supabase.table('prices').update({
            'last_ebay_fetch': datetime.now().isoformat(),
            'ebay_sold_count': len(listings)
        }).eq('card_id', card_id).execute()
        
        processed += 1
        
        # Rate limit: 1 request/second
        time.sleep(1)
        
        if processed % 100 == 0:
            log(f"  ✓ Processed {processed}/{len(cards)} cards ({api_calls_today} API calls)")
    
    log(f"✅ eBay fetch complete: {processed} cards processed, {skipped} skipped, {api_calls_today} API calls used")

def run_heartbeat():
    """
    Full heartbeat execution
    Run daily at 2 AM UTC
    """
    log("=" * 60)
    log("🚀 Starting LightPlay heartbeat")
    log("=" * 60)
    
    start_time = datetime.now()
    
    # Log pipeline run (use direct DB connection to avoid RLS issues)
    import psycopg2
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO pipeline_runs (started_at, status)
        VALUES (%s, %s)
        RETURNING id
    """, (start_time, 'running'))
    run_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    
    try:
        # Step 1: Fetch eBay data
        process_ebay_data()
        
        # Step 2: Calculate liquidity scores
        # (import from liquidity_scoring.py)
        from liquidity_scoring import update_all_liquidity_scores
        liquidity_result = update_all_liquidity_scores()
        
        # Step 3: Calculate trend indicators
        # TODO: Implement trend calculation
        
        # Step 4: Run predictions
        # TODO: Implement prediction model
        
        # Mark run as complete (use direct DB connection)
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        cur.execute("""
            UPDATE pipeline_runs
            SET finished_at = %s,
                duration_secs = %s,
                cards_processed = %s,
                status = %s
            WHERE id = %s
        """, (end_time, int(duration), api_calls_today, 'success', run_id))
        conn.commit()
        cur.close()
        conn.close()
        
        log(f"✅ Heartbeat complete in {duration:.1f}s")
        
    except Exception as e:
        log(f"❌ Heartbeat failed: {e}")
        
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        cur = conn.cursor()
        cur.execute("""
            UPDATE pipeline_runs
            SET finished_at = %s,
                status = %s,
                errors = %s
            WHERE id = %s
        """, (datetime.now(), 'failed', str(e), run_id))
        conn.commit()
        cur.close()
        conn.close()
    
    log("=" * 60)

# Schedule daily run at 2 AM UTC
schedule.every().day.at("02:00").do(run_heartbeat)

# Also run immediately on startup (for testing)
if __name__ == "__main__":
    log("🎯 LightPlay Heartbeat starting...")
    log("📅 Scheduled: Daily at 2:00 AM UTC")
    
    # Run once immediately
    run_heartbeat()
    
    # Then keep running on schedule
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute
