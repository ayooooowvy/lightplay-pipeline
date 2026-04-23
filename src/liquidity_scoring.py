# =====================================================
# NEARMINT — LIQUIDITY SCORING (Python Heartbeat Integration)
# =====================================================
# Add this to your heartbeat pipeline to calculate
# liquidity scores during each run
# =====================================================

from supabase import create_client
import os
from datetime import datetime

# Initialize Supabase
supabase = create_client(
    os.getenv('SUPABASE_URL'),
    os.getenv('SUPABASE_SERVICE_ROLE_KEY')
)

def calculate_liquidity_score(card_id):
    """
    Calculate liquidity score for a single card.
    Returns score 0-100.
    """
    # Get 7-day sales volume
    response_7d = supabase.table('ebay_sold_comps')\
        .select('*', count='exact')\
        .eq('card_id', card_id)\
        .gte('sold_date', 'now() - interval \'7 days\'')\
        .execute()
    volume_7day = response_7d.count or 0
    
    # Get 30-day sales volume
    response_30d = supabase.table('ebay_sold_comps')\
        .select('*', count='exact')\
        .eq('card_id', card_id)\
        .gte('sold_date', 'now() - interval \'30 days\'')\
        .execute()
    volume_30day = response_30d.count or 0
    
    # Get price volatility from trend_indicators
    trend_response = supabase.table('trend_indicators')\
        .select('volatility')\
        .eq('card_id', card_id)\
        .single()\
        .execute()
    price_volatility = trend_response.data.get('volatility', 10) if trend_response.data else 10
    
    # Calculate average days to sell
    # Simplified: assume listings sell within 14 days on average
    # Can improve with Feed API active listing tracking later
    avg_days_to_sell = 14 - (volume_7day * 2)  # More volume = faster sales
    avg_days_to_sell = max(avg_days_to_sell, 1)
    
    # Calculate sell-through rate
    # Estimate: high volume = high sell-through
    sell_through_rate = min(volume_30day / 50.0, 1.0)
    
    # SCORING LOGIC (max 100 points)
    score = 0
    
    # High volume = more liquid (max 30 points)
    if volume_7day >= 10:
        score += 30
    elif volume_7day >= 5:
        score += 20
    elif volume_7day >= 2:
        score += 10
    
    # Fast sell time = more liquid (max 25 points)
    if avg_days_to_sell <= 3:
        score += 25
    elif avg_days_to_sell <= 7:
        score += 15
    elif avg_days_to_sell <= 14:
        score += 5
    
    # High sell-through = more liquid (max 25 points)
    if sell_through_rate >= 0.80:
        score += 25
    elif sell_through_rate >= 0.60:
        score += 15
    elif sell_through_rate >= 0.40:
        score += 5
    
    # Low volatility = predictable (max 20 points)
    if price_volatility <= 5:
        score += 20
    elif price_volatility <= 10:
        score += 10
    
    # Cap at 100
    score = min(score, 100)
    
    # Update trend_indicators with metrics
    supabase.table('trend_indicators').upsert({
        'card_id': card_id,
        'volume_7day': volume_7day,
        'volume_30day': volume_30day,
        'avg_days_to_sell': avg_days_to_sell,
        'sell_through_rate': sell_through_rate
    }).execute()
    
    return score


def update_all_liquidity_scores():
    """
    Recalculate liquidity scores for all tracked cards.
    Run this in the daily heartbeat.
    """
    print("🔄 Updating liquidity scores...")
    
    # Get all cards that need liquidity scoring (Tiers 1-5)
    response = supabase.table('prices')\
        .select('card_id')\
        .gte('refresh_tier', 1)\
        .lte('refresh_tier', 5)\
        .execute()
    
    cards = response.data
    total = len(cards)
    updated = 0
    total_score = 0
    
    for card in cards:
        card_id = card['card_id']
        
        try:
            score = calculate_liquidity_score(card_id)
            
            # Update prices table with new score
            supabase.table('prices').update({
                'liquidity_score': score,
                'liquidity_calculated_at': datetime.now().isoformat()
            }).eq('card_id', card_id).execute()
            
            updated += 1
            total_score += score
            
            if updated % 100 == 0:
                print(f"  ✓ {updated}/{total} cards scored...")
        
        except Exception as e:
            print(f"  ✗ Error scoring {card_id}: {e}")
            continue
    
    avg_score = total_score / updated if updated > 0 else 0
    
    print(f"✅ Liquidity scoring complete:")
    print(f"  Cards updated: {updated}")
    print(f"  Average score: {avg_score:.1f}/100")
    
    return {
        'cards_updated': updated,
        'avg_score': avg_score
    }


def get_suggested_sell_percent(card_id):
    """
    Get dynamic sell % recommendation based on liquidity score.
    Used by buy/sell algorithm.
    """
    # Get liquidity score
    response = supabase.table('prices')\
        .select('liquidity_score')\
        .eq('card_id', card_id)\
        .single()\
        .execute()
    
    liquidity_score = response.data.get('liquidity_score', 50) if response.data else 50
    
    # Dynamic sell % based on liquidity
    if liquidity_score >= 70:
        return 105  # Hot card - price above market
    elif liquidity_score >= 40:
        return 100  # Normal - price at market
    else:
        return 95   # Slow mover - discount to move fast


# =====================================================
# ADD TO HEARTBEAT PIPELINE
# =====================================================
# In your main heartbeat script, add this step:
#
# def daily_heartbeat():
#     # ... existing steps ...
#     
#     # Step N: Update liquidity scores
#     liquidity_results = update_all_liquidity_scores()
#     
#     # Log results to pipeline_runs
#     # ...
# =====================================================
