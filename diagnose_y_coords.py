"""
URGENT: Diagnose Y-coordinate binning bug in Line & Length heatmap
"""
import mysql.connector
import pandas as pd
import os
import json

def analyze_y_coordinates():
    # Load config
    config_path = "tailwick/config.json"
    with open(config_path, 'r') as f:
        cfg = json.load(f)
    
    # Connect to MySQL
    conn = mysql.connector.connect(
        host=cfg['host'],
        port=cfg['port'],
        user=cfg['username'],
        password=cfg['password'],
        database=cfg['database']
    )
    
    # Get Y coordinate distribution
    query = """
    SELECT 
        scrM_PitchY,
        scrM_PitchYPos,
        COUNT(*) as count
    FROM tblscoremaster
    WHERE scrM_PitchY IS NOT NULL
    GROUP BY scrM_PitchY, scrM_PitchYPos
    ORDER BY scrM_PitchY
    LIMIT 50
    """
    
    print("="*80)
    print("ANALYZING Y-COORDINATE DISTRIBUTION")
    print("="*80)
    
    try:
        df = pd.read_sql_query(query, conn)
        print(f"\n✅ Found {len(df)} distinct Y values (showing first 50)")
        print(f"\nY-coordinate range: {df['scrM_PitchY'].min()} to {df['scrM_PitchY'].max()}")
        print(f"\nFirst 20 rows:")
        print(df.head(20).to_string())
        
        # Analyze distribution
        print("\n" + "="*80)
        print("CURRENT LENGTH BINS IN UTILS.PY:")
        print("="*80)
        length_bins = [-float('inf'), 80, 120, 180, 220, 250, 270, float('inf')]
        length_labels = ['Short Pitch', 'Short of Good', 'Good Length', 'Overpitch',
                         'Full Length', 'Yorker', 'Fulltoss']
        
        print(f"Bins: {length_bins}")
        print(f"Labels: {length_labels}")
        
        # Check distribution across bins
        df_copy = df.copy()
        df_copy['LengthZone'] = pd.cut(df_copy['scrM_PitchY'], bins=length_bins, labels=length_labels, right=False)
        
        zone_counts = df_copy.groupby('LengthZone', observed=False)['count'].sum()
        
        print("\n" + "="*80)
        print("BALL DISTRIBUTION ACROSS ZONES (with current bins):")
        print("="*80)
        for zone, count in zone_counts.items():
            print(f"{zone:20s}: {count:6.0f} balls")
        
        # Get specific examples with their positions
        print("\n" + "="*80)
        print("SAMPLE BALLS WITH Y-COORDINATE AND ASSIGNED ZONE:")
        print("="*80)
        
        query_samples = """
        SELECT 
            scrM_PitchY,
            scrM_PitchYPos,
            scrM_PitchX,
            scrM_PitchXPos,
            scrM_BatsmanRuns,
            scrM_IsWicket
        FROM tblscoremaster
        WHERE scrM_PitchY IS NOT NULL
        ORDER BY scrM_PitchY
        LIMIT 100
        """
        
        df_samples = pd.read_sql_query(query_samples, conn)
        df_samples['LengthZone'] = pd.cut(df_samples['scrM_PitchY'], bins=length_bins, labels=length_labels, right=False)
        
        print("\nLowest Y values (supposedly closest to batsman):")
        print(df_samples.head(10)[['scrM_PitchY', 'scrM_PitchYPos', 'LengthZone']].to_string())
        
        print("\nHighest Y values (supposedly furthest from batsman):")
        print(df_samples.tail(10)[['scrM_PitchY', 'scrM_PitchYPos', 'LengthZone']].to_string())
        
        # Check the scrM_PitchYPos field (pre-categorized position)
        print("\n" + "="*80)
        print("CHECKING PRE-CATEGORIZED POSITION (scrM_PitchYPos):")
        print("="*80)
        
        query_pos = """
        SELECT 
            scrM_PitchYPos,
            MIN(scrM_PitchY) as min_y,
            MAX(scrM_PitchY) as max_y,
            AVG(scrM_PitchY) as avg_y,
            COUNT(*) as count
        FROM tblscoremaster
        WHERE scrM_PitchY IS NOT NULL AND scrM_PitchYPos IS NOT NULL
        GROUP BY scrM_PitchYPos
        ORDER BY avg_y
        """
        
        df_pos = pd.read_sql_query(query_pos, conn)
        print("\nPosition labels with their Y ranges:")
        print(df_pos.to_string())
        
        # CRITICAL: Check if Y=0 is at top or bottom
        print("\n" + "="*80)
        print("🔍 CRITICAL ANALYSIS: Y-AXIS DIRECTION")
        print("="*80)
        
        if not df_pos.empty:
            # Find position names
            pos_names = df_pos['scrM_PitchYPos'].tolist()
            y_ranges = df_pos[['scrM_PitchYPos', 'min_y', 'max_y', 'avg_y']].values
            
            print("\nIf Y=0 is at BATSMAN END (top):")
            print("  → Low Y values = Full/Yorker (near batsman)")
            print("  → High Y values = Short Pitch (away from batsman)")
            
            print("\nIf Y=0 is at BOWLER END (bottom):")
            print("  → Low Y values = Short Pitch (away from batsman)")
            print("  → High Y values = Full/Yorker (near batsman)")
            
            print("\n📊 Based on actual data:")
            for pos, min_y, max_y, avg_y in y_ranges:
                print(f"  {pos:30s}: Y range [{min_y:6.1f} - {max_y:6.1f}], avg={avg_y:6.1f}")
        
    except Exception as e:
        print(f"❌ Error querying database: {e}")
    
    finally:
        conn.close()

if __name__ == "__main__":
    analyze_y_coordinates()
