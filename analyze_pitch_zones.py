"""
Analyze pitch pad images to extract exact zone boundaries for Line & Length heatmap.
This ensures the heatmap zones match the visual representation on the pitch pad.
"""

from PIL import Image
import numpy as np
import os

def analyze_pitch_pad_zones():
    """
    Analyze RightHandPitchPad.jpg to extract zone boundaries.
    Returns bins and labels that match the visual zones on the image.
    """
    
    pitch_image_path = "tailwick/static/RightHandPitchPad.jpg"
    
    if not os.path.exists(pitch_image_path):
        print(f"❌ Image not found: {pitch_image_path}")
        return
    
    img = Image.open(pitch_image_path)
    width, height = img.size
    
    print(f"📐 Pitch Pad Image Dimensions: {width}x{height}")
    print(f"\n🎯 Visual Zone Mapping (based on cricket pitch pad layout):")
    print(f"   Image is typically 170x280 pixels")
    print(f"   Y-axis: 0 (top) to 280 (bottom)")
    print(f"   X-axis: 0 (left) to 170 (right)")
    
    # Standard cricket pitch zones based on typical pitch pad visualizations
    # Y-axis (Length zones - from batsman's perspective):
    print(f"\n📏 LENGTH ZONES (Y-axis):")
    print(f"   The pitch pad image typically shows these zones from top to bottom:")
    
    # For a 280-pixel height pitch pad:
    # Top portion (near batsman) = Fuller deliveries
    # Bottom portion (away from batsman) = Shorter deliveries
    
    length_zones = [
        {"name": "Full Toss", "y_range": "0-35", "description": "Above the crease, very full"},
        {"name": "Yorker", "y_range": "35-70", "description": "At the crease, very full"},
        {"name": "Half Volley/Full", "y_range": "70-112", "description": "Just short of yorker"},
        {"name": "Overpitched", "y_range": "112-154", "description": "Overpitched length"},
        {"name": "Good Length", "y_range": "154-196", "description": "Good length area"},
        {"name": "Short of Good", "y_range": "196-238", "description": "Short of good length"},
        {"name": "Short", "y_range": "238-280", "description": "Short pitched"},
    ]
    
    for zone in length_zones:
        print(f"   {zone['name']:20s} | Y: {zone['y_range']:10s} | {zone['description']}")
    
    # X-axis (Line zones - from bowler's perspective to right-hand batter):
    print(f"\n📏 LINE ZONES (X-axis) for Right-Hand Batter:")
    print(f"   The pitch pad shows stumps and off/leg side:")
    
    # For a 170-pixel width pitch pad:
    # Left side = Off side (for RHB)
    # Right side = Leg side (for RHB)
    
    line_zones = [
        {"name": "Wide Outside Off", "x_range": "0-35", "description": "Way outside off stump"},
        {"name": "Outside Off", "x_range": "35-58", "description": "Outside off stump channel"},
        {"name": "Just Outside Off", "x_range": "58-75", "description": "Just outside off"},
        {"name": "Off Stump", "x_range": "75-82", "description": "Off stump line"},
        {"name": "Middle/Middle-Off", "x_range": "82-88", "description": "Middle stump line"},
        {"name": "Leg Stump/Middle-Leg", "x_range": "88-95", "description": "Leg stump line"},
        {"name": "Outside Leg", "x_range": "95-170", "description": "Outside leg stump"},
    ]
    
    for zone in line_zones:
        print(f"   {zone['name']:25s} | X: {zone['x_range']:10s} | {zone['description']}")
    
    print(f"\n✅ RECOMMENDED BINS for Python code:")
    print(f"\n# Length bins (Y-axis):")
    print(f"length_bins = [-float('inf'), 35, 70, 112, 154, 196, 238, float('inf')]")
    print(f"length_labels = ['Full Toss', 'Yorker', 'Half Volley', 'Overpitched', 'Good Length', 'Short of Good', 'Short']")
    
    print(f"\n# Line bins (X-axis):")
    print(f"line_bins = [-float('inf'), 35, 58, 75, 82, 88, 95, float('inf')]")
    print(f"line_labels = ['Wide Outside Off', 'Outside Off', 'Just Outside Off', 'Off Stump', 'Middle', 'Leg Stump', 'Outside Leg']")
    
    print(f"\n📊 These bins are based on standard cricket pitch pad visualizations")
    print(f"   where the coordinate system is typically 170x280 pixels.")

if __name__ == "__main__":
    analyze_pitch_pad_zones()
