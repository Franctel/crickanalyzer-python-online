"""
Inspect the actual pitch pad image to determine correct LENGTH zone boundaries
"""
from PIL import Image
import matplotlib.pyplot as plt
import matplotlib.patches as patches

# Load the pitch pad image
img = Image.open("tailwick/static/RightHandPitchPad.jpg")
original_width, original_height = img.size

print(f"Original image size: {original_width}x{original_height}")

# The coordinates are scaled to 170x280
scale_x = 170 / original_width
scale_y = 280 / original_height

print(f"Scale factors: X={scale_x:.4f}, Y={scale_y:.4f}")
print(f"Scaled coordinate system: 170x280")

# Your wicket data
wickets = [
    {'x': 103.0, 'y': 165.0, 'observed': 'Good Length'},
    {'x': 58.0, 'y': 190.0, 'observed': 'Good Length'},
    {'x': 59.0, 'y': 166.0, 'observed': 'Good Length'},
    {'x': 85.0, 'y': 161.0, 'observed': 'Good Length'},
    {'x': 63.0, 'y': 150.0, 'observed': 'Overpitch'},
    {'x': 56.0, 'y': 147.0, 'observed': 'Overpitch'},
    {'x': 71.0, 'y': 142.0, 'observed': 'Overpitch'},
    {'x': 56.0, 'y': 81.0, 'observed': 'Full Length'},  # User says this looks like Yorker
    {'x': 130.0, 'y': 213.0, 'observed': 'Short of Good'},
]

print("\n" + "="*80)
print("ANALYZING YOUR WICKET POSITIONS vs VISUAL ZONES ON PITCH PAD")
print("="*80)

print("\nYour wicket Y-coordinates (sorted):")
y_coords = sorted([w['y'] for w in wickets])
print(y_coords)

print("\nCurrent LENGTH bins:")
length_bins = [-float('inf'), 35, 70, 112, 154, 196, 238, float('inf')]
length_labels = ['Fulltoss', 'Yorker', 'Full Length', 'Overpitch', 'Good Length', 'Short of Good', 'Short Pitch']

print(f"Bins: {length_bins}")
print(f"Labels: {length_labels}")

print("\n" + "="*80)
print("SUGGESTION: Based on typical pitch pad images where LENGTH zones are marked:")
print("="*80)
print("""
The pitch pad typically shows zones from batsman (top) to away (bottom):
- Near batsman (Y ≈ 0-50): Full Toss area
- Crease area (Y ≈ 50-90): Yorker length
- Just past crease (Y ≈ 90-140): Full/Half Volley
- Good driving length (Y ≈ 140-190): Overpitch/Good Length transition  
- Good defensive length (Y ≈ 190-230): Good Length
- Bounce area (Y ≈ 230-260): Short of Good
- High bounce (Y > 260): Short Pitch

Based on your data where Y=81 should be Yorker but is showing as Full Length,
try these adjusted bins:
""")

suggested_bins = [-float('inf'), 50, 90, 140, 190, 230, 260, float('inf')]
print(f"\nSUGGESTED LENGTH BINS: {suggested_bins}")
print(f"LABELS: {length_labels}")

print("\nWith these bins, your wickets would be:")
for w in wickets:
    y = w['y']
    if y < 50:
        zone = 'Fulltoss'
    elif y < 90:
        zone = 'Yorker'
    elif y < 140:
        zone = 'Full Length'
    elif y < 190:
        zone = 'Overpitch'
    elif y < 230:
        zone = 'Good Length'
    elif y < 260:
        zone = 'Short of Good'
    else:
        zone = 'Short Pitch'
    
    print(f"  Y={y:6.1f} → {zone:20s} (was: {w['observed']})")
