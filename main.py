import os, sys, io
from pathlib import Path
from tailwick import create_app

# Ensure stdout/stderr use UTF-8 (safe for PyInstaller where sys.stdout may be None)
if sys.stdout and hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr and hasattr(sys.stderr, "buffer"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Helper to resolve paths in dev + packaged exe
def resource_path(rel):
    base = getattr(sys, "_MEIPASS", Path(__file__).resolve().parent)
    return str(Path(base, rel))

# Create the Flask app with safe paths
app = create_app()

if __name__ == "__main__":
    app.run(debug=True)
