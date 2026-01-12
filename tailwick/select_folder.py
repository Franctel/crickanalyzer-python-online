# select_folder.py
import tkinter as tk
from tkinter import filedialog
import json
import sys

root = tk.Tk()
root.withdraw()
folder = filedialog.askdirectory(title="Select Video Folder")
print(json.dumps({"folder": folder}))
