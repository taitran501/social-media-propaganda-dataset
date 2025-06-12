#!/usr/bin/env python3

import sys
from csv import reader
from os import system, getcwd, remove, path, makedirs
from datetime import datetime as d
from pyperclip import paste, PyperclipException
from openpyxl import Workbook

# Get current directory and set output directory
cur_dir = path.dirname(path.abspath(__file__))
output_dir = path.join(cur_dir, "..", "output")

# Create output directory if it doesn't exist
if not path.exists(output_dir):
    try:
        makedirs(output_dir)
        print("\x1b[32m[*]\x1b[0m Output directory created successfully.")
    except Exception as e:
        print(f"\x1b[31m[!]\x1b[0m Could not create output directory: {e}")
        # If output directory can't be created, use the parent directory
        output_dir = path.join(cur_dir, "..")

# Set paths for CSV and Excel files in the output directory
csv_path = path.join(output_dir, "RedditComments.csv")
excel_path = path.join(output_dir, f"Comments_{d.timestamp(d.now())}.xlsx")

# Initialize prompt to support ANSI escape sequences
system("")

# Get data from clipboard
try:
    csv_data = paste()
except PyperclipException:
    print("\x1b[31m[*]\x1b[0m Could not find copy/paste mechanism on this system. Please paste the CSV data below and end the input with an empty line:")
    aux = ''
    csv_data = '\n'.join(iter(input, aux))

# Write CSV data to file
try:
    print("\x1b[34m[*]\x1b[0m Writing CSV from clipboard to file + " \
        "removing carriage return characters ('\\r').", end="", flush=True)
    open(csv_path, "w", encoding="utf-8").write(csv_data.replace("\r","\n").replace("\n\n","\n"))
except Exception as e:
    print(e)
    print("\n\x1b[31m[X]\x1b[0m Couldn't write to CSV file. Does it already exist?")
    sys.exit(1)

print("\r\x1b[32m[*]\x1b[0m Writing CSV from clipboard to file + removing carriage return characters ('\\r').")

# Create Excel workbook and set active worksheet
wb = Workbook()
ws = wb.active
ws.title = "Reddit Comments"

# Add column headers
headers = ["post_id", "post_raw", "comment_id", "author", "created_date", "comment_raw"]
ws.append(headers)

# Convert CSV to Excel
print("\x1b[34m[*]\x1b[0m Converting CSV file to Excel Workbook (XLSX).", end="", flush=True)
line_count = 0

with open(csv_path, 'r+', encoding="utf-8") as f:
    csv_reader = reader(f)
    next(csv_reader, None)  # Skip header
    
    for row in csv_reader:
        if len(row) >= 6:  # Ensure row has enough columns
            ws.append(row)
            line_count += 1

print("\r\x1b[32m[*]\x1b[0m Converting CSV file to Excel Workbook (XLSX).")
print(f"\x1b[32m[*]\x1b[0m Written {line_count} line(s).")

# Save Excel file
print("\x1b[34m[*]\x1b[0m Saving XLSX file.", end="", flush=True)
wb.save(excel_path)
print("\r\x1b[32m[*]\x1b[0m Saving XLSX file.")
print(f"\x1b[32m[*]\x1b[0m File saved to: {excel_path}")

# Delete temporary CSV file
print("\x1b[34m[*]\x1b[0m Deleting CSV file.", end="", flush=True)
try:
    remove(csv_path)
    print("\r\x1b[32m[*]\x1b[0m Deleting CSV file.")
except:
    print("\r\x1b[31m[*]\x1b[0m Could not delete CSV file.")

print("\x1b[32m[*]\x1b[0m Done.", end="\n\n") 