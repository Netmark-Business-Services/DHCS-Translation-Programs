import csv
import os
import re
from datetime import datetime
import json

# Load config.json
with open("config.json", "r") as f:
    config = json.load(f)

# Use config values
input_file = config["input_file"]
output_csv = config["hisdb_output_csv"]


# Define headers
headers = [
    'Medicaid ID', 'Carrier Code(s)', 'Policy Number(s)', 'Scope of Coverage',
    'Policy Start Date(s)', 'Policy End Date(s)', 'Last Change Date(s)',
    'County ID', 'MEDS Current Year/Month (CCYYMM)', 'Segment Type',
    'Source of Change', 'Transaction Type', 'Insurance Status Code',
    'Termination Reason', 'File Type', 'File Date'
]

# --- Extract File Type and File Date ---
file_type = config["file_type"]

# Extract date pattern starting with 'D' followed by 6 digits
match = re.search(r'\.D(\d{6})$', input_file)
if match:
    file_date = match.group(1) 
else:
    file_date = ''

# --- Write CSV ---
with open(output_csv, 'w', newline='') as out_file:
    writer = csv.DictWriter(out_file, fieldnames=headers,delimiter='|')
    writer.writeheader()

    with open(input_file, 'r') as file:
        for record in file:
            if len(record.strip()) == 0:
                continue 

            # Beneficiary Info
            medicaid_id_raw = record[0:9].strip()
            # Skip if medicaid_id contains non-ASCII or is corrupt
            try:
                medicaid_id = medicaid_id_raw.encode('latin1').decode('ascii').strip()
            except UnicodeDecodeError:
                continue
                    
            try:
                num_segments = int(record[9:11].strip())
            except ValueError:
                continue 

            meds_current_ym = record[73:79].strip()

            # Insurance Segments
            segment_start = 83
            segment_length = 511
            
            if num_segments == 0:
                    writer.writerow({
                        'Medicaid ID': medicaid_id,
                        'Carrier Code(s)': '',
                        'Policy Number(s)': '',
                        'Scope of Coverage': '',
                        'Policy Start Date(s)': '',
                        'Policy End Date(s)': '',
                        'Last Change Date(s)': '',
                        'County ID': '',
                        'MEDS Current Year/Month (CCYYMM)': meds_current_ym,
                        'Segment Type': '',
                        'Source of Change': '',
                        'Transaction Type': '',
                        'Insurance Status Code': '',
                        'Termination Reason': '',
                        'File Type': file_type,
                        'File Date': file_date
                    })
                    continue


            for seg_num in range(num_segments):
                start = segment_start + (seg_num * segment_length)
                segment = record[start:start + segment_length]
                row = {
                    'Medicaid ID': medicaid_id,
                    'Carrier Code(s)': segment[0:4].strip(),
                    'Policy Number(s)': segment[4:34].strip(),
                    'Scope of Coverage': segment[35:51].rstrip(),
                    'Policy Start Date(s)': segment[51:59].strip(),
                    'Policy End Date(s)': segment[59:67].strip(),
                    'Last Change Date(s)': segment[67:75].strip(),
                    'County ID': segment[425:439].strip(),
                    'MEDS Current Year/Month (CCYYMM)': meds_current_ym,
                    'Segment Type': segment[34].strip(),
                    'Source of Change': segment[75:79].strip(),
                    'Transaction Type': segment[79].strip(),
                    'Insurance Status Code': segment[89].strip(),
                    'Termination Reason': segment[93:95].strip(),
                    'File Type': file_type,
                    'File Date': file_date
                }
             
                writer.writerow(row)

print(f"Extraction complete! Data saved to '{output_csv}'. File Type: {file_type}, File Date: {file_date}")
