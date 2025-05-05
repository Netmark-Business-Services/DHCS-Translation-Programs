import os
import zipfile
import pandas as pd
import json

# Load config.json
with open("config.json", "r") as f:
    config = json.load(f)

# Use config values
zip_path = config["zip_path"]
extract_dir = config["extract_dir"]
output_csv = config["834x12_output_csv"]
output_pipe = config["output_pipe"]
cleaned_pipe_file = config["cleaned_pipe_file"]


# --- UNZIP FILE ---
os.makedirs(extract_dir, exist_ok=True)
with zipfile.ZipFile(zip_path, 'r') as zip_ref:
    zip_ref.extractall(extract_dir)
x12_path = os.path.join(extract_dir, os.listdir(extract_dir)[0])

with open(x12_path, 'r') as file:
    segments = file.read().split("~")

# --- FILE TYPE AND DATE ---
file_type = file_date = ""
for segment in segments:
    parts = segment.split("*")
    if parts[0] == "BGN" and len(parts) > 2:
        bgn_parts = parts[2].split("-")
        if len(bgn_parts) >= 3:
            file_type = bgn_parts[1]
            file_date = bgn_parts[2]
        break

# --- HEADERS ---
expected_fields = [
    'CIN', 'MEDS ID', 'MBI', 'Last Name', 'First Name', 'MI', 'DOB', 'Gender', 'Death Date',
    'Redeterm Date', 'Expected Delivery Date', 'Case County Code', 'Case Aid Code', 'Case Number',
    'Term Reason', 'Ethnicity', 'Language1', 'Language2', 'Res Address 1', 'Res Address 2',
    'Res City', 'Res State', 'Res Zip Code', 'Res County Code', 'Phone number', 'Mail Address 1',
    'Mail Address 2', 'Mail City State', 'Mail State', 'Mail ZipCode', 'ElgPer ID',
    'Benefit Begin Date', 'Benefit End Date', 'HCP Status', 'HCP Code', 'HCP Code Status',
    'Restricted Service Code (Hospice Indicator)', 'Cap AidCode', 'OHC Code',
    'Medicare Part A Status Code', 'Medicare Part B Status Code', 'Medicare Part D Status Code',
    'Prim AidCode', 'Primary ESC', 'SPEC1', 'SPEC1-ESC', 'SPEC2-AID', 'SPEC2-ESC',
    'SPEC3-AID', 'SPEC3-ESC', 'SOC Amount', 'SOC CertDay', 'ESRD Indicator','Nursing Facility Resident',
    'IMMIGRATION STATUS', 'File Type', 'File Date', 'AFS', 'CCS'
]

# --- PARSING VARIABLES ---
records = []
current_member = {}
hd_ce_blocks = []
in_member = False
current_mail_flag = False
immigration_list, phone_number = [], ""
soc_amount_list = []
soc_certday_list = []

ccs_list = []
cap_aidcode_list = []
esrd_indicator_list = []
nursing_facility_list = []
pending_hd = None
pending_ce = {}
pending_medicare_status = {}
# For SOC Amount (AMT)
captured_soc_amount = ""
soc_amount_pending = False

# For Cap AidCode (RB)
captured_cap_aidcode = ""
cap_aidcode_pending = False
current_afs = ""

current_restricted_service = ""
restricted_service_pending = False



# --- PARSING LOGIC ---
for segment in segments:
    parts = segment.split("*")
    tag = parts[0]

    if tag == "INS":
        if in_member:
            for block in hd_ce_blocks:
                combined = current_member.copy()
                combined.update(block["hd"])
                combined.update(block["ce"])
                combined["ElgPer ID"] = block["ElgPer ID"]
                combined["Medicare Part A Status Code"] = block.get("Medicare Part A Status Code", "")
                combined["Medicare Part B Status Code"] = block.get("Medicare Part B Status Code", "")
                combined["Medicare Part D Status Code"] = block.get("Medicare Part D Status Code", "")
                combined["IMMIGRATION STATUS"] = immigration_list.pop(0) if immigration_list else ""
                combined["SOC Amount"] = soc_amount_list.pop(0) if soc_amount_list else ""
                combined["SOC CertDay"] = soc_certday_list.pop(0) if soc_certday_list else ""
                combined["CCS"] = ccs_list.pop(0) if ccs_list else ""
                combined["Cap AidCode"] = cap_aidcode_list.pop(0) if cap_aidcode_list else ""
                combined["ESRD Indicator"] = esrd_indicator_list.pop(0) if esrd_indicator_list else ""
                combined["Nursing Facility Resident"] = nursing_facility_list.pop(0) if nursing_facility_list else ""
                combined["Phone number"] = phone_number
                combined["AFS"] = current_afs
                combined["File Type"] = file_type
                combined["File Date"] = file_date

                records.append(combined)
        current_member = {}
        hd_ce_blocks = []
        phone_number = ""
        current_afs = ""
        pending_hd = None
        pending_ce = {}
        pending_medicare_status = {}
        in_member = True

        if "D8" in parts:
            d8_index = parts.index("D8")
            if len(parts) > d8_index + 1:
                current_member["Death Date"] = parts[d8_index + 1]

    elif tag == "REF" and len(parts) >= 3:
        qualifier, value = parts[1], parts[2]
        if qualifier == "0F": current_member["CIN"] = value
        elif qualifier == "1L": current_member["MEDS ID"] = value
        elif qualifier == "F6": current_member["MBI"] = value
        elif qualifier == "17":
            values = value.split(";")
            if value.count(";") == 13:
                if pending_hd is not None:
                    pending_hd["OHC Code"] = values[0].strip() if len(values) > 0 else ""
                if len(values) >= 14:
                    ccs_list.append(values[-2].strip())
                    immigration_list.append(values[-1].strip())
                esrd_indicator_list.append(values[4].strip() if len(values) > 4 else "")
                nursing_facility_list.append(values[7].strip() if len(values) > 7 else "")
            
            elif value.count(";") == 3:
                current_member["Redeterm Date"] = values[0].strip()
                current_member["Expected Delivery Date"] = values[3].strip() if len(values) > 3 else ""
        elif qualifier == "23":
            values = value.split(";")
            if len(values) >= 1:
                current_afs = values[4].strip()

        elif qualifier == "XX1":
            current_restricted_service = value.strip()
            restricted_service_pending = True


        elif qualifier == "9V":
            p = value.split(";")
            pending_medicare_status = {
                "Medicare Part A Status Code": p[0].strip() if len(p) > 0 else "",
                "Medicare Part B Status Code": p[1].strip() if len(p) > 1 else "",
                "Medicare Part D Status Code": p[2].strip() if len(p) > 2 else ""
            }
        elif qualifier == "3H":
            p = value.split(";")
            current_member["Case County Code"] = p[0].strip() if len(p) > 0 else ""
            current_member["Case Aid Code"] = p[1].strip() if len(p) > 1 else ""
            current_member["Case Number"] = p[2].strip() if len(p) > 2 else ""

        elif qualifier == "QQ":
            current_member["Term Reason"] = value.split(";")[0].strip()

        elif qualifier == "RB":
            captured_cap_aidcode = value.strip()
            cap_aidcode_pending = True

        elif qualifier == "CE":
            ce = value.split(";")
            pending_ce = {
                "Prim AidCode": ce[0] if len(ce) > 0 else "",
                "Primary ESC": ce[1] if len(ce) > 1 else "",
                "SPEC1": ce[2] if len(ce) > 2 else "",
                "SPEC1-ESC": ce[3] if len(ce) > 3 else "",
                "SPEC2-AID": ce[4] if len(ce) > 4 else "",
                "SPEC2-ESC": ce[5] if len(ce) > 5 else "",
                "SPEC3-AID": ce[6] if len(ce) > 6 else "",
                "SPEC3-ESC": ce[7] if len(ce) > 7 else ""
            }
        elif qualifier == "ZZ":
            zz_parts = value.split(";")
            soc_certday = zz_parts[1].strip() if len(zz_parts) > 1 else ""
            current_elgper_id = zz_parts[2].strip() if len(zz_parts) > 2 else ""

            if pending_hd:
                # Handle SOC Amount alignment
                if soc_amount_pending:
                    soc_amount_list.append(captured_soc_amount)
                else:
                    soc_amount_list.append("")
                soc_amount_pending = False
                captured_soc_amount = ""

                # Handle Cap AidCode alignment
                if cap_aidcode_pending:
                    cap_aidcode_list.append(captured_cap_aidcode)
                else:
                    cap_aidcode_list.append("")
                cap_aidcode_pending = False
                captured_cap_aidcode = ""

                # Handle Restricted Service Code (Hospice Indicator) alignment
                if restricted_service_pending:
                    pending_hd["Restricted Service Code (Hospice Indicator)"] = current_restricted_service
                else:
                    pending_hd["Restricted Service Code (Hospice Indicator)"] = ""

                # Reset restricted service after appending
                restricted_service_pending = False
                current_restricted_service = ""


                block = {
                    "hd": pending_hd,
                    "ce": pending_ce,
                    "ElgPer ID": current_elgper_id
                }
                block.update(pending_medicare_status)

                # Append SOC CertDay **with** the block
                soc_certday_list.append(soc_certday)

                hd_ce_blocks.append(block)

                # Reset pending after appending
                pending_hd = None
                pending_ce = {}
                pending_medicare_status = {}


    if tag == "NM1":
        if parts[1] == "IL":
            current_member["Last Name"] = parts[3] if len(parts) > 3 else ""
            current_member["First Name"] = parts[4] if len(parts) > 4 else ""
            current_member["MI"] = parts[5] if len(parts) > 5 else ""

        elif parts[1] == "31": current_mail_flag = True

    elif tag == "DMG" and parts[1] == "D8":
        current_member["DOB"] = parts[2]
        current_member["Gender"] = parts[3]
        current_member["Ethnicity"] = parts[5].lstrip(":") if len(parts) > 5 else ""

    elif tag == "AMT" and len(parts) > 2 and parts[1] == "R":
        captured_soc_amount = parts[2]
        soc_amount_pending = True


    elif tag == "PER" and parts[1] == "IP":
        for i in range(2, len(parts) - 1):
            if parts[i] == "TE": phone_number = parts[i + 1]

    elif tag == "N3":
        if current_mail_flag:
            current_member["Mail Address 1"] = parts[1]
            current_member["Mail Address 2"] = parts[2] if len(parts) > 2 else ""
        else:
            current_member["Res Address 1"] = parts[1]
            current_member["Res Address 2"] = parts[2] if len(parts) > 2 else ""

    elif tag == "N4":
        if current_mail_flag:
            current_member["Mail City State"] = parts[1]
            current_member["Mail State"] = parts[2]
            current_member["Mail ZipCode"] = parts[3]
            current_mail_flag = False
        else:
            current_member["Res City"] = parts[1]
            current_member["Res State"] = parts[2]
            current_member["Res Zip Code"] = parts[3]
            if len(parts) > 5 and parts[5] == "CY":
                current_member["Res County Code"] = parts[6]

    elif tag == "HD":
        pending_hd = {"HCP Status": parts[1]}
        if len(parts) > 4:
            hcp = parts[4].split(";")
            pending_hd["HCP Code"] = hcp[0]
            pending_hd["HCP Code Status"] = hcp[1] if len(hcp) > 1 else ""


    elif tag == "DTP":
        if pending_hd:
            if parts[1] == "348": pending_hd["Benefit Begin Date"] = parts[3]
            elif parts[1] == "349": pending_hd["Benefit End Date"] = parts[3]

    elif tag == "LUI":
        if parts[1] == "LD":
            current_member["Language1"] = parts[2]
        elif len(parts) > 3:
            current_member["Language2"] = parts[3]

# --- FINALIZE LAST MEMBER ---
if in_member:
    for block in hd_ce_blocks:
        combined = current_member.copy()
        combined.update(block["hd"])
        combined.update(block["ce"])
        combined["ElgPer ID"] = block["ElgPer ID"]
        combined["Cap AidCode"] = block.get("Cap AidCode", "")
        combined["Medicare Part A Status Code"] = block.get("Medicare Part A Status Code", "")
        combined["Medicare Part B Status Code"] = block.get("Medicare Part B Status Code", "")
        combined["Medicare Part D Status Code"] = block.get("Medicare Part D Status Code", "")
        combined["IMMIGRATION STATUS"] = immigration_list.pop(0) if immigration_list else ""
        combined["SOC Amount"] = soc_amount_list.pop(0) if soc_amount_list else ""
        combined["SOC CertDay"] = soc_certday_list.pop(0) if soc_certday_list else ""
       
        combined["CCS"] = ccs_list.pop(0) if ccs_list else ""
        combined["Cap AidCode"] = cap_aidcode_list.pop(0) if cap_aidcode_list else ""
        combined["ESRD Indicator"] = esrd_indicator_list.pop(0) if esrd_indicator_list else ""
        combined["Nursing Facility Resident"] = nursing_facility_list.pop(0) if nursing_facility_list else ""
        combined["Phone number"] = phone_number
        combined["AFS"] = current_afs
        combined["File Type"] = file_type
        combined["File Date"] = file_date
        records.append(combined)

# --- FINAL OUTPUT ---
# --- CREATE DATAFRAME AND ENSURE ALL FIELDS ARE PRESENT ---
df = pd.DataFrame(records).fillna("")   # Fill NaN right here

# Add any missing columns from expected_fields
for col in expected_fields:
    if col not in df.columns:
        df[col] = ""

# Reorder columns to match expected output structure
df = df[expected_fields]

# --- GENERATE OUTPUT FILES ---

# 1. Save as CSV
df.to_csv(output_csv, index=False)

# 2. Save as Pipe-Delimited File (with header)
df.to_csv(output_pipe, sep="|", index=False, header=True)

# 3. Create Cleaned Pipe-Delimited File (removing trailing pipes if values are empty)
with open(cleaned_pipe_file, "w") as f:
    f.write("|".join(expected_fields) + "\n")
    for _, row in df.iterrows():
        line = "|".join(str(val) for val in row.tolist())
        # Remove trailing empty fields (pipes) safely
        line = line.rstrip("|")
        f.write(line + "\n")

# --- SUMMARY LOG ---
print(f"Total Records Parsed: {len(df)}")
print(f"CSV Output Generated: {output_csv}")
print(f"Pipe-delimited Output Generated: {output_pipe}")
print(f"Cleaned Pipe-delimited Output Generated: {cleaned_pipe_file}")
