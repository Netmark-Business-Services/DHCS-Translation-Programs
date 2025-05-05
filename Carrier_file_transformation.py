import json
def parse_carrier_line(line):
    return [
        line[0:4].strip(),     
        line[4:12].strip(),    
        line[12:13].strip(),   
        line[13:14].strip(),   
        line[14:15].strip(),   
        line[15:16].strip(),   
        line[55:65].strip(),   
        line[65:76].strip(),
        line[76:80].strip(),
        line[80:81].strip(),   
        line[81:97].strip(),   
        line[97:100].strip(),  
        line[100:105].strip(), 
        line[105:106].strip(), 
        line[106:156].strip(), 
        line[156:206].strip(), 
        line[206:226].strip(), 
        line[226:228].strip(), 
        line[228:237].strip(), 
        line[237:267].strip(), 
        line[267:277].strip(), 
        line[277:281].strip(), 
        line[281:311].strip(), 
        line[311:341].strip(), 
        line[341:355].strip(), 
    ]

column_headers = [
    "Carrier Code",
    "Last Change Date",
    "Status Code",
    "Electronic Billing Flag",
    "Electronic Response Flag",
    "OHC Code",
    "Carrier Name 1",
    "Carrier Name 2",
    "Refer to Carrier",
    "Multiple Carrier Indicator",
    "Scope of Coverage",
    "Operator ID",
    "Filler 1",
    "Top 100 Carrier Code",
    "Carrier Address Line 1",
    "Carrier Address Line 2",
    "Carrier Address City",
    "Carrier Address State",
    "Carrier Address Zip Code",
    "Carrier Address Attention",
    "Carrier Phone Number",
    "Carrier Phone Number Extension",
    "Footnote Line 1",
    "Footnote Line 2",
    "Filler 2"
]

# Load config.json
with open("config.json", "r") as f:
    config = json.load(f)

input_file = config['carrier_input_file']  # Path to the input file
output_file = config['carrier_output_file']  # Path to the output file

with open(input_file, "r") as infile, open(output_file, "w") as outfile:
    outfile.write("|".join(column_headers) + "\n")

    for line in infile:
        fields = parse_carrier_line(line)
        outfile.write("|".join(fields) + "\n")

print(f"Done")