import urllib.request
import io
import json
import PyPDF2
import tabula
import pandas as pd
import os
from datetime import datetime

# Set the URL of the remote PDF file
url = "https://pmo.govmu.org/Communique/Public_Holidays_2023.pdf"

# Read the PDF file from the URL
response = urllib.request.urlopen(url)
pdf_file = io.BytesIO(response.read())

# Create a PyPDF2 PdfFileReader object from the PDF file
pdf_reader = PyPDF2.PdfReader(pdf_file)

# Get the number of pages in the PDF file
num_pages = len(pdf_reader.pages)

# Extract the table data from the first page of the PDF using tabula-py
tables = tabula.read_pdf(url, pages=1, pandas_options={"header": None})

# Set the headers for the table data
headers = ["name", "date"]

cols = tables[0].values.tolist()[0]

# Convert the table data to a pandas DataFrame
df = pd.DataFrame(tables[0])

# Convert the DataFrame to a list of dictionaries
table_data = df.to_dict(orient="records")

# Create a dictionary with the key-value pairs
# Create a list of dictionaries with the holiday name and date
result = []
# result.append(dict(zip(headers, headers)))
for row in table_data:
    row[1] += " "
    row[1] += str(datetime.now().year)
    result.append(
        {
            "name": row[0].title(),
            "date": datetime.strptime(row[1], "%A %d %B %Y").strftime("%Y-%m-%d"),
        }
    )

# Create the data folder if it does not exist
if not os.path.exists("data"):
    os.makedirs("data")

# Write the output to a JSON file in the data folder
with open("data/public-holidays.json", "w") as outfile:
    json.dump(result, outfile)
