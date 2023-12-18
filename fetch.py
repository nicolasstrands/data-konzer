import io
import json
import pathlib
import PyPDF2
import pandas as pd
import requests
import tabula
from datetime import datetime

# Read the links from the JSON file using a context manager
with open("links.json") as f:
    links_data = json.load(f)
links = links_data["links"]

# Initialize the result dictionary
result_by_year = {}

# Loop through each link
for link in links:
    for year, url in link.items():
        # Read the PDF file from the URL using requests library
        response = requests.get(url)
        pdf_file = io.BytesIO(response.content)

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
        result = []
        for row in table_data:
            row[1] += " "
            row[1] += str(year)
            result.append(
                {
                    "name": row[0].title(),
                    "date": datetime.strptime(row[1], "%A %d %B %Y").strftime(
                        "%Y-%m-%d"
                    ),
                }
            )

        # Group the result by year
        if year not in result_by_year:
            result_by_year[year] = []
        result_by_year[year].extend(result)

# Create the data folder if it does not exist using pathlib library
pathlib.Path("data").mkdir(parents=True, exist_ok=True)

# Write the output to a JSON file in the data folder using pathlib library
with open(pathlib.Path("data") / "public-holidays.json", "w") as outfile:
    json.dump(result_by_year, outfile)