import io
import json
import pathlib
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import pypdf
import pandas as pd
import requests
import tabula

# Read the links from the JSON file using a context manager
with open("links.json") as f:
    links_data = json.load(f)
links = links_data["links"]

# Initialize the result dictionary
result_by_year = {}


def process_link(link):
    for year, url in link.items():
        try:
            # Read the PDF file from the URL using requests library
            response = requests.get(url)
            response.raise_for_status()
            pdf_file = io.BytesIO(response.content)

            # Create a pypdf PdfFileReader object from the PDF file
            pdf_reader = pypdf.PdfReader(pdf_file)

            # Extract the table data from the first page of the PDF using tabula-py
            tables = tabula.read_pdf(url, pages=1, pandas_options={"header": None})

            # Convert the table data to a pandas DataFrame
            df = pd.DataFrame(tables[0])

            # Convert the DataFrame to a list of dictionaries
            table_data = df.to_dict(orient="records")

            # Create a dictionary with the key-value pairs
            result = []
            for row in table_data:
                row[1] += f" {year}"
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
        except requests.RequestException as e:
            print(f"Failed to fetch {url}: {e}")
        except Exception as e:
            print(f"Error processing {url}: {e}")


# Use ThreadPoolExecutor to process links concurrently
with ThreadPoolExecutor() as executor:
    executor.map(process_link, links)

# Sort the result by year
sorted_result_by_year = dict(sorted(result_by_year.items()))

# Create the data folder if it does not exist using pathlib library
pathlib.Path("data").mkdir(parents=True, exist_ok=True)

# Write the output to a JSON file in the data folder using pathlib library
with open(pathlib.Path("data") / "public-holidays.json", "w") as outfile:
    json.dump(sorted_result_by_year, outfile, indent=4)
