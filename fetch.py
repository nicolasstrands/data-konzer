import os
import io
import json
import pathlib
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

import requests
import pypdf
import pandas as pd
import tabula
import openai
from bs4 import BeautifulSoup

# Load links from a JSON file
with open("links.json") as f:
    links_data = json.load(f)

# Initialize the result dictionary
result_by_country = {}


# Function to check if the year already exists in the JSON file
def year_exists_in_file(country, year):
    file_path = pathlib.Path("data") / f"public-holidays-{country}.json"
    if not file_path.exists():
        return False
    with open(file_path, "r") as file:
        data = json.load(file)
        return str(year) in data


# Fetch webpage content
def fetch_webpage_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching the webpage: {e}")
        return None


def extract_relevant_text(html_content, html_selector, html_attribute, html_value):
    """
    Parses the HTML content and extracts the relevant text containing public holidays.
    Adjust the parsing logic based on the webpage structure.
    """
    soup = BeautifulSoup(html_content, "html.parser")

    # Example: Assuming public holidays are listed within a specific table or div
    # You'll need to inspect the actual webpage to identify the correct tags and classes/ids
    holidays_section = soup.find(
        html_selector, {html_attribute: html_value}
    )  # Modify as needed

    if not holidays_section:
        print("Could not find the holidays section in the webpage.")
        sys.exit(1)

    text_content = holidays_section.get_text(separator="\n")
    print("Successfully extracted relevant text from the webpage.")
    return text_content


# Extract holidays using OpenAI
def extract_holidays_with_openai(content, country, year):
    openai.api_key = os.getenv("SPECIAL_OPENAI_KEY")
    if not openai.api_key:
        print("Error: SPECIAL_OPENAI_KEY environment variable not set.")
        sys.exit(1)

    prompt = f"""
    Extract the public holidays for {country} in the year from the following text.
    Provide the data in JSON format with the structure:
    {{
        "{year}": [
            {{
                "name": "Holiday Name",
                "date": "YYYY-MM-DD",
            }},
            ...
        ]
    }}
    
    Here is the text:
    {content}
    """

    try:
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a helpful assistant that extracts public holidays from text.",
                },
                {"role": "user", "content": prompt},
            ],
            max_tokens=2000,
            temperature=0,
        )

        return response["choices"][0]["message"]["content"]
    except openai.error.OpenAIError as e:
        print(f"OpenAI API error: {e}")
        return None


# Validate and load JSON
def validate_and_load_json(json_str):
    try:
        json_start = json_str.find("{")
        json_end = json_str.rfind("}") + 1
        json_clean = json_str[json_start:json_end]
        return json.loads(json_clean)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None


# Process each link (PDF or webpage)
def process_link(country, link_info, year):
    # for link_info in link.items():
    if country == "mu":
        country_name = "Mauritius"
    elif country == "fr":
        country_name = "France"
    elif country == "za":
        country_name = "South Africa"
    elif country == "sg":
        country_name = "Singapore"

    print(f"Processing link for {country_name} in {year}...")
    url = link_info["url"]
    link_type = link_info["type"]
    try:
        if link_type == "pdf":
            response = requests.get(url)
            response.raise_for_status()
            pdf_file = io.BytesIO(response.content)
            pdf_reader = pypdf.PdfReader(pdf_file)
            tables = tabula.read_pdf(url, pages=1, pandas_options={"header": None})
            df = pd.DataFrame(tables[0])
            table_data = df.to_dict(orient="records")

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

            if country not in result_by_country:
                result_by_country[country] = {}
            if year not in result_by_country[country]:
                result_by_country[country][year] = []
            result_by_country[country][year].extend(result)

        elif link_type == "webpage":
            content = fetch_webpage_content(url)
            if content:
                relevant_text = extract_relevant_text(
                    content, link_info["tag"], link_info["attr"], link_info["value"]
                )
                if relevant_text:
                    print(f"Extracting holidays for {country} in {year}...")
                    extracted_json_str = extract_holidays_with_openai(
                        relevant_text, country_name, year
                    )
                    data = validate_and_load_json(extracted_json_str)
                    if data:
                        if country not in result_by_country:
                            result_by_country[country] = {}
                        if year not in result_by_country[country]:
                            result_by_country[country][year] = []
                        result_by_country[country][year].extend(data[year])

    except Exception as e:
        print(f"Error processing {url}: {e}")


# Process links sequentially
for country, links in links_data["countries"].items():
    print(f"Processing links for {country}...")
    for link in links:
        for year, link_info in link.items():
            # print link_info as json
            print(json.dumps(link_info, indent=4))

            if year_exists_in_file(country, year):
                print(f"Skipping {country} for year {year} as it already exists.")
                continue
            process_link(country, link_info, year)

# Create the data folder and save output
pathlib.Path("data").mkdir(parents=True, exist_ok=True)
for country, data in result_by_country.items():
    sorted_result_by_year = dict(sorted(data.items()))
    with open(pathlib.Path("data") / f"public-holidays-{country}.json", "w") as outfile:
        json.dump(sorted_result_by_year, outfile, indent=4)
