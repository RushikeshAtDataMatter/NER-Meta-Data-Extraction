from collections import Counter
import pycountry
from geotext import GeoText
import phonenumbers
import spacy
from datetime import datetime
import dateparser
import re
import chardet
from bs4 import BeautifulSoup
import pgeocode
import pandas as pd
import os
# print(os.listdir("D:\FastAPI\metadata\helper\CustomRegexModelNER"))
nlp = spacy.load("D:/FastAPI/metadata/helper/CustomRegexModelNER")

def get_entities(column, df,sample_size=10):
    entities = []
    non_empty_sample = df[column].dropna().astype(str)
    sample_size = min(sample_size, len(non_empty_sample))  # Adjust sample size
    sample = non_empty_sample.sample(sample_size, random_state=42)

    for text in sample:
        text = text.strip()
        doc = nlp(text)
        entities.extend(ent.label_ for ent in doc.ents)
    # print(entities)
    most_common_entities = Counter(entities).most_common(1)
    return most_common_entities[0] if most_common_entities else None


# Define boolean variables
is_city = False
is_state = False
is_country = False

def classify_location(df, column):
    global is_city, is_state, is_country  
    
    sample = df[column].dropna().sample(n=min(10, len(df[column])), random_state=42)

    city_results = []
    state_results = []
    country_results = []

    def is_state_pycountry(location_name):
        
        for subdivision in pycountry.subdivisions:
            if location_name.lower() in subdivision.name.lower():
                return True
        return False

    for location_name in sample:
        try:
           
            places = GeoText(location_name)
            city_results.append(bool(places.cities))  # True if it's a city
            country_results.append(bool(places.countries))  # True if it's a country

            
            state_results.append(is_state_pycountry(location_name))

        except Exception as e:
            # print(f"Error processing location '{location_name}': {e}")
            city_results.append(False)
            state_results.append(False)
            country_results.append(False)

    is_city = max(set(city_results), key=city_results.count)
    is_state = max(set(state_results), key=state_results.count)
    is_country = max(set(country_results), key=country_results.count)


    
# List of date formats
date_formats = [
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%m.%d.%Y",
    "%d-%m-%Y",  # DD-MM-YYYY
    "%Y-%m-%d",  # YYYY-MM-DD
    "%d/%m/%Y",  # DD/MM/YYYY
    "%Y/%m/%d",  # YYYY/MM/DD
    "%d.%m.%Y",  # DD.MM.YYYY
    "%Y.%m.%d",  # YYYY.MM.DD
    "%d %B %Y",  # DD MMMM YYYY
    "%d %b %Y",  # DD MMM YYYY
    "%B %d, %Y",  # MMMM DD, YYYY
    "%b %d, %Y",  # MMM DD, YYYY
    "%d %B, %Y",  # DD MMMM, YYYY
    "%d %b, %Y",  # DD MMM, YYYY
    "%B %d %Y",  # MMMM DD YYYY
    "%b %d %Y",  # MMM DD YYYY
    "%d %B %y",  # DD MMMM YY
    "%d %b %y",  # DD MMM YY
    "%B %d, %y",  # MMMM DD, YY
    "%b %d, %y",  # MMM DD, YY
    "%d %B, %y",  # DD MMMM, YY
    "%d %b, %y",  # DD MMM, YY
    "%B %d %y",  # MMMM DD YY
    "%b %d %y",  # MMM DD YY
    "%d-%m-%y",  # DD-MM-YY
    "%y-%m-%d",  # YY-MM-DD
    "%d/%m/%y",  # DD/MM/YY
    "%y/%m/%d",  # YY/MM/DD
    "%d.%m.%y",  # DD.MM.YY
    "%y.%m.%d",  # YY.MM.DD
]

is_date = False
def detect_date_format(df, column):
    sample = df[column].dropna().sample(n=min(10, len(df[column])), random_state=1)
    for date in sample:
        for fmt in date_formats:
            try:
                datetime.strptime(date, fmt)
                is_date = True
                return fmt  
            except ValueError:
                continue
    return None  


def is_address_line_column(column, df):
  
    results = []
    non_empty_sample = df[column].dropna().astype(str)
    if non_empty_sample.empty:
        # print(f"Column {column} has no non-empty entries to evaluate.")
        return False
    
    sample_size = min(20, len(non_empty_sample))
    sample = non_empty_sample.sample(sample_size, random_state=42)

    for text in sample:
        doc = nlp(text.strip())
        is_address = any(
            ent.label_ in ["GPE", "LOC", "FAC", "ADDRESS"] for ent in doc.ents
        )

        if not is_address:
            address_keywords = ["street", "road", "avenue", "lane", "city", "village", "block", "apartment", "house"]
            is_address = any(keyword.lower() in text.lower() for keyword in address_keywords)
        
        results.append(is_address)

    true_count = results.count(True)
    false_count = results.count(False)
    return true_count > false_count

# # is_duration_column
# def is_duration_column(column, df):
#     results = []
#     sample = df[column].dropna().sample(n=min(10, len(df[column])), random_state=1)
#     for text in sample:
#         duration = dateparser.parse(text, settings={"PREFER_DATES_FROM": "future"})
#         if duration is not None:
#             results.append(True)
#         else:
#             results.append(False)

#         most_common = Counter(results).most_common(1)[0]
#     return most_common

# is_noun 
def is_noun(df, column):
    is_noun_detected = False
    if df[column].dtype != "int64" and df[column].dtype != "float64":
        non_empty_values = df[column].dropna().astype(str).str.strip()
        if non_empty_values.empty:
            return False
        sample_size = min(10, len(non_empty_values))
        sample = non_empty_values.sample(sample_size, random_state=42)
        for value in sample:
            doc = nlp(value)
            if any(token.pos_ == "NOUN" for token in doc):
                is_noun_detected = True
                break
    return is_noun_detected


def analyze_text_case(column_values):
    if not all(isinstance(value, str) for value in column_values if pd.notna(value)):
        return None, None  

    cases = {
        "UPPERCASE": 0,
        "lowercase": 0,
        "Title Case": 0,
        "Mixed Case": 0,
        "Camel Case": 0,
        "Snake Case": 0,
        "Kebab Case": 0,
        "Pascal Case": 0,
    }
    is_case_sensitive = False
    
    # Regex patterns for different case styles
    camel_case_pattern = r"^[a-z]+([A-Z][a-z0-9]+)+$"
    snake_case_pattern = r"^[a-z0-9]+(_[a-z0-9]+)+$"
    kebab_case_pattern = r"^[a-z0-9]+(-[a-z0-9]+)+$"
    pascal_case_pattern = r"^[A-Z][a-z0-9]+([A-Z][a-z0-9]+)*$"
    
    for value in column_values:
        if isinstance(value, str):  
            if value.isupper():
                cases["UPPERCASE"] += 1
            elif value.islower():
                cases["lowercase"] += 1
            elif value.istitle():
                cases["Title Case"] += 1
            elif re.match(camel_case_pattern, value):
                cases["Camel Case"] += 1
            elif re.match(snake_case_pattern, value):
                cases["Snake Case"] += 1
            elif re.match(kebab_case_pattern, value):
                cases["Kebab Case"] += 1
            elif re.match(pascal_case_pattern, value):
                cases["Pascal Case"] += 1
            else:
                cases["Mixed Case"] += 1
                

    unique_values = set(column_values)
    lower_unique_values = set(val.lower() for val in column_values if isinstance(val, str))
    is_case_sensitive = len(unique_values) != len(lower_unique_values)
    
    predominant_case = max(cases, key=cases.get) if sum(cases.values()) > 0 else "Unknown"
    return is_case_sensitive, predominant_case



def detect_column_encoding(df, column, sample_size=10):
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in the DataFrame.")

    if df[column].dtype != "object":
        raise ValueError(f"Column '{column}' is not a text (object) column.")
    
    # Drop NaN values and sample up to the specified sample size
    sample_values = df[column].dropna().sample(min(sample_size, len(df[column])), random_state=42)

    # Detect encoding for each sample value
    encodings = []
    for value in sample_values:
        raw_data = str(value).encode('utf-8', 'ignore')  # Convert string to bytes
        result = chardet.detect(raw_data)
        encodings.append(result.get('encoding', 'unknown'))

    # Find the most common encoding
    most_common_encoding = Counter(encodings).most_common(1)[0][0]
    return most_common_encoding[0] if most_common_encoding else None






def is_html_in_column(df, column, sample_size=10):
    if column not in df.columns:
        raise ValueError(f"Column '{column}' not found in the DataFrame.")
    sample = df[column].dropna().sample(n=min(sample_size, len(df[column])), random_state=42)

    def is_html(text):
        try:
            soup = BeautifulSoup(text, "html.parser")
            return bool(soup.find())
        except Exception:
            return False
    results = [is_html(str(value)) for value in sample]
    counts = Counter(results)
    return counts[True] > counts[False]



def is_valid_pincode(df, country, column, sample_size=10):
    picodelist = []
    sample = df[column].dropna().sample(n=min(sample_size, len(df[column])), random_state=42)
    
    nomi = pgeocode.Nominatim(country)  # Initialize once outside the loop for efficiency

    for value in sample:
        value = str(value)
        location = nomi.query_postal_code(value)
        if not location.empty and not pd.isna(location.latitude):
            picodelist.append(True)
        else:
            picodelist.append(False)
        most_common_pincode = Counter(picodelist).most_common(1)[0]
    return most_common_pincode[0] if most_common_pincode else None




def is_phone(df, column, country, sample_size=10):
    sample = df[column].dropna().sample(n=min(sample_size, len(df[column])), random_state=42)
    phonelist = []

    for value in sample:
        try:
            value = str(value)  
            parsed_number = phonenumbers.parse(value, country)
    
            e164_number = phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.E164)
           
            parsed_e164 = phonenumbers.parse(e164_number)
            if phonenumbers.is_valid_number(parsed_e164):
                phonelist.append(True)
            else:
                phonelist.append(False)
        except phonenumbers.NumberParseException:
            phonelist.append(False)
    most_common_phone = Counter(phonelist).most_common(1)[0]
    return most_common_phone[0] if most_common_phone else None