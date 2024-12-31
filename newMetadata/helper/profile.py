from fastapi import HTTPException
from sqlalchemy.orm import Session
from newMetadata.schema.new_metadata import newMetaData
import os 
import json
from newMetadata.helper.custome import *

def extract_and_store_metadata(json_path: str, db: Session, df):
    if not os.path.exists(json_path):
        raise HTTPException(status_code=404, detail="Report file not found.")

    # Read the report.json file
    with open(json_path, 'r') as file:
        report_data = json.load(file)

    for column in df.columns:
        datatype = str(df[column].dtype)

        # Calculate byte lengths for string columns
        if df[column].dtype == 'object':
            byte_lengths = df[column].dropna().apply(lambda x: len(x.encode('utf-8')))
            avg_byte_length = byte_lengths.mean() if not byte_lengths.empty else None
            max_byte_length = byte_lengths.max() if not byte_lengths.empty else None
            max_byte_length = str(max_byte_length)

        else:
            avg_byte_length = None
            max_byte_length = None

        # Null check
        is_null = report_data['variables'][column].get('n') == report_data['variables'][column].get('n_missing')

        # Distribution determination
        if df[column].dtype in ['int64', 'float64']:
            kurtosis = report_data['variables'][column].get('kurtosis', 0)
            skewness = report_data['variables'][column].get('skewness', 0)
            p_value = report_data['variables'][column].get('chi_squared', {}).get('p_value', 0)

            if abs(skewness) < 0.5 and -1 <= kurtosis <= 1:
                data_distribution = "Normal"
            elif p_value > 0.05:
                data_distribution = "Uniform"
            else:
                data_distribution = "Skewed"
        else:
            p_value = None
            data_distribution = None

        # Data trend determination
        monotonic_increase = report_data['variables'][column].get('monotonic_increase')
        monotonic_increase_strict = report_data['variables'][column].get('monotonic_increase_strict')
        if monotonic_increase is True:
            data_trend = "monotonic increase"
        elif monotonic_increase is False:
            data_trend = "monotonic decrease"
        elif monotonic_increase_strict is True:
            data_trend = "strictly monotonic increase"
        elif monotonic_increase_strict is False:
            data_trend = "strictly monotonic decrease"
        else:
            data_trend = None

        # Picklist determination
        word_count = report_data['variables'][column].get('word_counts', {})
        if isinstance(word_count, dict) and len(word_count) <= 11:
            picklist = list(word_count.keys())
            is_picklist = True
        else:
            picklist = []
            is_picklist = False

        # Data density calculation
        n = report_data['variables'][column].get('n', 0)
        n_missing = report_data['variables'][column].get('n_missing', 0)
        data_density = ((n - n_missing) / n) * 100 if n > 0 else None

        # Data quality calculation
        bad_data_percentage = n_missing / n if n > 0 else 0
        data_quality = 100 - (bad_data_percentage * 100)

        # custome
        entity = get_entities(column, df, nlp, sample_size=10)

        # Main logic for each column
        is_date = False
        date_format = None
# Check if the column is a date
        if df[column].dtype == "object":
            date_format = detect_date_format(df, column)
        if date_format:
            is_date = True
        else:
            date_format = ""
        
        is_address=is_address_line_column(column , df , nlp)


        # entites by NER
    # Get entities by NER
        label_counts = get_entities(column, df, nlp)
        if label_counts:
            most_common_entity = label_counts
        else:
            most_common_entity = None  # Handle no entities detected

        # Check for specific entity types
        is_email = most_common_entity == "EMAIL"
        is_link = most_common_entity == "URL"

        # is_noun
        is_noun_result = is_noun(df, column)

           # Analyze text case and sensitivity
        column_values = df[column].dropna().tolist()
        is_case_sensitive, predominant_case = analyze_text_case(column_values)

        # text encoding 
        encoding = ""
        if df[column].dtype == "object":
            encoding = detect_column_encoding(df, column)


        # is_html
        is_majority_html = is_html_in_column(df, column)
            
        # pinCode
        result = is_valid_pincode(df, "IN", column, sample_size=10)
        most_commonn_pincode = result.most_common(1)[0][0]

        # phone number 
        phone_number = is_phone(df, column)
        most_common_phone = phone_number.most_common(1)[0][0]
        
        is_city,is_country,is_state=classify_location(df, column)
        # Create metadata record
        metadata_record = newMetaData(
            name=column,
            datatype=datatype,
            is_unique=report_data['variables'][column].get('is_unique', None),
            unique_values_count=report_data['variables'][column].get('n_unique', None),
            null_count=report_data['variables'][column].get('n_missing', None),
            mean_value=report_data['variables'][column].get('mean', None),
            standard_deviation=report_data['variables'][column].get('std', None),
            min_value=report_data['variables'][column].get('min', None),
            max_value=report_data['variables'][column].get('max', None),
            median_value=report_data['variables'][column].get('median', None),
            is_float=(datatype == 'float64'),
            is_integer=(datatype == 'int64'),
            data_density=data_density,
            is_picklist=is_picklist,
            picklist_values=picklist,
            data_trend=data_trend,
            data_distribution=data_distribution,
            data_quality=data_quality,
            byteLength=max_byte_length,
            is_null=is_null,

            lt_mean_count = str(sum(1 for value in df[column] if isinstance(value, (int, float)) and value < report_data['variables'][column].get('mean', 0))),
            gt_mean_count=str(sum(1 for value in df[column] if isinstance(value, (int, float)) and value > report_data['variables'][column].get('mean', 0))),
            eq_mean_count =str(sum(1 for value in df[column] if isinstance(value, (int, float)) and value == report_data['variables'][column].get('mean', 0))),
            lt_median_count = str(sum(1 for value in df[column] if isinstance(value, (int, float)) and value < report_data['variables'][column].get('median', 0))),
            eq_median_count = str(sum(1 for value in df[column] if value == report_data['variables'][column].get('median', 0))),
            eq_zero_count = report_data['variables'][column].get('n_zeros', 0),
            gt_median_count = str(sum(1 for value in df[column] if isinstance(value, (int, float)) and value > report_data['variables'][column].get('median', 0))),
            lt_zero_count=str(sum(1 for value in df[column] if isinstance(value, (int, float)) and value < 0)), 

            max_length= report_data['variables'][column].get('max_length', None),
            min_length= report_data['variables'][column].get('min_length', None),

            is_fixed_length = report_data['variables'][column].get('min_length') == report_data['variables'][column].get('max_length'),
            is_required=report_data['variables'][column].get('n_missing') == 0,
            data_trend_direction = "Positive" if report_data['variables'][column].get('monotonic_increase') else "Negative",

# custome 
            entity = entity,
            is_city = is_city,
            is_country=is_country,
            is_state=is_state,
            is_date=is_date,
            date_format=date_format,
            is_address_line=is_address,
            is_email=is_email,
            is_link=is_link,    
            is_noun=is_noun_result,
            is_case_sensitive=is_case_sensitive,
            text_case=predominant_case,
            text_encoding=encoding,
            is_html=is_majority_html,
            is_pincode=most_commonn_pincode,
            is_phone=most_common_phone,

        
        )
        # Add record to session
        db.add(metadata_record)

    # Commit the session
    db.commit()
