from metadata.helper.Custom import *
from metadata.helper.Profiling import *
import json

  # Fallback to an empty dictionary


def analyze_csv_data(df, report_data,country_code):
    results = {}
    for column in df.columns:

        # get entites
        entites = "" 
        if get_entities(column, df,sample_size=10):
            entites = get_entities(column, df,sample_size=10)
        else:
            entites = ""

        is_email = entites == "EMAIL"
        is_link = entites == "URL"
        entity = entites

        # Date & date format
        is_date = False  
        if df[column].dtype == "object":
            date_format = detect_date_format(df, column)
            if date_format is not None:
                is_date = True
        else:
            date_format = None

        # text case
        column_values = df[column].dropna().tolist()
        is_case_sensitive, predominant_case = analyze_text_case(column_values)
    

        # text encoding
        encoding=""
        if df[column].dtype == "object":
            encoding = detect_column_encoding(df, column)

        
    
        results= {
            #profiling
            "name":names(column),
            "datatype":dataTypes(df ,column),
            "is_unique":is_unique(report_data, column),
            "unique_values_count":unique_values_count(report_data, column),
            "null_count":null_count(report_data, column),
            "mean_value":mean_value(report_data, column),
            "max_value":max_value(report_data, column),
            "median_value":median_value(report_data, column),
            "mode_value":mode_value(df, column),
            "is_float":is_float(df, column),
            "is_integer":is_integer(df, column),
            "data_density":data_density(column ,report_data),
            "data_count":data_count(report_data, column),
            "is_multipicklist":is_multipicklist(report_data, column),
            "data_trend":data_trend(report_data, column),
            "data_distribution":data_distribution(df,report_data, column),
            "data_quality":data_quality(report_data, column),
            "byte_length":byte_length(df, column),
            "lt_mean_count":lt_mean_count(df, column, report_data),
            "gt_mean_count":gt_mean_count(df, column, report_data),
            "eq_mean_count":eq_mean_count(df, column, report_data),
            "lt_median_count":lt_median_count(df, column, report_data),
            "eq_median_count":eq_median_count(df, column, report_data),
            "eq_zero_count":eq_zero_count(df, column),
            "gt_median_count":gt_median_count(df, column, report_data),
            "lt_zero_count":lt_zero_count(df, column),
            "max_length":max_length (report_data, column),
            "min_length":min_length (report_data, column),
            "is_fixed_length":is_fixed_length(report_data, column),
            "is_required":is_required(report_data, column),
            "data_trend_direction": data_trend_direction(report_data,column),
            #custom
            "is_city":is_city,
            "is_country":is_country,
            "is_state":is_state,
            "is_date":is_date,
            "date_format":date_format,
            "is_address_line":is_address_line_column(column, df),
            "is_noun":is_noun,
            "is_case_sensitive":is_case_sensitive,
            "text_case":predominant_case,
            # "analyze_text_case":analyze_text_case(column_values),
            "encoding":encoding,
            "is_html":is_html_in_column(df, column, sample_size=10),
            "is_pincode":is_valid_pincode(df, country_code, column, sample_size=10),
            "is_phone": is_phone(df, column, country_code, sample_size=10),
            "is_email": is_email,
            "is_link": is_link,
            "entity": entity,
           
        }
    return results




