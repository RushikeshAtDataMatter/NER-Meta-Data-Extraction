import pandas as pd
import numpy as np
from typing import Union, Dict

def names(column):
    return str(column)

def dataTypes(df, column):
    return str(df[column].dtypes)

def is_unique(report_data, column):
    return str(report_data['variables'].get(column, {}).get('is_unique', False))

def unique_values_count(report_data, column):
    return str(report_data['variables'].get(column, {}).get('n_unique', 0))

def null_count(report_data, column):
    return str(report_data['variables'].get(column, {}).get('n_missing', 0))

def mean_value(report_data, column):
    return str(float(report_data['variables'].get(column, {}).get('mean', 0)))

def max_value(report_data, column):
    return str(report_data['variables'].get(column, {}).get('max', 0))

def median_value(report_data, column):
    return str(report_data['variables'].get(column, {}).get('median_length', None))

def mode_value(df, column):
    mode = df[column].mode()
    return str(mode.iloc[0]) if df[column].dtype in ['int64', 'float64'] and not mode.empty else str(None)

def is_float(df, column):
    return str(df[column].dtype in ['float64'])

def is_integer(df, column):
    return str(df[column].dtype in ['int64'])

def data_density(column, report_data):
    column_data = report_data['variables'].get(column, {})
    n = column_data.get('n', 0)
    n_missing = column_data.get('n_missing', 0)
    if n > 0:
        return str(((n - n_missing) / n) * 100)
    return str(None)

def data_count(report_data, column):
    column_data = report_data['variables'].get(column, {})
    n = column_data.get('n', 0)
    n_missing = column_data.get('n_missing', 0)
    return str(n - n_missing)

def is_multipicklist(report_data, column):
    multipicklist = report_data['variables'].get(column, {}).get('first_rows', {})
    for value in multipicklist.values():
        if isinstance(value, str) and ',' in value:
            return str(True)
    return str(False)

def data_trend(report_data, column):
    column_data = report_data['variables'].get(column, {})
    if column_data.get('monotonic_increase'):
        return "monotonic increase"
    if not column_data.get('monotonic_increase'):
        return "monotonic decrease"
    if column_data.get('monotonic_increase_strict'):
        return "strictly monotonic increase"
    if not column_data.get('monotonic_increase_strict'):
        return "strictly monotonic decrease"
    return str(None)

def data_distribution(df, report_data, column):
    column_data = report_data['variables'].get(column, {})
    if df[column].dtype in ['int64', 'float64']:
        kurtosis = column_data.get('kurtosis', 0)
        skewness = column_data.get('skewness', 0)
        p_value = column_data.get('chi_squared', {}).get('p_value', 0)

        if abs(skewness) < 0.5 and -1 <= kurtosis <= 1:
            return "Normal"
        if p_value > 0.05:
            return "Uniform"
        return "Skewed"
    return str(None)

def data_quality(report_data, column):
    column_data = report_data['variables'].get(column, {})
    n = column_data.get('n', 0)
    n_missing = column_data.get('n_missing', 0)
    if n > 0:
        bad_data_percentage = n_missing / n
        return str(100 - (bad_data_percentage * 100))
    return str(0)

def byte_length(df, column):
    if df[column].dtype == 'object':
        byte_lengths = df[column].dropna().apply(lambda x: len(x.encode('utf-8')))
        return str(byte_lengths.max()) if not byte_lengths.empty else str(0)
    return str(0)

def lt_mean_count(df, column, report_data):
    mean_value = report_data['variables'].get(column, {}).get('mean', None)
    if mean_value is not None:
        return str(sum(1 for value in df[column] if isinstance(value, (int, float)) and value < mean_value))
    return str(0)

def validate_stat_value(value, stat_name, column):
    if value is None:
        raise ValueError(f"Missing {stat_name} value for column: {column}")
    return value

def count_values(df, column, comparator, reference):
    return str(df[column].apply(lambda x: isinstance(x, (int, float)) and comparator(x, reference)).sum())

def gt_mean_count(df, column, report_data):
    column_data = report_data['variables'].get(column, {})
    mean_value = validate_stat_value(column_data.get('mean'), 'mean', column)
    return count_values(df, column, lambda x, ref: x > ref, mean_value)

def eq_mean_count(df, column, report_data):
    column_data = report_data['variables'].get(column, {})
    mean_value = column_data.get('mean', 0)
    return count_values(df, column, lambda x, ref: x == ref, mean_value)

def lt_median_count(df, column, report_data):
    column_data = report_data['variables'].get(column, {})
    median_value = column_data.get('median_length', 0)
    return count_values(df, column, lambda x, ref: x < ref, median_value)

def eq_median_count(df, column, report_data):
    column_data = report_data['variables'].get(column, {})
    median_value = column_data.get('median_length', 0)
    return count_values(df, column, lambda x, ref: x == ref, median_value)

def eq_zero_count(df, column):
    return str(df[column].apply(lambda x: x == 0).sum())

def gt_median_count(df, column, report_data):
    column_data = report_data['variables'].get(column, {})
    median_value = column_data.get('median_length', 0)
    return count_values(df, column, lambda x, ref: x > ref, median_value)

def lt_zero_count(df, column):
    return str(df[column].apply(lambda x: isinstance(x, (int, float)) and x < 0).sum())

def max_length(report_data, column):
    return str(report_data['variables'].get(column, {}).get('max_length', None))

def min_length(report_data, column):
    return str(report_data['variables'].get(column, {}).get('min_length', None))

def is_fixed_length(report_data, column):
    column_data = report_data['variables'].get(column, {})
    min_length = column_data.get('min_length')
    max_length = column_data.get('max_length')
    return str(min_length == max_length)

def is_required(report_data, column):
    n_missing = report_data['variables'].get(column, {}).get('n_missing', 0)
    return str(n_missing == 0)

def data_trend_direction(report_data, column):
    monotonic = report_data['variables'].get(column, {}).get('monotonic_increase', False)
    return "Positive" if monotonic else "Negative"
