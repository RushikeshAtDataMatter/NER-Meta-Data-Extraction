from sqlalchemy.orm import Session
from metadata.schema.new_metadata import newMetaData


def store_metadata_in_db(results, db: Session):
    for column, attributes in results.items():
        # Use `.get()` to provide default values for missing attributes
        metadata_entry = newMetaData(
            name=attributes.get("name", column),
            datatype=attributes.get("datatype", "unknown"),
            is_unique=attributes.get("is_unique", False),
            unique_values_count=attributes.get("unique_values_count", 0),
            null_count=attributes.get("null_count", 0),
            mean_value=attributes.get("mean_value", 0),
            max_value=attributes.get("max_value", 0),
            median_value=attributes.get("median_value", 0),  # Provide a default median value
            mode_value=attributes.get("mode_value", 0),
            is_float=attributes.get("is_float", False),
            is_integer=attributes.get("is_integer", False),
            data_density=attributes.get("data_density", 0),
            data_count=attributes.get("data_count", 0),
            is_multipicklist=attributes.get("is_multipicklist", False),
            data_trend=attributes.get("data_trend", "none"),
            data_distribution=attributes.get("data_distribution", "unknown"),
            data_quality=attributes.get("data_quality", "unknown"),
            byte_length=attributes.get("byte_length", 0),
            lt_mean_count=attributes.get("lt_mean_count", 0),
            gt_mean_count=attributes.get("gt_mean_count", 0),
            eq_mean_count=attributes.get("eq_mean_count", 0),
            lt_median_count=attributes.get("lt_median_count", 0),
            eq_median_count=attributes.get("eq_median_count", 0),
            eq_zero_count=attributes.get("eq_zero_count", 0),
            gt_median_count=attributes.get("gt_median_count", 0),
            lt_zero_count=attributes.get("lt_zero_count", 0),
            max_length=attributes.get("max_length", 0),
            min_length=attributes.get("min_length", 0),
            is_fixed_length=attributes.get("is_fixed_length", False),
            is_required=attributes.get("is_required", False),
            data_trend_direction=attributes.get("data_trend_direction", "unknown"),
            # Custom attributes
            entity=attributes.get("entity", "unknown"),
            is_city=attributes.get("is_city", False),
            is_country=attributes.get("is_country", False),
            is_state=attributes.get("is_state", False),
            is_date=attributes.get("is_date", False),
            data_format=attributes.get("date_format", "unknown"),
            is_address_line=attributes.get("is_address_line", False),
            is_noun=attributes.get("is_noun", False),
            is_case_sensitive=attributes.get("is_case_sensitive", False),
            is_text=attributes.get("is_text", False),
            encoding=attributes.get("encoding", "utf-8"),
            is_html=attributes.get("is_html", False),
            is_pincode=attributes.get("is_pincode", False),
            is_phone=attributes.get("is_phone", False),
            is_email=attributes.get("is_email", False),
            is_link=attributes.get("is_link", False),
        )
        db.add(metadata_entry)

    db.commit()

