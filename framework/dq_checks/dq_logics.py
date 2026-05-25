from pyspark.sql.functions import col, expr, length, to_date, lit

# Individual validation functions for different data quality rules
# Each returns a dictionary with validation results (field name, check type, row counts, failure %)

def float_check(df, column):
    """Validate that a field can be cast to float type."""
    total = df.count()
    failed = df.filter((col(column).isNotNull()) & col(column).cast('float').isNull()).count()

    return {
        "field": column,
        "check": "Datatype - float",
        "total_rows": total,
        "failed_rows": failed,
        "percentage": failed / total if total else 0
    }


def int_check(df, column):
    """Validate that a field can be cast to integer type."""
    total = df.count()
    failed = df.filter((col(column).isNotNull()) & col(column).cast('int').isNull()).count()

    return {
        "field": column,
        "check": "Datatype - int",
        "total_rows": total,
        "failed_rows": failed,
        "percentage": failed / total if total else 0
    }


def field_value_check(df, column):
    """Validate that field values don't exceed 2 characters (e.g., state codes)."""
    total = df.count()
    failed = df.filter((col(column).isNotNull()) & (length(col(column)) > 2)).count()

    return {
        "field": column,
        "check": "Field Value - 2 characters",
        "total_rows": total,
        "failed_rows": failed,
        "percentage": failed / total if total else 0
    }


def grade_check(df, column):
    """Validate that grade field is exactly 1 character (A-G)."""
    total = df.count()
    failed = df.filter((col(column).isNotNull()) & (length(col(column)) > 1)).count()

    return {
        "field": column,
        "check": "Field Value - 1 character",
        "total_rows": total,
        "failed_rows": failed,
        "percentage": failed / total if total else 0
    }


def zip_code_check(df, column):
    """Validate that zip code field is exactly 5 characters."""
    total = df.count()
    failed = df.filter((col(column).isNotNull()) & (length(col(column)) != 5)).count()

    return {
        "field": column,
        "check": "Field Value - 5 characters",
        "total_rows": total,
        "failed_rows": failed,
        "percentage": failed / total if total else 0
    }


def member_id_duplication(df, column):
    """Validate that member IDs are unique (no duplicates)."""
    total = df.count()
    dup = df.groupBy(column).agg(expr('count(*)').alias('count')).filter(col('count') > 1)
    failed = dup.count()

    return {
        "field": column,
        "check": "Member ID Duplication",
        "total_rows": total,
        "failed_rows": failed,
        "percentage": failed / total if total else 0
    }


def allowed_values_check(df, column, allowed_values):
    """Validate that field contains only values from a predefined list."""
    total = df.count()
    failed = df.filter(col(column).isNotNull() & ~col(column).isin(allowed_values)).count()

    return {
        "field": column,
        "check": "Allowed Values",
        "total_rows": total,
        "failed_rows": failed,
        "percentage": failed / total if total else 0
    }


def calculation_check(df, column, dependent_columns):
    """Validate that a derived field equals the sum of its dependent columns."""
    total = df.count()
    failed = df.filter(col(column) != expr(" + ".join(dependent_columns))).count()

    return {
        "field": column,
        "check": "Calculation check",
        "total_rows": total,
        "failed_rows": failed,
        "percentage": failed / total if total else 0
    }


def string_check(df, column):
    """Validate that a string field is in valid date format (MMM-yyyy)."""
    total = df.count()
    failed = df.filter((col(column).isNotNull()) & to_date(col(column), 'MMM-yyyy').isNull()).count()

    return {
        "field": column,
        "check": "String check",
        "total_rows": total,
        "failed_rows": failed,
        "percentage": failed / total if total else 0
    }

def null_check(df, column):
    """Validate that a field does not contain null/missing values."""
    total = df.count()
    df_check = df.filter(col(column).isNull())
    failed = df_check.count()

    return {
        "field": column,
        "check": "Null check",
        "total_rows": total,
        "failed_rows": failed,
        "percentage": failed / total if total else 0
    }


# Helper functions for rule processing and failure identification

def _failed_rows_for_column(df, column, rule):
    """
    Extract rows that fail a specific validation rule for a given column.
    Returns the filtered DataFrame with only records that violate the rule.
    """
    rule_type = rule["type"]

    if rule_type == "float_check":
        return df.filter(col(column).isNotNull() & col(column).cast('float').isNull())

    if rule_type == "int_check":
        return df.filter(col(column).isNotNull() & col(column).cast('int').isNull())

    if rule_type == "field_value_check":
        return df.filter(col(column).isNotNull() & (length(col(column)) > 2))

    if rule_type == "grade_check":
        return df.filter(col(column).isNotNull() & (length(col(column)) > 1))

    if rule_type == "zip_code_check":
        return df.filter(col(column).isNotNull() & (length(col(column)) != 5))

    if rule_type == "allowed_values_check":
        allowed = rule.get("allowed_values", [])
        return df.filter(col(column).isNotNull() & ~col(column).isin(allowed))

    if rule_type == "calculation_check":
        dependent_columns = rule.get("dependent_columns", [])
        return df.filter(col(column) != expr(" + ".join(dependent_columns)))

    if rule_type == "string_check":
        return df.filter(col(column).isNotNull() & to_date(col(column), 'MMM-yyyy').isNull())

    if rule_type == "member_id_duplication":
        dup = df.groupBy(column).agg(expr('count(*)').alias('count')).filter(col('count') > 1).select(column)
        return df.join(dup, on=column, how='inner')

    if rule["type"] == "null_check":
        return df.filter(col(column).isNull())

    raise ValueError(f"Unknown DQ rule type: {rule_type}")


def _existing_rule_columns(df, rule):
    """Return only the columns from the rule that actually exist in the DataFrame."""
    return [c for c in rule.get("columns", []) if c in df.columns]


def _empty_detailed_failures(df):
    """Create an empty DataFrame with the expected failure report schema."""
    empty = df.limit(0)
    empty = empty.withColumn("rule_id", lit(None))
    empty = empty.withColumn("check_type", lit(None))
    empty = empty.withColumn("check_description", lit(None))
    empty = empty.withColumn("failed_column", lit(None))
    return empty


def get_failed_rows_for_rule(df, rule):
    """
    Find all rows that fail a specific validation rule.
    Applies the rule to all its columns and returns a deduplicated DataFrame
    of records that violated at least one check.
    """
    failed = None
    for column in _existing_rule_columns(df, rule):
        failed_rows = _failed_rows_for_column(df, column, rule)
        failed = failed_rows if failed is None else failed.unionByName(failed_rows)
    if failed is not None:
        return failed.dropDuplicates()
    else:        
        return df.limit(0)


def get_failed_rows_with_details(df, rule):
    """
    Find all rows that fail a validation rule, with detailed failure information.
    Adds metadata columns (rule ID, check type, description, failed column) to enable
    root cause analysis and data remediation efforts.
    """
    failed = None
    for column in _existing_rule_columns(df, rule):
        failed_rows = _failed_rows_for_column(df, column, rule)
        # Annotate failure with rule details for investigation
        failed_rows = failed_rows.withColumn("rule_id", lit(rule.get("rule_id", "")))
        failed_rows = failed_rows.withColumn("check_type", lit(rule.get("type", "")))
        failed_rows = failed_rows.withColumn("check_description", lit(rule.get("check", "")))
        failed_rows = failed_rows.withColumn("failed_column", lit(column))

        failed = failed_rows if failed is None else failed.unionByName(failed_rows, allowMissingColumns=True)

    if failed is not None:
        return failed.dropDuplicates()
    else:
        return _empty_detailed_failures(df)