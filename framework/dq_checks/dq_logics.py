from pyspark.sql.functions import col, expr, length, to_date, lit


def float_check(df, column):
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
    total = df.count()
    failed = df.filter((col(column).isNotNull()) & to_date(col(column), 'MMM-yyyy').isNull()).count()

    return {
        "field": column,
        "check": "String check",
        "total_rows": total,
        "failed_rows": failed,
        "percentage": failed / total if total else 0
    }

def null_check(df,column):
    total=df.count()
    df_check=df.filter(col(column).isNull())
    failed=df_check.count()

    return{
        "field":column,
        "check":"Null check",
        "total_rows":total,
        "failed_rows":failed,
        "percentage":failed/total if total else 0
    }


def _failed_rows_for_column(df, column, rule):
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
    return [c for c in rule.get("columns", []) if c in df.columns]


def _empty_detailed_failures(df):
    empty = df.limit(0)
    empty = empty.withColumn("rule_id", lit(None))
    empty = empty.withColumn("check_type", lit(None))
    empty = empty.withColumn("check_description", lit(None))
    empty = empty.withColumn("failed_column", lit(None))
    return empty


def get_failed_rows_for_rule(df, rule):
    failed = None
    for column in _existing_rule_columns(df, rule):
        failed_rows = _failed_rows_for_column(df, column, rule)
        failed = failed_rows if failed is None else failed.unionByName(failed_rows)
    if failed is not None:
        return failed.dropDuplicates()
    else:        
        return df.limit(0)


def get_failed_rows_with_details(df, rule):
    failed = None
    for column in _existing_rule_columns(df, rule):
        failed_rows = _failed_rows_for_column(df, column, rule)
        failed_rows = failed_rows.withColumn("rule_id", lit(rule.get("rule_id", "")))
        failed_rows = failed_rows.withColumn("check_type", lit(rule.get("type", "")))
        failed_rows = failed_rows.withColumn("check_description", lit(rule.get("check", "")))
        failed_rows = failed_rows.withColumn("failed_column", lit(column))

        failed = failed_rows if failed is None else failed.unionByName(failed_rows, allowMissingColumns=True)

    if failed is not None:
        return failed.dropDuplicates()
    else:
        return _empty_detailed_failures(df)