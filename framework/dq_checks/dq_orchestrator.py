from collections import defaultdict
import os
import json

from .dq_logics import get_failed_rows_for_rule, get_failed_rows_with_details

# Load data quality rules from JSON configuration
script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.abspath(
    os.path.join(
        script_dir,
        "..",
        "..",
        "configs",
        "dq_rules.json"
    )
)

# Build a mapping of table names to their applicable rules
table_task_mapping = defaultdict(list)
with open(config_path, 'r') as f:
    dq_rules = json.load(f)

    for rule in dq_rules:
        for table in rule["tables"]:
            table_task_mapping[table].append(rule)


def apply_dq_for_table(df, table_name, key_columns=None):
    """
    Apply all data quality rules for a given table and separate valid/invalid records.
    
    Args:
        df (DataFrame): Input data to validate
        table_name (str): Name of the table (must match a key in dq_rules.json)
        key_columns (list): Column names that uniquely identify a record. Used for anti-join
                           to exclude bad records. If None, exceptAll is used instead.
    
    Returns:
        tuple: (clean_df, bad_df, detailed_bad_df)
            - clean_df: Valid records that passed all quality checks
            - bad_df: Records that failed at least one quality check
            - detailed_bad_df: Failed records with metadata (rule ID, check type, failed column)
    
    The function:
    1. Loads rules applicable to this table from configuration
    2. Applies each rule to identify failing records
    3. Combines failures across all rules (union)
    4. Deduplicates failures (a record can fail multiple rules)
    5. Separates clean records using anti-join (if key columns provided)
    6. Logs record counts for monitoring and auditing
    """
    rules = table_task_mapping.get(table_name, [])
    bad_df = None
    detailed_bad_df = None

    # Apply all rules to find bad records
    for rule in rules:
        rule_failed = get_failed_rows_for_rule(df, rule)
        rule_failed_with_details = get_failed_rows_with_details(df, rule)

        bad_df = rule_failed if bad_df is None else bad_df.unionByName(rule_failed)
        detailed_bad_df = rule_failed_with_details if detailed_bad_df is None else detailed_bad_df.unionByName(rule_failed_with_details, allowMissingColumns=True)

    # If no failures found, create empty DataFrames with matching schema
    if bad_df is None:
        bad_df = df.limit(0)
        detailed_bad_df = bad_df
    else:
        bad_df = bad_df.dropDuplicates()

    # Extract clean records (those not in bad_df)
    if key_columns:
        # Use anti-join on key columns to exclude any record that appears in bad_df
        clean_df = df.join(bad_df.select(*key_columns).dropDuplicates(), on=key_columns, how='anti')
    else:
        # Use set difference for datasets without specific key columns
        clean_df = df.exceptAll(bad_df)
    
    # Calculate and log record counts for audit trail
    raw = df.count()
    clean = clean_df.count()
    bad = bad_df.count()

    print(f'raw data records count:{raw},\
        clean data record count:{clean},\
        bad data record count:{bad}')

    return clean_df, bad_df, detailed_bad_df
