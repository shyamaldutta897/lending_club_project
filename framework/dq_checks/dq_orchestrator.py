from collections import defaultdict
import os
import json

from .dq_logics import get_failed_rows_for_rule, get_failed_rows_with_details

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

table_task_mapping = defaultdict(list)
with open(config_path, 'r') as f:
    dq_rules = json.load(f)

    for rule in dq_rules:
        for table in rule["tables"]:
            table_task_mapping[table].append(rule)


def apply_dq_for_table(df, table_name, key_columns=None):
    rules = table_task_mapping.get(table_name, [])
    bad_df = None
    detailed_bad_df = None

    for rule in rules:
        rule_failed = get_failed_rows_for_rule(df, rule)
        rule_failed_with_details = get_failed_rows_with_details(df, rule)

        bad_df = rule_failed if bad_df is None else bad_df.unionByName(rule_failed)
        detailed_bad_df = rule_failed_with_details if detailed_bad_df is None else detailed_bad_df.unionByName(rule_failed_with_details, allowMissingColumns=True)

    if bad_df is None:
        bad_df = df.limit(0)
        detailed_bad_df = bad_df
    else:
        bad_df = bad_df.dropDuplicates()

    if key_columns:
        clean_df = df.join(bad_df.select(*key_columns).dropDuplicates(), on=key_columns, how='anti')
    else:
        clean_df = df.exceptAll(bad_df)
    raw=df.count()
    clean=clean_df.count()
    bad=bad_df.count()

    print(f'raw data records count:{raw},\
        clean data record count:{clean},\
        bad data record count:{bad}')

    return clean_df, bad_df, detailed_bad_df
    
