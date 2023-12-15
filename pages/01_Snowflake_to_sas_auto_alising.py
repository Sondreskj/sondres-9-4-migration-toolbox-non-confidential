import streamlit as st
import pandas as pd
import re

def extract_extract(sas_code):
    print("Extract extraction started")
    # Parse target table
    target_table_match = re.search(r'Target Table:\s+.+\.+(\w*)\s\s', sas_code, re.DOTALL)
    target_table = target_table_match.group(1) if target_table_match else None

    # Parse data from table
    select_statement_match = re.search(r'\s+select\n(.*?)\s+from\s', sas_code, re.DOTALL | re.IGNORECASE)
    if select_statement_match:
        column_lines = re.split(',\n', select_statement_match.group(1))
    else:
        column_lines = []

    print("Length of data step: ", len(column_lines))

    columns = []
    for line in column_lines:
        column_info = re.search(r'\s*(\w+)', line)
        if column_info:
            column_name = column_info.group(1)
            columns.append({'column_name': column_name, 'column_alias': column_name})
    
    return pd.DataFrame(columns)

def generate_sql_query(df, input_table, output_table):
    sql_query = "proc sql;\n"
    sql_query += f"   create table {output_table} as\n"
    sql_query += "      select\n"
    for _, row in df.iterrows():
        column_name = row['column_name'].upper()
        column_alias = row['column_alias']
        sql_query += f"         {column_name} as {column_alias},\n"
    sql_query = sql_query.rstrip(',\n')  # remove trailing comma
    sql_query += f"\n   from {input_table};\n"
    sql_query += "quit;"
    return sql_query

# Streamlit interface
st.title('SAS to SQL Converter')
st.write("""
This page will take raw SAS code from the extract function in 9.4, and create a SAS proc sql renaming from Upper to the classic camel case we have in SAS. This is made to lessen the work amount and also reduce the risk of error when changing the casing manual to get it to work with migrated pipelines. Simply take the output code and put in in a sas program and connect that to the SNOWFLAKE input.
""")
# Text area for user input
user_input = st.text_area("Enter your SAS code here:", height=300)

if st.button('Convert SAS to SQL'):
    if user_input:
        try:
            df = extract_extract(user_input)
            input_table = '&_input1'
            output_table = '&_output1'
            sql_query = generate_sql_query(df, input_table, output_table)
            st.code(sql_query, language='sql')
        except Exception as e:
            st.error(f"An error occurred: {e}")
    else:
        st.error("Please input some SAS code.")
