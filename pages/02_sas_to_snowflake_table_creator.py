import streamlit as st
import re
import pandas as pd
from datetime import datetime

def get_step_info(step):
    step_type = ""
    try:
        step_type = re.search(r'Transform:\s*(\w*\s?\w*)', step).group(1).strip()
    except:
        pass
    return step_type

def extract_table_loader(sas_code, df, sequence):
    print("Table Loader extraction started")
    # Parse source table
    source_table_match = re.search(r'Source Table:.*?\w+ \w+ - \w+\.(\w+)', sas_code, re.DOTALL)
    source_table = source_table_match.group(1) if source_table_match else None
    
    # Parse target table
    target_table_match = re.search(r'Target Table:\s+(\w+)', sas_code, re.DOTALL)
    target_table = target_table_match.group(1) if target_table_match else None

    # Parse data from table
    # this line will find all values in the data step sas code
    select_statement_match = re.search(r'data(.*?)(attrib.*)call\smissing\(of\s_all_\);', sas_code, re.DOTALL | re.IGNORECASE)
    if select_statement_match:
        column_lines = re.split(';\s?\n', select_statement_match.group(2))
    else:
        column_lines = []
    print("Lenght of data step: ", len(column_lines)-1)
    for line in range(len(column_lines)-1):
  
        # Parse column name / att-1ribute name
        column_match = re.search(r'attrib\s(\w*)', column_lines[line])
        column_name = column_match.group(1) if column_match else None

        lenght_match = re.search(r'length = ([\w\d\.$]+)', column_lines[line])
        length_name = lenght_match.group(1) if lenght_match else None
 
        format_match = re.search(r'format = ([\w\d\.$]+)', column_lines[line])
        format_name = format_match.group(1) if format_match else None

        informat_match = re.search(r'informat = ([\w\d\.$]+)', column_lines[line])
        informat_name = informat_match.group(1) if informat_match else None

        label_match = re.search(r"label\s=\s'(.*)'", column_lines[line])
        label_name = label_match.group(1) if label_match else None

        new_row = pd.DataFrame([{
            'source_table': source_table,
            'target_table': target_table,
            'column_name': column_name,
            'column_alias': column_name,
            'length':length_name,
            'format':format_name,
            'informat':informat_name,
            'label':label_name,
            'column_transformation': "",
            'extract_condition': "",
            'join_condition': "",
            'Sequence': sequence,
            'step_type': 'Table Loader'
        }])  

        df = pd.concat([df, new_row], ignore_index=True)

    # Parse column names, aliases and transformations

    # IF this don't find the proc sql, means we have no column name changes in the table loader --> else

    if 'proc sql;' in sas_code:
        proc_sql_statement = re.search(r'\n\s*select(.*?)from\s\&etls_lastTable', sas_code, re.DOTALL | re.IGNORECASE)
        if proc_sql_statement:
            column_lines = re.split(',\n', proc_sql_statement.group(1))
        else:
            column_lines = []
        print("length of SQL: ", len(column_lines))    
        counter = 0
        for line in column_lines:
            column_name_sql = re.search(r'(\w+)\sas\s(\w+)', line, re.IGNORECASE)
            if column_name_sql:
                df["column_alias"][counter] = column_name_sql.group(2)
                df["column_name"][counter] = column_name_sql.group(1)
            else:
                df["column_alias"][counter] = df["column_name"][counter]
            counter += 1
        return df
    else:
        return df
    
def extract_join(sas_code, df, sequence):
    print("join extraction started")
    # Parse target table
    target_table_match = re.search(r'Target Table:\s+.+\.+(\w*)\s\s', sas_code, re.DOTALL)
    target_table = target_table_match.group(1) if target_table_match else None
    # Parse data from table
    # each type of join have sligthly different syntax :) so need seperate handling for each 
    if 'inner join' in sas_code:
        select_statement_match = re.search(r'proc sql;\n(.*?)(distinct\n.*)\s+from\n', sas_code, re.DOTALL | re.IGNORECASE)
        join_statement_match = re.search(r'\s*from\n(.*)\;\nquit\;\n', sas_code, re.DOTALL | re.IGNORECASE)
        if select_statement_match:
            column_lines = re.split(',\n', select_statement_match.group(2))
        else:
            column_lines = []
        print("Lenght of data step: ", len(column_lines))
        for line in range(len(column_lines)):

            # Parse column name / att-1ribute name
            column_info = re.search(r'(\w*)\.(\w*)\slength\s\=\s(\d*)', column_lines[line])
            source_table = column_info.group(1) if column_info else None
            column_name = column_info.group(2) if column_info else None
            length_name = column_info.group(3) if column_info else None
            
            format_match = re.search(r'format = ([\w\d\.$]+)', column_lines[line])
            format_name = format_match.group(1) if format_match else None

            label_match = re.search(r"label\s=\s'(.*)'", column_lines[line])
            label_name = label_match.group(1) if label_match else None

            new_row = pd.DataFrame([{
                'source_table': source_table,
                'target_table': target_table,
                'column_name': column_name,
                'column_alias': "",
                'length':length_name,
                'format':format_name,
                'informat':"",
                'label':label_name,
                'column_transformation': "",
                'extract_condition': "",
                'join_condition': join_statement_match.group(1),
                'Sequence': sequence,
                'step_type': 'Join'
            }])  

            df = pd.concat([df, new_row], ignore_index=True)
        return df
    elif 'left join' in sas_code:
        return df
    
def extract_extract(sas_code, df, sequence):
    print("extract extraction started")
    # Parse target table
    target_table_match = re.search(r'Target Table:\s+.+\.+(\w*)\s\s', sas_code, re.DOTALL)
    target_table = target_table_match.group(1) if target_table_match else None

    # Parse data from table
    # each type of join have sligthly different syntax :) so need seperate handling for each 
    select_statement_match = re.search(r'\s+select\n(.*?)\s+from\s', sas_code, re.DOTALL | re.IGNORECASE)

    if select_statement_match:
        column_lines = re.split(',\n', select_statement_match.group(1))
    else:
        column_lines = []
    print("Lenght of data step: ", len(column_lines))
    for line in range(len(column_lines)):

        # Parse column name / att-1ribute name
        column_info = re.search(r'(\w*),', column_lines[line])
        source_table = column_info.group(1) if column_info else None
        column_name = column_info.group(2) if column_info else None
        length_name = column_info.group(3) if column_info else None
        
        format_match = re.search(r'format = ([\w\d\.$]+)', column_lines[line])
        format_name = format_match.group(1) if format_match else None

        label_match = re.search(r"label\s=\s'(.*)'", column_lines[line])
        label_name = label_match.group(1) if label_match else None

        new_row = pd.DataFrame([{
            'source_table': source_table,
            'target_table': target_table,
            'column_name': column_name,
            'column_alias': "",
            'length':length_name,
            'format':format_name,
            'informat':"",
            'label':label_name,
            'column_transformation': "",
            'extract_condition': "",
            'join_condition': "",
            'Sequence': sequence,
            'step_type': 'Join'
        }])  

        df = pd.concat([df, new_row], ignore_index=True)
    return df

    
def infer_data_type(df):
    def _get_type(row):
        format_val = str(row['format']) if pd.notna(row['format']) else str(row['length'])
        attrib_name = str(row['column_name'])
        digit_match = re.search(r'(\d+)', format_val)
        digit_decimal_match = re.search(r'(\d+\.\d+)', format_val)
        is_it_decimal = True if digit_match and pd.isnull(row['format']) and pd.isnull(row['informat']) else False 
        if 'LOAD_TIME' in attrib_name:
            return "TIMESTAMP"
        elif 'DATETIME' in format_val:
            return "DATETIME"
        elif 'Datetime' in format_val:
            return "DATETIME"
        elif 'datetime' in format_val:
            return "DATETIME"
        elif 'DATE' in format_val:
            return "DATE"
        elif 'date' in format_val:
            return "DATE"
        elif 'Date' in format_val:
            return "DATE"
        elif format_val.startswith('$') and digit_match:
            return "VARCHAR({})".format(digit_match.group(1))
        elif re.search(r'([a-zA-Z]+)', format_val):
            return "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        elif digit_decimal_match:
            return "FLOAT"
        elif is_it_decimal:
            return "FLOAT"
        elif digit_match:
            return "NUMBER({})".format(digit_match.group(1))
        else:
            return "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    # Apply the inner function to each row of the DataFrame
    df['inferred_type'] = df.apply(_get_type, axis=1)
    return df

def extract_metadata(sas_code):
    # Split the code into chunks which represent a sas step/transformation
    steps = re.split(r'/\*+\s*Step end .*? \s*\*+/', sas_code)
    df = pd.DataFrame(columns=['source_table', 'target_table', 'column_name', 'column_alias', 'column_transformation', 'join_condition', 'extract_condition', 'Sequence', 'step_type'])
    sequence = 1

    for step in steps:
        step_type = get_step_info(step)
        print(f'sequence: {sequence} is recognised as step type: {step_type}')
        # for each transformation, we have separate ways of parsing the code
        if step_type == 'Table Loader':
            df = extract_table_loader(step, df, sequence)
        if step_type == 'Join':
            df = extract_join(step, df, sequence)
        if step_type == 'Extract':

            df = extract_extract(step, df, sequence)
        sequence += 1
    df = infer_data_type(df)
    return df

def get_procedure_name(sas_code):
    pattern = r"Target\sTables?:\s*(\w*)"
    matches = re.search(pattern, sas_code, re.MULTILINE)
    if matches:
        return matches.group(1)
    else:
        return None

def generate_create_table(df, procedure_name, author, schema_to):
    create_table_code = f"CREATE OR REPLACE TABLE {schema_to}.{procedure_name.upper()}(\n"
    for _, row in df.iterrows():
        column_name = row['column_alias'].upper()
        column_type = row['inferred_type'].upper()
        column_comment = row['label']
        if column_type:  # add column only if type is not None
            create_table_code += f"    {column_name} {column_type} COMMENT '{column_comment}',\n"
    create_table_code = create_table_code.rstrip(',\n')  # remove trailing comma
    create_table_code += f"\n) COMMENT = '{procedure_name} Table, recreated from SAS 9.4 in DI studio | Created by {author} {datetime.now().strftime('%Y-%m-%d %H:%M')}';"
    return create_table_code

# Streamlit code
st.title('SAS Table-Loader Converter to Snowflake')
st.write("""
This page will take raw SAS code from the Table Loader function, and create a Snowflake table.
""")
sas_code = st.text_area("Paste your SAS code here")

if st.button('Convert Table loader code'):
    if sas_code:
        df = extract_metadata(sas_code)
        author = st.text_input("Enter the author's name", value='Skjels')
        schema_to = st.text_input("Enter the destination schema name", value='NOR_OPS')
        procedure_name = st.text_input("Enter the procedure Name", value=get_procedure_name(sas_code))
        df_updated = st.data_editor(df)  # editable dataframe
        st.write(f"Procedure Name: {procedure_name}")

        if procedure_name and author:
            with st.expander("Create Table Statement"):
                create_table_code = generate_create_table(df_updated, procedure_name, author, schema_to)
                st.code(create_table_code, language='sql')