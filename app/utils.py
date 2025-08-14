import boto3
from botocore.exceptions import NoCredentialsError
import os
from dotenv import load_dotenv
import pandas as pd
import psycopg2 as pg
from psycopg2 import OperationalError
import re
from datetime import datetime
from collections import Counter




import logging
import json
from app.config import Config

from highcharts_core.chart import Chart
from highcharts_core.options.exporting import Exporting
import copy

import atexit

import threading
import time
from datetime import datetime, timedelta
from dateutil.parser import parse as dateutil_parse
from decimal import Decimal
from google import genai
#from google.genai.types import HttpOptions
from google import genai
from google.genai.types import HttpOptions



# Session management
SESSIONS_DB = {}
SESSIONS_LOCK = threading.Lock()



try:
    
    client = genai.Client(http_options=HttpOptions(api_version="v1"))
    
    print("Google GenAI client initialized successfully")
except Exception as e:
    print(f"Failed to initialize Google GenAI client: {e}")







def create_connection(host, user, password, db, port):
    
    try:
        connection = pg.connect(
            host=host,
            user=user,
            password=password,
            dbname=db,
            port=port
        )
        print("PostgreSQL connection established successfully.")
        return connection
    except OperationalError as e:
        print(f"Connection Error: Could not connect to PostgreSQL. The error was: '{e}'")
        return None


def download_file_from_s3(bucket_name, s3_key, local_file_path, access_key, secret_key, region):

    if Config.APP_ENV != 'development':
        client_kwargs = {
            'region_name' : region,
        }

    else :
        client_kwargs = {
            'aws_access_key_id': access_key,
            'aws_secret_access_key': secret_key,
            'region_name': region
        }

    
    s3 = boto3.client('s3', **client_kwargs)
    try:
        print(f"Downloading '{s3_key}' from bucket '{bucket_name}'...")
        s3.download_file(bucket_name, s3_key, local_file_path)

        logging.info(f"File downloaded successfully to {local_file_path}")
        return True
    except NoCredentialsError:
        logging.error("Credentials not available. Please check your AWS credentials.")
        return False
    except Exception as e:
        logging.error(f"An error occurred during S3 download: {e}")
        return False


def dtype_and_pattern_identifier(csv_file, sample_size=100):
    
    null_values = {'nan', 'null', 'none', 'na', 'n/a', ''}
    patterns = {
        'timestamp': [r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$", r"^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\]\d{2}$", r"^\d{2}-\d{2}-\d{4} \d{2}:\d{2}:\d{2}$", r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$"],
        'date': ['%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y', '%Y/%m/%d', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S', '%m/%d/%Y %H:%M:%S', '%d-%m-%Y %H:%M:%S', '%Y/%m/%d %H:%M:%S', '%a %b %d %Y %H:%M:%S GMT%z (%Z)', '%a %b %d %Y %H:%M:%S GMT%z','%Y-%m-%dT%H:%M:%S%z', '%d-%m-%Y %H:%M', '%a, %d %b %Y %H:%M:%S GMT',     '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%dT%H:%M:%S.%fZ',
    '%Y-%m-%d %H:%M:%S', '%d-%m-%Y %H:%M:%S', '%m/%d/%Y %H:%M:%S', '%d/%m/%Y %H:%M:%S',
    '%Y/%m/%d %H:%M:%S', '%d-%b-%Y %H:%M', '%d-%m-%Y %H:%M',
    '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y', '%d-%b-%Y',
    '%a, %d %b %Y %H:%M:%S GMT', '%a, %d %b %y %H:%M:%S GMT', '%a %b %d %H:%M:%S %Y',
    '%Y-%m-%d %H:%M:%S %z', '%a %b %d %Y %H:%M:%S GMT%z', '%a %b %d %Y %H:%M:%S GMT%z (%Z)'],
        'int': [r"^-?\d+$", r"^-?\d{1,2}(?:,\d{2})+(?:,\d{3})?$"],
        'float': [r"^-?(\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?$", r"^-?\d{1,2}(?:,\d{2})+(?:,\d{3})?(?:\.\d+)?$"],
        'bool': [r"^(?:true|false|1|0|yes|no|y|n|t|f)$"]
    }

    try:
        
        data = pd.read_csv(csv_file, nrows=sample_size, low_memory=False)
    except FileNotFoundError:
        return f"Error: File not found at {csv_file}"
    except Exception as e:
        return f"Error reading CSV: {e}"

    column_results = {}
    for column in data.columns:
        pattern_counts = Counter()
        non_null_count = 0

        for value in data[column].dropna():
            str_value = str(value).strip()
            if str_value.lower() in null_values:
                continue
            non_null_count += 1
            found_match = False

            
            for pat in patterns['date']:
                try:
                    test_value = str_value
                   
                    if '(%Z)' in pat:
                        test_value = re.sub(r"\s*\(.*?\)", "", test_value)
                        pat = pat.replace(" (%Z)", "")  
                    datetime.strptime(test_value, pat)
                    pattern_counts[('date', pat)] += 1
                    found_match = True
                    break
                except ValueError:
                    continue
            if found_match: continue

            for pat in patterns['timestamp']:
                try:
                    test_value = str_value
                    if '(%Z)' in pat:
                        test_value = re.sub(r"\s*\(.*?\)", "", test_value)
                        pat = pat.replace(" (%Z)", "")
                    datetime.strptime(test_value, pat)
                    pattern_counts[('timestamp', pat)] += 1
                    found_match = True
                    break
                except ValueError:
                    continue
            if found_match: continue

            
            for dtype in ['int', 'float', 'bool']:
                for pat in patterns[dtype]:
                    if re.fullmatch(pat, str_value, re.IGNORECASE if dtype == 'bool' else 0):
                        pattern_counts[(dtype, pat)] += 1
                        found_match = True
                        break
                if found_match: break
        
       
        if not pattern_counts or non_null_count == 0:
            column_results[column] = {'dtype': 'string', 'pattern': None}
        else:
            (best_dtype, best_pattern), count = pattern_counts.most_common(1)[0]
            
            if count / non_null_count > 0.5:
                column_results[column] = {'dtype': best_dtype, 'pattern': best_pattern}
            else:
                column_results[column] = {'dtype': 'string', 'pattern': 'mixed'}

    return column_results


def date_type_converter(value, pattern=None, return_type="date"):
    
    if pd.isna(value) or str(value).strip().lower() in {'nan', 'null', 'none', 'na', 'n/a', ''}:
        return None

    str_value = str(value).strip()

    cleaned_value = re.sub(r"\s*\(.*?\)", "", str_value)

 
    if pattern:
        try:
            parsed = datetime.strptime(cleaned_value, pattern)
            return parsed.date() if return_type == "date" else parsed
        except (ValueError, TypeError):
            pass

   
    try:
        parsed = dateutil_parse(cleaned_value, fuzzy=True)
        return parsed.date() if return_type == "date" else parsed
    except Exception:
        return None

def timestamp_type_converter(value, pattern=None):
    if pd.isna(value) or str(value).strip().lower() in {'nan', 'null', 'none', 'na', 'n/a', ''}:
        return None

    str_value = str(value).strip()

    cleaned_value = re.sub(r"\s*\(.*?\)", "", str_value)

    if pattern:
        try:
            parsed = datetime.strptime(cleaned_value, pattern)
            return parsed
        except (ValueError, TypeError):
            pass

    try:
        parsed = dateutil_parse(cleaned_value, fuzzy=True)
        return parsed
    except Exception:
        return None


def integer_type_converter(value):
    try:
        # Remove commas (both international and Indian formats)
        cleaned = str(value).replace(",", "")
        return int(float(cleaned))
    except (ValueError, TypeError):
        return None


def float_type_converter(value):
    try:
        # Remove commas (both international and Indian formats)
        cleaned = str(value).replace(",", "")
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def bool_type_converter(value):
   
    true_values = {'true', '1', 'yes', 'y', 't'}
    false_values = {'false', '0', 'no', 'n', 'f'}
    val_lower = str(value).strip().lower()
    if val_lower in true_values:
        return True
    elif val_lower in false_values:
        return False
    return None


def pg_table_creater(connection, table_name, column_patterns):
    
    cursor = connection.cursor()
    columns = []
    for column, pattern_info in column_patterns.items():
        dtype = pattern_info['dtype']
        col_name_quoted = f'"{column}"'
        
        if dtype == 'int':
            pg_type = "BIGINT" 
        elif dtype == 'float':
            pg_type = "DOUBLE PRECISION"
        elif dtype == 'date':
            pg_type = "DATE"
        elif dtype == 'bool':
            pg_type = "BOOLEAN"
        else: # Default to TEXT
            pg_type = "TEXT"
        
        columns.append(f"{col_name_quoted} {pg_type}")
    
    columns_str = ", ".join(columns)
  
    drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
    create_table_query = f"CREATE TABLE {table_name} ({columns_str});"
    
    try:
        print(f"Preparing to create table '{table_name}'...")
        cursor.execute(drop_table_query)
        cursor.execute(create_table_query)
        connection.commit()
        print(f"Table '{table_name}' created successfully.")
    except pg.Error as e:
        print(f"Error creating table '{table_name}': {e}")
        connection.rollback()
    finally:
        cursor.close()


def process_and_insert_chunk(connection, table_name, data_chunk, column_info):
    
    cursor = connection.cursor()
    
    columns_str = ", ".join(f'"{col}"' for col in data_chunk.columns)
    placeholders = ", ".join(["%s"] * len(data_chunk.columns))
    insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

    processed_rows = []
    for _, row in data_chunk.iterrows():
        row_values = []
        for col, val in row.items():
            
          
            if pd.isna(val) or str(val).strip().lower() in {'', 'nan', 'null', 'na', 'n/a'}:
                processed_val = None
            else:
                
                info = column_info.get(col, {'dtype': 'string'})
                dtype = info.get('dtype')
                
                if dtype == 'date':
                    processed_val = date_type_converter(val, info.get('pattern'))
                elif dtype == 'timestamp':
                    processed_val = timestamp_type_converter(val, info.get('pattern'))
                elif dtype == 'int':
                    processed_val = integer_type_converter(val)
                elif dtype == 'float':
                    processed_val = float_type_converter(val)
                elif dtype == 'bool':
                    processed_val = bool_type_converter(val)
                else:  
                    processed_val = str(val)

            row_values.append(processed_val)
        
        processed_rows.append(tuple(row_values))
    
    
    try:
        cursor.executemany(insert_query, processed_rows)
        connection.commit()
    except pg.Error as e:
        print(f"\n--- DATABASE ERROR ---")
        print(f"Error during batch insert: {e}")
        print("Rolling back transaction.")
        connection.rollback()
        if processed_rows:
            print("First row of failing batch:", processed_rows[0])
        
        raise e
    finally:
        cursor.close()

def delete_table(connection, table_name):
    cursor = connection.cursor()
    try:
        print(f"Deleting table '{table_name}'...")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        connection.commit()
        print(f"Table '{table_name}' deleted successfully.")
    except pg.Error as e:
        print(f"Error deleting table '{table_name}': {e}")
        connection.rollback()
    finally:
        cursor.close()

def get_query(user_query, TABLE_NAME, DB_Column_Name):
    prompt = f"""
[Role & Goal]
You are an advanced AI assistant specializing in Natural Language to SQL conversion. Your primary function is to analyze a user's text input, determine its intent, and respond with a structured JSON object. You must operate exclusively within the constraints of the provided database schema.

[Context: Database Schema]
You have access to the following database schema. You MUST NOT invent or use any table or column names not listed here.
*   **Table Name:** {TABLE_NAME}
*   **Column Names:** {DB_Column_Name}

[Processing Logic & Rules]
Analyze the user input by following these steps in order:

1.  **Social Interaction Analysis (`is_greeting`):**
    *   Analyze the user's input for social interactions such as greetings (e.g., "Hi", "Hello"), farewells (e.g., "Bye", "Goodbye"), compliments (e.g., "Good job", "You're helpful"), or expressions of gratitude (e.g., "Thanks", "Thank you") or permissions (e.g.,"can i ask you question", "should we begin", etc.).
    *   If the input contains any such social interaction, generate a brief, warm, and relevant response (e.g., "Hello! How can I help?", "You're welcome!", "Goodbye!") and place this string in the `is_greeting` field.
    *   If the input is **only** a social interaction (and not a data request), you should still provide the warm response in `is_greeting`, but set the `query` field to `null` and other boolean flags to their default safe state.
    *   If the input contains both a social interaction **and** a data request (e.g., "Thanks, now show me the total sales"), provide the warm response in `is_greeting` AND continue with the subsequent analysis steps to generate the SQL query.
    *   If the input contains no social interaction, set the `is_greeting` field to `null`.

2.  **Validity Analysis (`is_valid`):**
    *   Determine if the user's request can be answered using **only** the provided {TABLE_NAME} and {DB_Column_Name}.
    *   If the data user is asking is not directly present but can be generated with the help of the provided table and columns, you can consider it valid.
    *   Set `is_valid` to `false` if:
        *   The request refers to tables or columns that do not exist in the schema.
        *   The request is completely irrelevant to data retrieval (e.g., "What's the weather like?").
        *   The request is too vague or ambiguous to be converted into a specific SQL query.
    *   If the request can be mapped to the schema, set `is_valid` to `true`.

3.  **Safety Analysis (`is_safe`):**
    *   Analyze the user's intent. The system is read-only.
    *   Set `is_safe` to `false` if the request implies any data modification or schema change. Look for keywords like `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `CREATE`, `TRUNCATE`.
    *   If the request is for data retrieval (`SELECT`), set `is_safe` to `true`.
    *   If the request is not a query (e.g., a greeting or irrelevant question), this should default to `true`.

4.  **Table information or Metadata Related question (`is_meta_data`)**
      * If user is asking questions that are related to table name or columns names and if you have that information then return that information otherwise set it to null
      * If the request is related to metadata (e.g., "What are the columns in the table?", "What is the name of the table?", then set `is_meta_data` to warm response like "The table name is sales_data_scalable and the columns are ['column1', 'column2', ...]". If the request is not related to metadata, set `is_meta_data` to null.)
      * If the request is related to metadata and you don't have that information, set `is_meta_data` to null.

4.  **PostgreSQL Generation (`query`):**
    *   Make sure to generate query that can be executable on PostgreSQL
    *   If `is_valid` is `true` AND `is_safe` is `true`, generate the corresponding `SELECT` SQL query.
    *   If `is_valid` is `false` or `is_safe` is `false`, the `query` must be `null`.
    *   If a query is generated, it must be syntactically correct SQL.
    *   Only add LIMIT 100 to the SQL query if the user's request is likely to return more than 100 rows (for example, if the user asks for "all", uses plural forms, or requests a large set). If the user asks for a specific number of rows less than or equal to 100, use that number as the limit. If the user query is likely to return 100 rows or fewer, do not add a LIMIT clause.
    *   **Case-Insensitivity Handling:** For any `WHERE` clause that filters a text/string column (like varchar, char, text), you **MUST** ensure the comparison is case-insensitive. To do this, apply the `LOWER()` function to both the column name and the literal string value provided by the user with that use `ILIKE` operator instead of `=` operator.
    *   Do note to wrap the column names in double quotes to ensure case-sensitivity in PostgreSQL. E.g `SELECT * FROM "sales_data_scalable" WHERE "product_name" ILIKE '%example%';`
    *   Use ILIKE with % wildcards if the user query implies string  match operations
   5.  **Reasoning (`reason`):**
    *   Write reasoning like your thought process behind the decision-making.
    *   Provide a concise, one-to-two-line explanation for the outcome like you're explaining to a human.
    *   If successful, state "Success." and then describe the steps you took to arrive at the SQL query. For example: "Success. To answer your request, I first looked for records where 'status' is 'completed', then I selected the 'order_id' and 'customer_name', and finally, I limited the results to the first 10 entries." Explain it as if you are detailing your actions.
    *   Don't include table name in explnation or reasoning  adress table name as just table
    *   In reasoning or explanation don't mention that you are limiting result to 100 rows.
    *   If it fails, explain why (e.g., "Failure." and then the specific reason for the failure, like "The column 'order_date' you asked for isn't available in this dataset.").
[Output Format]
You must respond ONLY with a single JSON object in the following format. Do not add any text before or after the JSON block.

{{
  "is_greeting": <null or (warm response to greeting / farewell / compliment / gratitude)>,
  "is_valid": <true/false>,
  "is_safe": <true/false>,
  "is_meta_data": <true/false>,
  "query": "<SQL query string or null>",
  "reason": "<A concise 1-2 line explanation of the result.>"
}}

"""
    try:
        raw_response = client.models.generate_content(
             model="gemini-2.0-flash",
             contents= [prompt, f"user input: {user_query}"],
        )
        
    except Exception as e:
        logging.error(f"Error in generating content: {e}")
        return f"There is some issue with our text to SQL model. \n Sorry for the inconvenience. \n Try after few minutes."
    response_text = raw_response.text.strip()
    response_text = response_text.strip("```json\n").strip("\n```").strip()
    response = json.loads(response_text)
    expected_response = {
        "is_greeting": (str, type(None)),  
        "is_valid": bool,
        "is_safe": bool,
        "is_meta_data": (str, type(None)),
        "query": (str, type(None)),
        
        "reason": str
    }
    for key, expected_type in expected_response.items():
        if key not in response or not isinstance(response[key], expected_type):
            logging.error(f"Response is missing key {key} or has wrong type")
            for i in range(5):
                get_query(user_query, Config.DB_TABLE_NAME, DB_Column_Name)
                logging.error(f"Retrying {i+1}/5")
            logging.info(f"Response  is missing key {key} or has wrong type")
            return (f"Sorry for inconvenience, we are unable to process your request at the moment. \n Please try again later.")

    return response
            
      

def drop_table(table_name):
    connection = create_connection(Config.PG_HOST, Config.PG_USER, Config.PG_PASSWORD, Config.PG_DB, Config.PG_PORT)
    if not connection:
        logging.error("Failed to connect to PostgreSQL for table deletion.")
        return False
    cursor = connection.cursor()
    try:
        print(f"Deleting table '{table_name}'...")
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        connection.commit()
        print(f"Table '{table_name}' deleted successfully.")
    except pg.Error as e:
        print(f"Error deleting table '{table_name}': {e}")
        connection.rollback()
    finally:
        cursor.close()
        logging.info(f"Table '{table_name}' deleted successfully.")
        return True
    
def process_result(result):
    if result is None or (isinstance(result, pd.DataFrame) and result.empty):
        return {"message": "No data found for your query."}
    elif isinstance(result, pd.DataFrame):
        return {"data": result.to_json(orient='records')}
    return {"message": result}





def cleanup_session(uuid):
    with SESSIONS_LOCK:
        SESSIONS_DB.pop(uuid, None)
    
    






def generate_hightChart_config_prompt(sql_query, data_frame_name, column_names):
    prompt = f"""You are an expert in generating Highcharts configuration JSON from SQL queries.

Your task is to analyze the given SQL query and determine whether the query result is suitable for visualization. If it is, generate and return a valid Highcharts configuration. If not, return `null`.


[SQL Query]
{sql_query}

[Available Columns After Execution] 
{column_names}

[Assumptions]
- The SQL query has been successfully executed in PostgreSQL.
- The result is stored in a pandas DataFrame named `{data_frame_name}`.
- The DataFrame is a list of dictionaries (List[Dict[str, Any]]), where each dictionary represents one row of the result.
- You do NOT need to evaluate the actual data — infer structure and intent based only on the SQL and the returned columns.


[Chart Generation Rules]

Generate a Highcharts configuration ONLY IF all of the following are true:
- The SQL result contains **at least two columns**
- The result includes **at least one categorical/groupable field** (e.g., ProductName, Country, Region, Date, etc.)
- The result includes **at least one numeric or aggregated field** (e.g., SUM, COUNT, AVG, or any numerical column)
- The result likely contains **multiple rows** (based on use of GROUP BY, LIMIT, etc.)

DO NOT generate a chart if:
- The result has **only one column**
- The result has **no categorical or groupable fields**
- The result has **no numeric or measurable fields**
- The result represents a **single aggregated value** (e.g., SELECT COUNT(*))
- The query result is not suitable for a meaningful chart


Chart Type Guidelines:
- Choose most suitable chart type based on the data
[Output Requirements]
If charting is appropriate, return a JSON object with the following structure:

{{
  "high_chart_config": {{
    "chart": {{
      "type": "<chart_type>"
    }},
    "title": {{
      "text": "<chart_title>"
    }},
    "xAxis": {{
      "categories": "<CODE>[record['x_column'] for record in {data_frame_name}]</CODE>",
      "title": {{
        "text": "<x-axis label>"
      }}
    }},
    "yAxis": {{
      "title": {{
        "text": "<y-axis label>"
      }}
    }},
    "series": [{{
      "name": "<series name>",
      "data": "<CODE>[record['y_column'] for record in {data_frame_name}]</CODE>"
    }}]
  }}
}}

If charting is **not appropriate**, return:
null


Invalid output example (result has only 1 column or 1 aggregate value):
null
"""


    try:
        raw_response = client.models.generate_content(
             model="gemini-2.0-flash",
             contents= [prompt],
        )
        response_text = raw_response.text.strip()
        response_text = response_text.strip("```json\n").strip("\n```").strip()
        if response_text.lower() == "null":
            logging.warning("LLM returned 'null': not enough data to create a chart.")
            return None

        response_json = json.loads(response_text)

        if "high_chart_config" in response_json and isinstance(response_json["high_chart_config"], dict):
            logging.info("Highcharts config generated successfully.")
            return response_json
        else:
            logging.error("Invalid Highcharts config structure returned by LLM.")
            return None

    except Exception as e:
        logging.error(f"Exception during Highcharts config generation: {e}")
        return None



def evaluate_code_blocks(config, df):
    config = copy.deepcopy(config)
    records = df.to_dict(orient="records") 

    def eval_code_string(s):
        match = re.match(r'<CODE>(.*)</CODE>', s)
        if match:
            code = match.group(1)
            try:
                return eval(code, {"__builtins__": {}}, {"df": records})
            except Exception as e:
                raise ValueError(f"Error evaluating code block: {code}\n{e}")
        return s

    highchart = config.get('high_chart_config', {})

   
    x_axis = highchart.get('xAxis')
    if x_axis and 'categories' in x_axis:
        highchart['xAxis']['categories'] = eval_code_string(x_axis['categories'])

    
    for i, series in enumerate(highchart.get('series', [])):
        if isinstance(series.get('data'), str):
            highchart['series'][i]['data'] = eval_code_string(series['data'])

    return config

def convert_decimals_to_floats(obj):
    if isinstance(obj, list):
        return [convert_decimals_to_floats(x) for x in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals_to_floats(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return float(obj)
    else:
        return obj



def create_chat_history_table_if_not_exists(conn):
    """Ensure chat_history table exists."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS chat_history_data (
        id SERIAL PRIMARY KEY,
        uuid TEXT NOT NULL,
        timestamp TIMESTAMPTZ NOT NULL,
        user_query TEXT,
        llm_response TEXT,
        sql_query TEXT,
        high_chart_config TEXT,
        table_name TEXT
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
        conn.commit()

def create_meta_data_table_if_not_exists(conn):
    """Ensure meta_data table exists."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS table_meta_data (
        id SERIAL PRIMARY KEY,
        uuid TEXT NOT NULL,
        table_name TEXT NOT NULL,
        timestamp TIMESTAMPTZ NOT NULL
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
        conn.commit()




def insert_metadata_record(connection, uuid, table_name):
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                INSERT INTO table_meta_data (uuid, table_name, timestamp)
                VALUES (%s, %s, %s)
            """, (uuid, table_name, datetime.now()))
            connection.commit()
    except Exception as e:
        logging.error(f"Failed to insert metadata record: {e}")

