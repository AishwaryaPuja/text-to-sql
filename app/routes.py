from flask import Blueprint, request, jsonify, render_template
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
from flask import Flask, request, jsonify
import google.generativeai as genai
import logging
import json
from app.config import Config
from flask_cors import CORS
from highcharts_core.chart import Chart
from highcharts_core.options.exporting import Exporting
import copy
from flask import render_template
import atexit
from flask import session
import threading
import time
from datetime import datetime, timedelta
from dateutil.parser import parse as dateutil_parse
from decimal import Decimal
from app.utils import SESSIONS_DB, SESSIONS_LOCK, cleanup_session
from flask import Response, stream_with_context
import ast


from app.utils import (
    create_connection,
    download_file_from_s3,
    dtype_and_pattern_identifier,
    
    pg_table_creater,
    process_and_insert_chunk,
   
    get_query,
    drop_table,
    process_result,
    cleanup_session,
    generate_hightChart_config_prompt,
    evaluate_code_blocks,
    convert_decimals_to_floats,
    create_chat_history_table_if_not_exists,
    create_meta_data_table_if_not_exists,
    insert_metadata_record

)



S3BUCKET = Config.S3BUCKET
ACCESSKEYID = Config.ACCESSKEYID             
SECRETACCESSKEY = Config.SECRETACCESSKEY     
S3_REGION = Config.S3_REGION
DB_TABLE_NAME = Config.DB_TABLE_NAME
PG_HOST = Config.PG_HOST
PG_USER = Config.PG_USER
PG_PASSWORD = Config.PG_PASSWORD
PG_DB = Config.PG_DB
PG_PORT = Config.PG_PORT
DB_TABLE_NAME = Config.DB_TABLE_NAME
PG_DB_NAME_CHAT_HISTORY = Config.PG_DB_NAME_CHAT_HISTORY
PG_DB_CHAT_HISTORY_TABLE = Config.PG_DB_CHAT_HISTORY_TABLE

routes = Blueprint('main', __name__)

# Session management

app = Flask(__name__)
@routes.route('/api/text-to-sql/start_session', methods=['POST'])
def start_session():
    data = request.get_json()
    if not data or 'uuid' not in data or ('s3_key' not in data and 'table_name' not in data):
        return jsonify({"error": "Invalid request. 'uuid' is required, and either 's3_key' or 'table_name' must be provided."}), 400

    uuid = data['uuid']
    s3_key = data['s3_key'] if 's3_key' in data else None
    table_name = data['table_name'] if 'table_name' in data else None
    if s3_key:
        safe_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', s3_key).lower()
        table_name = f"report_table_{safe_table_name}"

    connection = create_connection(PG_HOST, PG_USER, PG_PASSWORD, PG_DB, PG_PORT)
    if not connection:
        return jsonify({"error": "Database connection failed."}), 500
    
    
    
    create_meta_data_table_if_not_exists(connection)
    cursor = None
    try:
        cursor = connection.cursor()
        # Check if table already exists
        cursor.execute(f"""
            SELECT 1 FROM table_meta_data
            WHERE uuid = %s AND table_name = %s
            LIMIT 1;
        """, (uuid, table_name))
        result = cursor.fetchone()
        table_exists = result[0] if result else False


        if table_exists:
            # Load existing metadata
            cursor.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s;
            """, (table_name,))
            columns = cursor.fetchall()
            column_metadata = {col[0]: col[1] for col in columns}

            # Populate in-memory session
            with SESSIONS_LOCK:
                SESSIONS_DB[uuid] = {
                    "table_name": table_name,
                    "column_metadata": column_metadata,
                    "last_accessed": datetime.now()
                }

            logging.info(f"Reusing existing session for UUID {uuid}")
            return jsonify({
                "message": "Session already exists. Reusing existing session.",
                "tableName": table_name,
                "columns": list(column_metadata.keys())
            }), 200

    except Exception as e:
        logging.error(f"Error checking for existing session table: {e}")
        return jsonify({"error": "Failed to check existing session."}), 500
    finally:
        if cursor: cursor.close()
        if connection: connection.close()

    # Proceed with new session setup
    safe_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', uuid).lower() if s3_key else table_name
    local_file_path = f"temp_{safe_table_name}.csv"
    connection = None
    try:
        if not download_file_from_s3(S3BUCKET, s3_key, local_file_path, ACCESSKEYID, SECRETACCESSKEY, S3_REGION):
            raise Exception("Failed to download file from S3.")

        column_metadata = dtype_and_pattern_identifier(local_file_path)
        if isinstance(column_metadata, str):
            raise Exception(f"Failed to identify column types: {column_metadata}")

        connection = create_connection(PG_HOST, PG_USER, PG_PASSWORD, PG_DB, PG_PORT)
        if not connection:
            raise Exception("Database connection failed.")

        pg_table_creater(connection, table_name, column_metadata)

        chunk_size = 10000
        total_processed_rows = 0
        for chunk_df in pd.read_csv(local_file_path, low_memory=False, chunksize=chunk_size):
            process_and_insert_chunk(connection, table_name, chunk_df, column_metadata)
            total_processed_rows += len(chunk_df)
            logging.info(f"Processed {total_processed_rows} rows so far for UUID {uuid}.")

        with SESSIONS_LOCK:
            SESSIONS_DB[uuid] = {
                "table_name": table_name,
                "column_metadata": column_metadata,
                "last_accessed": datetime.now()
            }
        insert_metadata_record(connection, uuid, table_name)
        

        return jsonify({
            "message": "Session started successfully. You can now send queries.",
            "tableName": table_name,
            "columns": list(column_metadata.keys())
        }), 201

    except Exception as e:
        logging.error(f"Error during session start for UUID {uuid}: {e}")
        cleanup_session(uuid)
        return jsonify({"error": f"Failed to start session: {str(e)}"}), 500
    finally:
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
        if connection:
            connection.close()


@routes.route('/api/text-to-sql/read_collection_data', methods=['POST'])
def read_collection_data():
    data = request.get_json()
    if not data or 'uuid' not in data:
        return jsonify({"error": "Missing 'uuid' parameter."}), 400

    uuid = data['uuid']
    offset = data.get('offset', 0)
    try:
        offset = int(offset)
    except (ValueError, TypeError):
        offset = 0

    with SESSIONS_LOCK:
        session_data = SESSIONS_DB.get(uuid)
        if not session_data:
            return jsonify({"error": "Session not found or expired."}), 404
        table_name = session_data['table_name']

    connection = None
    try:
        connection = create_connection(PG_HOST, PG_USER, PG_PASSWORD, PG_DB, PG_PORT)
        if not connection:
            return jsonify({"error": "Database connection failed."}), 500

        cursor = connection.cursor()
        available_records = f'SELECT COUNT(1) FROM "{table_name}";'
        cursor.execute(available_records)
        available_records = cursor.fetchone()[0]
        query = f'SELECT * FROM "{table_name}" OFFSET {offset} LIMIT 100;'
        cursor.execute(query)
        rows = cursor.fetchall()
        
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        if df.empty:
            return jsonify({"message": "No more data."}), 200
        return jsonify({"data": df.to_dict(orient="records"), "next_offset": offset + 100, "available_records" : available_records },), 200
    except Exception as e:
        logging.error(f"Error reading collection data: {e}")
        return jsonify({"error": "Failed to read data."}), 500
    finally:
        if connection:
            connection.close()






@routes.route('/api/text-to-sql/query', methods=['POST'])
def query_session():
    chat_connection = create_connection(PG_HOST, PG_USER, PG_PASSWORD, PG_DB_NAME_CHAT_HISTORY, PG_PORT)
    if not chat_connection:
        logging.info("Failed to connect to chat history database.")
        return jsonify({"error": "Failed to connect to chat history database."}), 500
    else:
        create_chat_history_table_if_not_exists(chat_connection)

    data = request.get_json()
    if not data or 'uuid' not in data or 'UserQuery' not in data:
        logging.warning("Query attempt with missing uuid or user_query.")
        return jsonify({"error": "Invalid request. 'uuid' and 'user_query' are required."}), 400

    uuid = data['uuid']
    user_query = data['UserQuery']
    logging.info(f"Query received for UUID: {uuid}, Query: \"{user_query}\"")

    with SESSIONS_LOCK:
        if uuid not in SESSIONS_DB:
            logging.warning(f"Session not found or expired for UUID: {uuid}")
            return jsonify({"error": "Session not found or has expired. Please start a new session."}), 404
        session_data = SESSIONS_DB[uuid]
        SESSIONS_DB[uuid]['last_accessed'] = datetime.now()

    table_name = session_data['table_name']
    column_metadata = session_data['column_metadata']
    available_columns = list(column_metadata.keys())
    timestamp_now = datetime.now()

    # Step 1: Get LLM response
    llm_response = get_query(user_query, table_name, available_columns)

    def create_guided_error_response(reason_from_llm, status_code=400):
        return jsonify({
            "error": "Could not process your query.",
            "reason": reason_from_llm
        }), status_code

    cursor = chat_connection.cursor()

    # Handle string LLM error
    if isinstance(llm_response, str):
        cursor.execute("""
            INSERT INTO chat_history_data (uuid, timestamp, user_query, llm_response, sql_query, high_chart_config, table_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (uuid, timestamp_now, user_query, llm_response, None, None, table_name))
        chat_connection.commit()
        return create_guided_error_response(f"Query processing engine error: {llm_response}", 500)

    if not isinstance(llm_response, dict):
        cursor.execute("""
            INSERT INTO chat_history_data (uuid, timestamp, user_query, llm_response, sql_query, high_chart_config, table_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (uuid, timestamp_now, user_query, "Malformed LLM response", None, None, table_name))
        chat_connection.commit()
        return create_guided_error_response("Unexpected response format from the query processing engine.", 500)

    if llm_response.get("is_greeting") and not llm_response.get("query"):
        cursor.execute("""
            INSERT INTO chat_history_data (uuid, timestamp, user_query, llm_response, sql_query, high_chart_config, table_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (uuid, timestamp_now, user_query, llm_response.get('is_greeting'), None, None, table_name))
        chat_connection.commit()
        return jsonify({"message": llm_response["is_greeting"], "reason": llm_response.get("reason", "")}), 200

    if llm_response.get("is_meta_data") and not llm_response.get("query"):
        cursor.execute("""
            INSERT INTO chat_history_data (uuid, timestamp, user_query, llm_response, sql_query, high_chart_config, table_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (uuid, timestamp_now, user_query, llm_response.get('is_meta_data'), None, None, table_name))
        chat_connection.commit()
        return jsonify({
            "message": llm_response.get("is_meta_data"),
            "reason": llm_response.get("reason", "")
        }), 200

    if not llm_response.get("is_valid") or not llm_response.get("is_safe"):
        reason = llm_response.get("reason", "Query is invalid or unsafe.")
        cursor.execute("""
            INSERT INTO chat_history_data (uuid, timestamp, user_query, llm_response, sql_query, high_chart_config, table_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (uuid, timestamp_now, user_query, reason, None, None, table_name))
        chat_connection.commit()
        return create_guided_error_response(reason)

    sql_query = llm_response.get("query")
    if not sql_query:
        cursor.execute("""
            INSERT INTO chat_history_data (uuid, timestamp, user_query, llm_response, sql_query, high_chart_config, table_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (uuid, timestamp_now, user_query, llm_response.get("reason", " "), None, None, table_name))
        chat_connection.commit()
        return jsonify({"error": "Expected valid query but not found."}), 500

    logging.info(f"[SQL] Generated SQL for UUID {uuid}: {sql_query}")
    llm_reason = llm_response.get("reason", "Query processed.")

    # --- Execute SQL ---
    connection = None
    try:
        connection = create_connection(PG_HOST, PG_USER, PG_PASSWORD, PG_DB, PG_PORT)
        if not connection:
            return jsonify({"error": "Database connection failed."}), 500

        db_cursor = connection.cursor()
        db_cursor.execute(sql_query)

        high_chart_config = None
        if db_cursor.description:
            results = db_cursor.fetchall()
            columns = [desc[0] for desc in db_cursor.description]
            df = pd.DataFrame(results, columns=columns)

            chart_config = generate_hightChart_config_prompt(sql_query, "df", columns)

            if chart_config and not df.empty:
                try:
                    evaluated_config = evaluate_code_blocks(chart_config, df)
                    evaluated_config = convert_decimals_to_floats(evaluated_config)

                    # Strip data before saving config to DB
                    high_chart_config = evaluated_config.copy()
                    for s in high_chart_config.get("series", []):
                        s["data"] = []

                    # Store chart + data in memory (optional)
                    with SESSIONS_LOCK:
                        SESSIONS_DB[uuid]['high_chart_config'] = evaluated_config
                        SESSIONS_DB[uuid]['chart_data'] = df.to_dict(orient='records')
                except Exception as e:
                    logging.error(f"[Chart] Generation failed: {e}")
                    with SESSIONS_LOCK:
                        SESSIONS_DB[uuid]['high_chart_config'] = None
                        SESSIONS_DB[uuid]['chart_data'] = []
        else:
            connection.commit()
            df = pd.DataFrame()

        result_payload = process_result(df)
        result_payload["executed_query"] = sql_query
        result_payload["llm_reason"] = llm_reason

        # Save final result in chat_history_data
        cursor.execute("""
            INSERT INTO chat_history_data (uuid, timestamp, user_query, llm_response, sql_query, high_chart_config, table_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            uuid, timestamp_now, user_query,
            llm_reason, sql_query,
            json.dumps(high_chart_config) if high_chart_config else None,
            table_name
        ))
        chat_connection.commit()

        return jsonify(result_payload), 200

    except pg.Error as e:
        logging.error(f"[PostgreSQL Error] {e}")
        if connection:
            connection.rollback()
        chat_connection.rollback()
        cursor.execute("""
            INSERT INTO chat_history_data (uuid, timestamp, user_query, llm_response, sql_query, high_chart_config, table_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            uuid, timestamp_now, user_query,
            llm_response.get('reason', ' '), sql_query,
            None, table_name
        ))
        chat_connection.commit()
        return create_guided_error_response(f"Database execution error: {str(e)}")

    except Exception as e:
        logging.error(f"[Unhandled Exception] {e}", exc_info=True)
        if connection:
            connection.rollback()
        chat_connection.rollback()
        cursor.execute("""
            INSERT INTO chat_history_data (uuid, timestamp, user_query, llm_response, sql_query, high_chart_config, table_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            uuid, timestamp_now, user_query,
            llm_response.get('reason', ' '), sql_query,
            None, table_name
        ))
        chat_connection.commit()
        return jsonify({"error": "Internal server error."}), 500

    finally:
        if connection:
            connection.close()
        cursor.close()
        chat_connection.close()


@routes.route('/api/text-to-sql/view_chart', methods=['POST'])
def view_chart():
    data = request.get_json()
    if not data or 'uuid' not in data:
        return "Missing 'uuid' in request.", 400

    uuid = data.get("uuid")

    with SESSIONS_LOCK:
        session_data = SESSIONS_DB.get(uuid)

        if not session_data:
            return "Session not found or expired.", 404

        chart_config = session_data.get("high_chart_config")
        chart_data = session_data.get("chart_data")

    
    if isinstance(chart_config, dict) and "high_chart_config" in chart_config:
        config = chart_config["high_chart_config"]
    elif isinstance(chart_config, dict):
        config = chart_config
    else:
        config = {}

    return render_template("chart.html", chart_config=config, chart_data=chart_data)

@routes.route('/api/text-to-sql/end_session', methods=['POST'])
def end_session():
   
    data = request.get_json()
    if not data or 'uuid' not in data:
        return jsonify({"error": "Invalid request. 'uuid' is required."}), 400

    uuid = data['uuid']
    table_name = SESSIONS_DB.get(uuid, {}).get('table_name')
    #drop_table(table_name)
    cleanup_session(uuid)
    
    
    return jsonify({"message": f"Session for UUID {uuid} has been successfully terminated."}), 200


@routes.route('/api/text-to-sql/list_tables', methods=['GET'])
def list_tables():
    uuid = request.args.get('uuid')
    if not uuid:
        return jsonify({"error": "Missing required parameter: uuid"}), 400

    try:
        connection = create_connection(PG_HOST, PG_USER, PG_PASSWORD, PG_DB, PG_PORT)
        if not connection:
            return jsonify({"error": "Database connection failed."}), 500

        cursor = connection.cursor()
        cursor.execute("""
            SELECT table_name 
            FROM table_meta_data 
            WHERE uuid = %s
            ORDER BY timestamp DESC
        """, (uuid,))
        tables = cursor.fetchall()
        table_list = [table[0] for table in tables]

        return jsonify({"tables": table_list}), 200
    except OperationalError as e:
        logging.error(f"Database error: {e}")
        return jsonify({"error": "Failed to retrieve tables for UUID"}), 500
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return jsonify({"error": "An unexpected error occurred."}), 500
    finally:
        if connection:
            connection.close()






@routes.route('/api/text-to-sql/get_chat_history', methods=['GET'])
def get_chat_history():
    uuid = request.args.get('uuid')
    table_name = request.args.get('table_name')

    if not uuid or not table_name:
        return jsonify({"error": "Missing 'uuid' or 'table_name' parameter"}), 400

    chat_connection = create_connection(PG_HOST, PG_USER, PG_PASSWORD, PG_DB_NAME_CHAT_HISTORY, PG_PORT)
    if not chat_connection:
        return jsonify({"error": "Database connection failed"}), 500

    try:
        cursor = chat_connection.cursor()
        cursor.execute("""
            SELECT id, timestamp, user_query, llm_response, sql_query, high_chart_config
            FROM chat_history_data
            WHERE uuid = %s AND table_name = %s
            ORDER BY timestamp ASC
        """, (uuid, table_name))

        history = []
        for row in cursor.fetchall():
            sql_query_exist = row[4] is not None and row[4].strip() != ""
            history.append({
                "id": row[0],
                "timestamp": str(row[1]),
                "user_query": row[2],
                "llm_response": row[3],
                "sql_query_exist" : sql_query_exist,
                "high_chart_config": row[5]
                
            })

        cursor.close()
        chat_connection.close()

        return jsonify({
            "uuid": uuid,
            "table_name": table_name,
            "history": history
        }), 200

    except Exception as e:
        if chat_connection:
            chat_connection.close()
        return jsonify({"error": "Failed to fetch chat history", "details": str(e)}), 500








@routes.route('/api/text-to-sql/run_stored_sql', methods=['POST'])
def run_stored_sql():
    data = request.get_json()

    if not data or  'id' not in data:
        return jsonify({"error": "Missing 'uuid' or 'id'"}), 400

    
    id = data['id']

    chat_connection = create_connection(PG_HOST, PG_USER, PG_PASSWORD, PG_DB_NAME_CHAT_HISTORY, PG_PORT)
    if not chat_connection:
        return jsonify({"error": "Database connection failed."}), 500

    try:
        cursor = chat_connection.cursor()
        cursor.execute("""
            SELECT sql_query 
            FROM chat_history_data 
            WHERE id = %s 
        """, (id,))
        row = cursor.fetchone()
        cursor.close()
        chat_connection.close()
    except Exception as e:
        logging.error(f"[Chat History Fetch Error] {e}", exc_info=True)
        return jsonify({"error": "Error fetching SQL query from history.", "details": str(e)}), 500

    if not row or not row[0]:
        return jsonify({"error": "No stored SQL query found for the given UUID and ID."}), 404

    sql_query = row[0]
    logging.info(f"[SQL Query to Execute]: {sql_query}")

    
    connection = None
    try:
        connection = create_connection(PG_HOST, PG_USER, PG_PASSWORD, PG_DB, PG_PORT)
        if not connection:
            return jsonify({"error": "Database connection failed."}), 500

        cursor = connection.cursor()
        cursor.execute(sql_query)

        if cursor.description:
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(results, columns=columns)

            result_payload = process_result(df)
            result_payload["executed_query"] = sql_query
            

            cursor.close()
            connection.close()

            return jsonify(result_payload), 200
        else:
            connection.commit()
            cursor.close()
            connection.close()
            return jsonify({
                "message": "Query executed successfully but returned no data.",
                "executed_query": sql_query
            }), 200

    except Exception as e:
        logging.error(f"[Stored SQL Execution Error] {e}", exc_info=True)
        if connection:
            connection.rollback()
        return jsonify({"error": "Failed to execute SQL query.", "details": str(e)}), 500






@routes.route('/api/text-to-sql/view_chart_history', methods=['POST'])
def render_chart():
    data = request.get_json()

    if not data or 'id' not in data:
        return "Missing 'id' in request.", 400

    id = data['id']
   

    chat_connection = create_connection(PG_HOST, PG_USER, PG_PASSWORD, PG_DB_NAME_CHAT_HISTORY, PG_PORT)
    if not chat_connection:
        return "Failed to connect to chat history DB.", 500

    try:
        cursor = chat_connection.cursor()
        cursor.execute(
                "SELECT sql_query, high_chart_config FROM chat_history_data WHERE id = %s",
                (id,)
            )

        row = cursor.fetchone()
        cursor.close()
        chat_connection.close()

        if not row:
            return "No chart config found for the given ID.", 404

        sql_query, raw_config = row

        
        config = None
        if raw_config:
            try:
                config_obj = ast.literal_eval(raw_config)
                config = config_obj.get("high_chart_config", config_obj)
            except Exception:
                try:
                    config_obj = json.loads(raw_config)
                    config = config_obj.get("high_chart_config", config_obj)
                except Exception as e:
                    logging.warning(f"[Config Parse Error] {e}")
                    config = None

      
        connection = create_connection(PG_HOST, PG_USER, PG_PASSWORD, PG_DB, PG_PORT)
        if not connection:
            return "Failed to connect to main DB.", 500

        cur = connection.cursor()
        cur.execute(sql_query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=columns)
        cur.close()
        connection.close()

        chart_data = df.to_dict(orient='records')

        return render_template("chart.html", chart_config=config, chart_data=chart_data)

    except Exception as e:
        logging.error(f"[Chart Render Error] {e}", exc_info=True)
        return f"Chart rendering failed: {str(e)}", 500








