import json
from pathlib import Path

import pandas as pd
import streamlit as st
from sql_8week_danny.sql_engine import DuckDBEngine

import tomllib
from loguru import logger


def read_config():
    config_toml = Path.cwd() / "app" / "app.toml"
    if not config_toml.exists():
        logger.error(f"File Not Found - {str(config_toml)}")
        raise FileNotFoundError(f"{str(config_toml)}")
    with open(config_toml, "rb") as f:
        logger.info(f"Config loaded from {str(config_toml)}")
        return tomllib.load(f)


def setup_DuckDB(config):
    CREATE_SQL = Path.cwd() / config["sql"]["create_sql"]
    db = DuckDBEngine(config)
    db.execute_sql_file(CREATE_SQL)
    return db


def handle_query(db, query):
    validate = db.validate(query)
    if validate[0]:  # TODO: Wire in validation of SQL
        result = db.query(query)
        if isinstance(result, pd.DataFrame):
            # For DataFrame, convert to markdown for chat display
            # return result.to_markdown()
            return result
        elif result is not None:
            return str(result)
        else:
            return "No results returned."
    else:
        return f"ERROR: {validate[1]}"


def add_response_to_chat_hist(response):
    # if isinstance(response, pd.DataFrame):
    #    # Convert DataFrame to a list of records for JSON serialization
    #    serial_response = response.to_dict('records')
    # else:
    serial_response = response
    st.session_state.chat_hist.append({config["app"]["response_name"]: serial_response})


def convert_chat_hist_to_serial(chat_hist):
    serial_history = []
    for entry in chat_hist:
        serial_entry = {}
        for key, value in entry.items():
            if isinstance(value, pd.DataFrame):
                # Convert DataFrame to a list of records (dictionaries)
                value = value.map(
                    lambda x: x.isoformat() if isinstance(x, pd.Timestamp) else x
                ).to_dict("records")
            # Now handle the case where value is a list (of dictionaries, potentially with Timestamps)
            if isinstance(value, list):
                # Ensure each dictionary in the list handles Timestamps
                value = [
                    {
                        k: v.isoformat() if isinstance(v, pd.Timestamp) else v
                        for k, v in record.items()
                    }
                    for record in value
                ]
            serial_entry[key] = value
        serial_history.append(serial_entry)
    return serial_history


def get_sql_queries_only(chat_hist):
    sql_queries = [
        entry[config["app"]["prompt_name"]]
        for entry in chat_hist
        if config["app"]["prompt_name"] in entry
    ]
    return sql_queries


def default_handler(x):
    if isinstance(x, pd.Timestamp):
        return x.isoformat()
    raise TypeError(f"Object of type {type(x).__name__} is not JSON serial")


config = read_config()
db = setup_DuckDB(config)


# Create UI


st.sidebar.markdown("### Tables:")
table_info = {}
for table in db.table_names:
    table_info[table] = db.get_table_info(table)
    st.sidebar.button(
        label=table,
        use_container_width=True,
        help=f"Table: {table_info[table].name} - {table_info[table].row_count} rows\n {table_info[table].description}",
    )


if "chat_hist" not in st.session_state:
    st.session_state.chat_hist = []

st.sidebar.markdown(f"## {config['app']['title']}")


error_sidebar = st.sidebar.container()

user_query = st.chat_input("Enter your SQL query:", key="chat_input")

# TODO: Validate/check the query (prob in handle_response)


if user_query:
    if db.validate(user_query)[0]:
        user_query = db._check_and_validate_sql(user_query)

    response = handle_query(db, user_query)
    if isinstance(response, pd.DataFrame):
        st.session_state.chat_hist.append({config["app"]["prompt_name"]: user_query})
        add_response_to_chat_hist(response)
    else:
        error_sidebar.error(response)

for chat in st.session_state.chat_hist:
    if config["app"]["prompt_name"] in chat:
        st.caption(f"{config['app']['prompt_name']}:")
        st.code(f"{chat[config['app']['prompt_name']]}")
    elif config["app"]["response_name"] in chat:
        st.caption(f"{config['app']['response_name']}:")
        st.write(chat[config["app"]["response_name"]])
        st.markdown("----")


sql_limit = st.sidebar.number_input("Limit rows:", min_value=1, value=10, step=1)


# Download buttons

if st.sidebar.button("Download Query History"):
    serial_chat_hist = convert_chat_hist_to_serial(st.session_state.chat_hist)
    chat_hist_json = json.dumps(serial_chat_hist, indent=4, default=default_handler)

    # Create a download button for the JSON file
    st.sidebar.download_button(
        label="Download: Chat History",
        data=chat_hist_json,
        file_name="chat_hist.json",
        mime="application/json",
    )

    sql_queries = get_sql_queries_only(st.session_state.chat_hist)
    sql_queries_str = "\n".join(sql_queries)

    st.sidebar.download_button(
        label="Download: SQL Queries only",
        data=sql_queries_str,
        file_name="sql_queries.sql",
        mime="text/plain",
    )
