import json
from pathlib import Path

import pandas as pd
import streamlit as st
from pandas import Timestamp
from sql_8week_danny.sql_engine import DuckDBEngine

WEEK1_SQL = Path.cwd() / "sql/week1.sql"
db = DuckDBEngine()
db.execute_sql_file(WEEK1_SQL)

st.sidebar.markdown(f"## Data with Danny - {WEEK1_SQL.stem.capitalize()}")


# query_input = st.sidebar.text_input("Enter your SQL query here:", "")

limit = st.sidebar.number_input("Limit rows:", min_value=1, value=10, step=1)


# UI for choosing input location is not needed for chat_input
# Removed for simplicity in this context

# Maintain a session-level chat history
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []


# Function to handle query execution and response
def handle_query(query):
    if query.lower().startswith("select"):
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
        return "Only SELECT queries are allowed."


def add_response_to_chat_history(response):
    # if isinstance(response, pd.DataFrame):
    #    # Convert DataFrame to a list of records for JSON serialization
    #    serializable_response = response.to_dict('records')
    # else:
    serializable_response = response
    st.session_state.chat_history.append({"DuckDB": serializable_response})


# Chat input for SQL queries
user_query = st.chat_input("Enter your SQL query:", key="chat_input")

# Process and display chat history
if user_query:
    # Add user query to chat history
    st.session_state.chat_history.append({"User": user_query})
    # Execute query and get response
    response = handle_query(user_query)
    # Add response to chat history
    add_response_to_chat_history(response)

# Display chat history
for chat in st.session_state.chat_history:
    if "User" in chat:
        st.caption("User:")
        st.code(f"{chat['User']}")
    elif "DuckDB" in chat:
        st.caption("Result:")
        st.write(chat["DuckDB"])
        st.markdown("----")

# Note: Downloading chat history as a file is omitted for brevity
# You can add similar functionality as shown previously if needed


def convert_chat_history_to_serializable(chat_history):
    serializable_history = []
    for entry in chat_history:
        serializable_entry = {}
        for key, value in entry.items():
            # Check if the value is a DataFrame directly
            if isinstance(value, pd.DataFrame):
                # Convert DataFrame to a list of records (dictionaries)
                value = value.map(lambda x: x.isoformat() if isinstance(x, pd.Timestamp) else x).to_dict(
                    "records"
                )
            # Now handle the case where value is a list (of dictionaries, potentially with Timestamps)
            if isinstance(value, list):
                # Ensure each dictionary in the list handles Timestamps
                value = [
                    {k: v.isoformat() if isinstance(v, pd.Timestamp) else v for k, v in record.items()}
                    for record in value
                ]
            serializable_entry[key] = value
        serializable_history.append(serializable_entry)
    return serializable_history


def get_sql_queries_only(chat_history):
    sql_queries = [entry["User"] for entry in chat_history if "User" in entry]
    return sql_queries


sql_queries = get_sql_queries_only(st.session_state.chat_history)
sql_queries_str = "\n".join(sql_queries)

# Assuming st.session_state.chat_history is already populated, including DataFrames

if st.sidebar.button("Download Query History"):
    serializable_chat_history = convert_chat_history_to_serializable(st.session_state.chat_history)

    st.write(serializable_chat_history)
    # Serialize the serializable chat history to JSON

    def default_handler(x):
        if isinstance(x, pd.Timestamp):
            return x.isoformat()
        raise TypeError(f"Object of type {type(x).__name__} is not JSON serializable")

    chat_history_json = json.dumps(serializable_chat_history, indent=4, default=default_handler)

    # Create a download button for the JSON file
    st.sidebar.download_button(
        label="Download Chat History",
        data=chat_history_json,
        file_name="chat_history.json",
        mime="application/json",
    )

    st.sidebar.download_button(
        label="Download SQL Queries only",
        data=sql_queries_str,
        file_name="sql_queries.sql",
        mime="text/plain",
    )
