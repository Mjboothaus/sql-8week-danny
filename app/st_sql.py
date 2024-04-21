import streamlit as st
import pandas as pd
from pathlib import Path
from sql_8week_danny.sql_engine import DuckDBEngine

WEEK1_SQL = Path.cwd() / "sql/week1.sql"
db = DuckDBEngine()
db.execute_sql_file(WEEK1_SQL)

st.sidebar.markdown(f"## Data with Danny - {WEEK1_SQL.stem.capitalize()}")

# UI for choosing input location
input_location = st.sidebar.selectbox("Choose input location", ["Main", "Sidebar"])

query_input = None
if input_location == "Main":
    query_input = st.text_input("Enter your SQL query here:", "")
else:
    query_input = st.sidebar.text_input("Enter your SQL query here:", "")

limit = st.sidebar.number_input("Limit rows:", min_value=1, value=10, step=1)


# Maintain a session-level query history
if "query_history" not in st.session_state:
    st.session_state.query_history = []

# Execute query and display results
if st.button("Run Query"):
    if query_input.lower().startswith("select"):
        query_with_limit = f"{query_input} LIMIT {limit}"
        result = db.query(query_with_limit)
        if isinstance(result, pd.DataFrame):
            st.dataframe(result)
        elif result is not None:
            st.write(result)
        else:
            st.write("No results returned.")
        # Add to query history
        st.session_state.query_history.append(query_input)
    else:
        st.error("Only SELECT queries are allowed.")

# Display query history and allow re-execution
if st.session_state.query_history:
    st.sidebar.write("Query History:")
    for idx, query in enumerate(st.session_state.query_history):
        if st.sidebar.button(f"Run #{idx+1}: {query}"):
            result = db.query(f"{query} LIMIT {limit}")
            if isinstance(result, pd.DataFrame):
                st.dataframe(result)
            elif result is not None:
                st.write(result)
            else:
                st.write("No results returned.")

# Option to download query history
if st.sidebar.button("Download Query History"):
    history_str = "\n".join(st.session_state.query_history)
    st.download_button("Download History", history_str, "query_history.txt")
