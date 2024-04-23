from pathlib import Path

import duckdb
import sqlglot
from IPython.display import Markdown, display
from loguru import logger

LOG_FILE = "duckdb_db.log"

# logger.remove()  # Turn off console logging
logger.add(
    LOG_FILE,
    format="{time:ddd d MMM YYYY - HH:mm:ss} {level} {message}",
    rotation="1 MB",
)


class DuckDBEngine:
    def __init__(self, config=None, db_path=None, rm_db=False):
        """
        Initialise the DuckDB db.
        :param db_path: Path to the DuckDB database file. If None, an in-memory database is used.
        :param rm_db: Bool - remove db_path if it exists (default False)
        """
        self.table_names = []
        if config:
            self.config = config["sql"]
        else:
            self.config = None
        if db_path:
            if rm_db and Path(db_path).exists():
                logger.info(f"Removing existing {db_path}")
                logger.info(f"Removing existing {db_path}.wal")
                Path(db_path).unlink(missing_ok=True)
                Path(f"{db_path}.wal").unlink(missing_ok=True)

            logger.info(f"Persisting {db_path}\n")
            self.connection = duckdb.connect(database=db_path)
        else:
            logger.info("In-memory DuckDB")
            self.connection = duckdb.connect(read_only=False)

    def get_connection(self):
        return self.connection

    def execute_sql_file(self, file_path):
        """
        Load SQL commands from a file and execute them.
        :param file_path: Path to the .sql file containing SQL commands.
        """
        try:
            sql_commands = Path(file_path).read_text()
            self.connection.execute(sql_commands)
            self.refresh_table_names()
            return sql_commands
        except Exception as e:
            logger.error(f"Error executing SQL file {file_path}: {e}")
            return None

    def refresh_table_names(self):
        """
        Refresh the list of table names from the database.
        """
        try:
            result = self.connection.execute("SHOW TABLES;")
            self.table_names = [row[0] for row in result.fetchall()]
        except Exception as e:
            print(f"Error fetching table names: {e}")

    def validate(self, query):
        try:
            expressions = sqlglot.parse(query, dialect=self.dialect)
            if len(expressions) > 1:
                raise ValueError("Only one statement is allowed")
            disallowed_keywords = self.config["disallowed_keywords"]
            for expression in expressions:
                if any(
                    token in expression.sql().upper() for token in disallowed_keywords
                ):
                    raise ValueError("Disallowed SQL keywords detected")
            return True, "Query is valid"
        except Exception as e:
            return False, str(e)

    def query(self, sql, force_dataframe=False):
        """
        Execute a SQL query and automatically determine the return type, with an option to force a DataFrame result.
        :param sql: SQL query string.
        :param force_dataframe: If True, forces the result to be a pandas DataFrame.
        :return: Query results as a pandas DataFrame, a single value, or None if nothing is returned.
        """
        try:
            result = self.connection.execute(sql)
            # Fetch the result to inspect its shape
            fetched_result = result.fetchall()

            # Determine if the result is a single value
            is_single_value = len(fetched_result) == 1 and len(fetched_result[0]) == 1

            if force_dataframe or not is_single_value:
                # Re-execute the query for DataFrame to ensure full result set is returned
                result = self.connection.execute(sql)
                return result.df()
            elif is_single_value:
                # Return the single value directly
                return fetched_result[0][0]
            else:
                return None
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def load_tables_to_df(self):
        """
        Load all tables in the database into a dictionary of pandas DataFrames.
        :return: A tuple containing a list of table names and a dictionary with table names as keys and DataFrames as values.
        """
        if not self.table_names:
            print("No tables found. Ensure the SQL create file has been loaded.")
            return {}
        tables_df = dict()
        try:
            for table in self.table_names:
                tables_df[table] = self.query(f"SELECT * FROM {table}")
            logger.info(f"Loaded {self.table_names} to dataframes")
            return tables_df
        except Exception as e:
            print(f"Error loading tables to DataFrames: {e}")
            return {}

    def display_all_table_info(self, display_tables=False, notebook=True):
        df = dict()
        for table in sorted(self.table_names):
            df[table] = self.query(f"SELECT * FROM {table}")
            if notebook:
                display(Markdown(f"# {table}: {len(df[table])} records"))
            else:
                print(f"**{table}**: {len(df)} records")
            if notebook and display_tables:
                display(df[table])
        return df

    def close(self):
        self.connection.close()


if __name__ == "__main__":
    DEMO_SQL = Path.cwd() / "sql/week1.sql"

    db = DuckDBEngine()
    db.execute_sql_file(DEMO_SQL)
    df = db.load_tables_to_df()
    db.display_all_table_info(notebook=False)

    print(f"Members count: {db.query('SELECT COUNT(*) FROM members')}")
