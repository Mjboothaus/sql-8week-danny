import duckdb
from pathlib import Path
from loguru import logger

class DuckDBEngine:
    def __init__(self, db_path=None, rm_db=False):
        """
        Initialise the DuckDB engine.
        :param db_path: Path to the DuckDB database file. If None, an in-memory database is used.
        :param rm_db: Bool - remove db_path if it exists (default False)
        """
        if db_path:
            if rm_db and Path(db_path).exists():
                logger.info(f"Removing existing {db_path}\n")
                Path(db_path).unlink()
            logger.info(f"Persisting {db_path}\n")
            self.connection = duckdb.connect(database=db_path)
        else:
            logger.info("In-memory DuckDB\n")
            self.connection = duckdb.connect(read_only=False)

    def execute_sql_file(self, file_path):
        """
        Load SQL commands from a file and execute them.
        :param file_path: Path to the .sql file containing SQL commands.
        """
        try:
            sql_commands = Path(file_path).read_text()
            self.connection.execute(sql_commands)
        except Exception as e:
            logger.error(f"Error executing SQL file {file_path}: {e}")

    def query(self, sql, return_dataframe=True):
        """
        Execute a SQL query and return the results.
        :param sql: SQL query string.
        :param return_dataframe: If True, returns results as a pandas DataFrame, otherwise returns a single value.
        :return: Query results as a pandas DataFrame or a single value.
        """
        try:
            result = self.connection.execute(sql)
            if return_dataframe:
                return result.df()
            else:
                # Assuming the query is a 'count' or similar that returns a single value
                return result.fetchone()[0]
        except Exception as e:
            print(f"Error executing query: {e}")
            return None
        
    def load_tables_to_df(self):
        """
        Load all tables in the database into a dictionary of pandas DataFrames.
        :return: A tuple containing a list of table names and a dictionary with table names as keys and DataFrames as values.
        """
        tables_df = dict()
        try:
            table_names = self.query("SHOW TABLES;", return_dataframe=True)["name"].tolist()
            for table in table_names:
                tables_df[table] = self.query(f"SELECT * FROM {table}")
            logger.info(f"Loaded {table_names} to dataframes")
            return table_names, tables_df
        except Exception as e:
            print(f"Error loading tables to DataFrame: {e}")
            return [], {}
        
    def close(self):
        self.connection.close()


if __name__ == "__main__":
    engine = DuckDBEngine()

    DEMO_SQL = "/Users/mjboothaus/code/github/mjboothaus/sql-8week-danny/sql/week1.sql"

    engine.execute_sql_file(DEMO_SQL)
    
    tables_df = engine.query("SHOW TABLES;")

    df = dict()
    for table in tables_df["name"].tolist():
        df[table] = engine.query(f"SELECT * FROM {table}")
        print(df[table].info())

    count = engine.query("SELECT COUNT(*) FROM members", return_dataframe=False)
    print(count)