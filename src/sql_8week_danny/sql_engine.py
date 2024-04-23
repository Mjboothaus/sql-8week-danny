from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import duckdb
import sqlglot
from IPython.display import Markdown, display
from loguru import logger


@dataclass
class TableColumn:
    name: str
    data_type: str
    is_nullable: bool
    default: Optional[str] = None
    constraints: Optional[str] = None


@dataclass
class TableIndex:
    name: str
    columns: List[str]


@dataclass
class TableInfo:
    name: str
    schema: List[TableColumn] = field(default_factory=list)
    row_count: Optional[int] = None
    primary_key: Optional[List[str]] = None
    indexes: List[TableIndex] = field(default_factory=list)
    creation_date: Optional[str] = None
    last_modified_date: Optional[str] = None
    tablespace: Optional[str] = None
    description: Optional[str] = None


LOG_FILE = "duckdb_db.log"

# logger.remove()  # Uncomment to turn off console logging
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
            expressions = sqlglot.parse(query, dialect="duckdb")
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

    def _check_sql(self, sql):
        try:
            return sqlglot.transpile(sql, read="duckdb", write="duckdb")[0]
        except sqlglot.errors.ParseError as e:
            print(
                f"SQL ERROR: {e.errors[0]['description']}\nQuery: '{sql[:e.errors[0]['col']]}'"
            )
            # print(e.errors)
            return None

    def _check_and_validate_sql(self, sql):
        validate = self.validate(sql)
        if validate[0]:
            return self._check_sql(sql)
        else:
            return f"#### ERROR: {validate[1]}"

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

    def get_table_info(self, table_name: str) -> TableInfo:
        # Execute PRAGMA show_tables_expanded and convert to DataFrame
        result_df = self.query("PRAGMA show_tables_expanded;")

        # Filter the DataFrame for the specific table
        table_info_df = result_df[result_df["name"] == table_name].iloc[0]

        # Extract schema information
        column_names = table_info_df["column_names"]
        column_types = table_info_df["column_types"]

        schema = [
            TableColumn(
                name=col_name, data_type=col_type, is_nullable=True
            )  # Assuming nullable, adjust as necessary
            for col_name, col_type in zip(column_names, column_types)
        ]

        # For row count, execute a separate query
        row_count = self._get_table_row_count(table_name)

        # Construct and return the TableInfo instance
        return TableInfo(
            name=table_name,
            schema=schema,
            row_count=row_count,
            # Assuming defaults for other fields, adjust as necessary
            primary_key=None,  # You might need additional logic to determine the primary key
            indexes=[],  # Adjust based on available index information or separate logic
            creation_date=None,
            last_modified_date=None,
            tablespace=None,
            description=self.config[table_name]["description"],  # from app.toml
        )

    def _get_table_row_count(self, table_name: str) -> int:
        query = f"SELECT COUNT(*) FROM {table_name}"
        result = self.connection.execute(query).fetchone()
        return result[0] if result else 0

    def _get_table_schema(self, table_name: str) -> List[TableColumn]:
        result = self.connection.execute(
            f"PRAGMA table_info('{table_name}')"
        ).fetchall()
        return [
            TableColumn(
                name=row[1],
                data_type=row[2],
                is_nullable=row[3] == 0,
                default=row[4],
                constraints=None,  # You might need custom logic to parse constraints
            )
            for row in result
        ]

    def _get_table_row_count(self, table_name: str) -> int:
        result = self.connection.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()
        return result[0] if result else 0

    def _get_table_indexes(self, table_name: str) -> List[TableIndex]:
        result = self.connection.execute(
            f"PRAGMA show_indexes('{table_name}')"
        ).fetchall()
        indexes = []
        for row in result:
            index_name = row[1]
            column_name = row[4]
            # This simplistic approach assumes one column per index; you might need to adjust it
            indexes.append(TableIndex(name=index_name, columns=[column_name]))
        return indexes

    def close(self):
        self.connection.close()


if __name__ == "__main__":
    DEMO_SQL = Path.cwd() / "sql/week1.sql"

    db = DuckDBEngine()
    db.execute_sql_file(DEMO_SQL)
    df = db.load_tables_to_df()
    db.display_all_table_info(notebook=False)

    print(f"Members count: {db.query('SELECT COUNT(*) FROM members')}")
