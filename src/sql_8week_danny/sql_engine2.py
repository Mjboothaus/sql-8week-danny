# from dataclasses import dataclass, field
from pathlib import Path
# from typing import List, Optional

import duckdb
import sqlglot
import tomllib

# from IPython.display import Markdown, display
from loguru import logger


class DuckDBEngine:
    def __init__(self, config_file=None, db_path=None, rm_db=False):
        self.config = self.load_config(config_file) if config_file else None
        self.connection = self.setup_database(db_path, rm_db)
        self.table_names = self.refresh_table_names()

    def load_config(self, config_file):
        config_path = Path.cwd() / config_file
        if not config_path.exists():
            logger.error(f"File Not Found - {str(config_path)}")
            raise FileNotFoundError(f"{str(config_path)}")
        with open(config_path, "rb") as f:
            config = tomllib.load(f)
            logger.info(f"Config loaded from {str(config_path)}")
            return config.get("sql", {})

    def setup_database(self, db_path, rm_db):
        if db_path:
            if rm_db:
                self.remove_existing_database_files(db_path)
            logger.info(f"Persisting {db_path}")
            return duckdb.connect(database=db_path)
        else:
            logger.info("Using in-memory DuckDB")
            return duckdb.connect(read_only=False)

    def remove_existing_database_files(self, db_path):
        logger.info(f"Removing existing {db_path} and associated WAL file")
        Path(db_path).unlink(missing_ok=True)
        Path(f"{db_path}.wal").unlink(missing_ok=True)

    def refresh_table_names(self):
        try:
            result = self.connection.execute("SHOW TABLES;")
            return [row[0] for row in result.fetchall()]
        except Exception as e:
            logger.error(f"Error fetching table names: {e}")
            return []

    def execute_sql_file(self, file_path):
        try:
            sql_commands = Path(file_path).read_text()
            self.connection.execute(sql_commands)
            self.refresh_table_names()
        except Exception as e:
            logger.error(f"Error executing SQL file {file_path}: {e}")

    def validate_query(self, query):
        try:
            expressions = sqlglot.parse(query, dialect="duckdb")
            if len(expressions) > 1:
                raise ValueError("Only one statement is allowed")
            self.check_disallowed_keywords(expressions)
            return True
        except Exception as e:
            logger.error(f"Validation error: {e}")
            return False

    def check_disallowed_keywords(self, expressions):
        disallowed_keywords = self.config.get("disallowed_keywords", [])
        for expression in expressions:
            if any(token in expression.sql().upper() for token in disallowed_keywords):
                raise ValueError("Disallowed SQL keywords detected")

    def execute_query(self, sql, force_dataframe=False):
        try:
            result = self.connection.execute(sql)
            if force_dataframe:
                return result.df()
            return result.fetchone()[0] if result.rowcount == 1 else result.df()
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return None

    def query(self, sql, force_dataframe=False):
        if self.validate_query(sql):
            return self.execute_query(sql, force_dataframe)
        else:
            return None

    def close(self):
        self.connection.close()

    # Additional methods (load_tables_to_df, display_all_table_info, etc.) would be refactored similarly.
