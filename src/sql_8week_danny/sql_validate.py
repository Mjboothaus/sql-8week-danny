import sqlglot





def validate(self, query):
    try:
        expressions = sqlglot.parse(query, dialect=self.dialect)
        if len(expressions) > 1:
            raise ValueError("Only one statement is allowed")
        disallowed_keywords = [
            "INSERT",
            "UPDATE",
            "DELETE",
            "DROP",
            "EXEC",
            "CALL",
            "ALTER",
            "GRANT",
        ]
        for expression in expressions:
            if any(token in expression.sql().upper() for token in disallowed_keywords):
                raise ValueError("Disallowed SQL keywords detected")
        return True, "Query is valid"
    except Exception as e:
        return False, str(e)
        

