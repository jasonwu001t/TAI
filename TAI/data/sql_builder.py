import os

class SQLBuilder:
    def __init__(self):
        self.select_clause = ""
        self.from_clause = ""
        self.join_clause = ""
        self.where_clause = ""
        self.order_by_clause = ""
        self.with_clause = ""
        self.union_clause = ""
        self.create_table_clause = ""
        self.drop_table_clause = ""

    def select(self, *columns):
        columns_str = ",\n  ".join(columns)
        self.select_clause = f"SELECT\n  {columns_str}"
        return self

    def select_distinct(self, *columns):
        columns_str = ",\n  ".join(columns)
        self.select_clause = f"SELECT DISTINCT\n  {columns_str}"
        return self

    def from_table(self, table):
        self.from_clause = f"FROM\n  {table}"
        return self

    def join(self, table, condition, join_type="INNER"):
        join_clause = f"{join_type} JOIN\n  {table}\n  ON {condition}"
        if self.join_clause:
            self.join_clause += f"\n{join_clause}"
        else:
            self.join_clause = join_clause
        return self

    def union_all(self, query):
        union_clause = f"UNION ALL\n{query}"
        if self.union_clause:
            self.union_clause += f"\n{union_clause}"
        else:
            self.union_clause = union_clause
        return self

    def with_cte(self, cte_name, cte_query):
        with_clause = f"{cte_name} AS (\n{cte_query}\n)"
        if self.with_clause:
            self.with_clause += f",\n{with_clause}"
        else:
            self.with_clause = f"WITH\n{with_clause}"
        return self

    def where(self, *conditions):
        conditions_str = "\n  AND ".join(conditions)
        self.where_clause = f"WHERE\n  {conditions_str}"
        return self

    def order_by(self, *columns, ascending=True):
        direction = "ASC" if ascending else "DESC"
        columns_str = ",\n  ".join(columns)
        self.order_by_clause = f"ORDER BY\n  {columns_str} {direction}"
        return self

    def create_table(self, table_name, columns):
        columns_def = ",\n  ".join(columns)
        self.create_table_clause = f"CREATE TABLE {table_name} (\n  {columns_def}\n)"
        return self

    def drop_table(self, table_name):
        self.drop_table_clause = f"DROP TABLE {table_name}"
        return self

    def nullif(self, column1, column2):
        return f"NULLIF({column1}, {column2})"

    def sum(self, column):
        return f"SUM({column})"

    def select_as(self, column, alias):
        return f"{column} AS {alias}"

    def build(self):
        query_parts = [
            self.with_clause,
            self.select_clause,
            self.from_clause,
            self.join_clause,
            self.where_clause,
            self.order_by_clause,
            self.union_clause,
            self.create_table_clause,
            self.drop_table_clause,
        ]
        query = "\n".join(part for part in query_parts if part)
        return query

    def save_to_file(self, filename):
        query = self.build()
        with open(filename, 'w') as file:
            file.write(query)
        print(f"SQL query saved to {filename}")

    def append_sql(self, clause_type, clause):
        if clause_type.lower() == "where":
            if self.where_clause:
                self.where_clause += f"\n  AND {clause}"
            else:
                self.where_clause = f"WHERE\n  {clause}"
        elif clause_type.lower() == "join":
            self.join_clause += f"\n{clause}"
        elif clause_type.lower() == "order_by":
            if self.order_by_clause:
                self.order_by_clause += f",\n  {clause}"
            else:
                self.order_by_clause = f"ORDER BY\n  {clause}"
        elif clause_type.lower() == "select":
            if self.select_clause:
                self.select_clause = self.select_clause.replace(
                    "SELECT", f"SELECT\n  {clause},"
                )
            else:
                self.select_clause = f"SELECT\n  {clause}"
        # Add more clause types as needed
        else:
            raise ValueError(f"Unsupported clause type: {clause_type}")
        return self

# Example usage:
if __name__ == "__main__":
    builder = SQLBuilder()
    query = (
        builder.with_cte("cte_name", "SELECT * FROM other_table")
        .select(
            "column1", 
            builder.select_as("column2", "col2"), 
            builder.nullif("column3", "0"), 
            builder.sum("column4")
        )
        .from_table("table_name")
        .join("another_table", "table_name.id = another_table.id")
        .where("condition1 = 'value1'", "condition2 = 'value2'")
        .order_by("column1", "column2", ascending=True)
        .union_all("SELECT column1, column2, column3 FROM table2")
        .create_table("new_table", ["column1 INT", "column2 VARCHAR(100)"])
        .drop_table("old_table")
        .build()
    )
    
    # Append additional SQL clauses
    builder.append_sql("where", "condition3 = 'value3'")
    builder.append_sql("order_by", "column3 DESC")

    # Save the updated query to a file
    builder.save_to_file("updated_query.sql")

    # Print the updated query
    print(builder.build())
