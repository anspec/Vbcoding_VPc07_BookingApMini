import os
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extras import RealDictCursor


class PostgresDriver:
    """Reusable PostgreSQL driver for external projects."""

    def __init__(self, connection_params: dict[str, Any] | None = None) -> None:
        load_dotenv(dotenv_path=self._default_dotenv_path(), override=True)
        self._connection_params = connection_params or self.get_connection_params()
        self._connection: psycopg2.extensions.connection | None = None

    @staticmethod
    def _default_dotenv_path() -> Path:
        return Path(__file__).resolve().parent / ".env"

    @staticmethod
    def get_connection_params() -> dict[str, Any]:
        load_dotenv(dotenv_path=PostgresDriver._default_dotenv_path(), override=True)

        def _env_value(name: str, default: str = "") -> str:
            value = os.getenv(name, default)
            return value.strip().strip('"').strip("'")

        return {
            "host": _env_value("DB_HOST", "localhost"),
            "port": int(_env_value("DB_PORT", "5432")),
            "dbname": _env_value("DB_NAME", "test"),
            "user": _env_value("DB_USER", "postgres"),
            "password": _env_value("DB_PASSWORD", ""),
            "connect_timeout": 5,
        }

    def connect(self) -> None:
        if self._connection is None or self._connection.closed != 0:
            try:
                self._connection = psycopg2.connect(**self._connection_params)
            except psycopg2.Error as exc:
                raise RuntimeError(self._format_db_error(exc)) from exc

    def close(self) -> None:
        if self._connection and self._connection.closed == 0:
            self._connection.close()

    def __enter__(self) -> "PostgresDriver":
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def _ensure_connection(self) -> psycopg2.extensions.connection:
        self.connect()
        if self._connection is None:
            raise RuntimeError("Failed to initialize PostgreSQL connection.")
        return self._connection

    def _run_query(
        self,
        query: sql.Composable,
        params: Sequence[Any] | None = None,
        *,
        fetch: str = "none",
        commit: bool = False,
    ) -> Any:
        connection = self._ensure_connection()
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                if fetch == "one":
                    result = cursor.fetchone()
                elif fetch == "all":
                    result = cursor.fetchall()
                else:
                    result = None

            if commit:
                connection.commit()
            return result
        except psycopg2.Error as exc:
            if commit:
                connection.rollback()
            raise RuntimeError(self._format_db_error(exc)) from exc
        except Exception as exc:
            if commit:
                connection.rollback()
            raise RuntimeError(
                f"Database operation failed. Code: N/A. Description: {str(exc).strip()}"
            ) from exc

    @staticmethod
    def _format_db_error(exc: psycopg2.Error) -> str:
        error_code = exc.pgcode or "N/A"
        error_description = (exc.pgerror or str(exc)).strip()
        return f"Database operation failed. Code: {error_code}. Description: {error_description}"

    @staticmethod
    def _build_where_clause(filters: Mapping[str, Any] | None) -> tuple[sql.Composable, list[Any]]:
        if not filters:
            return sql.SQL(""), []

        conditions: list[sql.Composable] = []
        values: list[Any] = []

        for column, value in filters.items():
            column_ident = sql.Identifier(column)
            if value is None:
                conditions.append(sql.SQL("{} IS NULL").format(column_ident))
            elif isinstance(value, (list, tuple, set)):
                items = list(value)
                if not items:
                    conditions.append(sql.SQL("FALSE"))
                else:
                    placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(items))
                    conditions.append(
                        sql.SQL("{} IN ({})").format(column_ident, placeholders)
                    )
                    values.extend(items)
            else:
                conditions.append(
                    sql.SQL("{} = {}").format(column_ident, sql.Placeholder())
                )
                values.append(value)

        where_clause = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(conditions)
        return where_clause, values

    @staticmethod
    def _columns_sql(columns: Iterable[str] | None) -> sql.Composable:
        if not columns:
            return sql.SQL("*")
        return sql.SQL(", ").join(sql.Identifier(column) for column in columns)
    def create_table_from_model(self, model) -> None:
        """
        Создает таблицу в базе данных на основе SQLAlchemy модели, если она не существует.
        """
        sql_query = self._generate_create_table_sql(model)
        print(f"создаем таблицу {model.__tablename__} с SQL: {sql_query}")
        self.create_table(sql_query)

    @staticmethod
    def _generate_create_table_sql(model) -> str:
        """
        Генерирует SQL запрос для создания таблицы на основе SQLAlchemy модели.
        Поддерживает Integer, String, nullable, primary_key, unique, default и ForeignKey.
        """
        table_name = model.__tablename__
        columns_sql = []
        for column in model.__table__.columns:
            column_sql = f'"{column.name}"'
            coltype = column.type
            is_integer = coltype.__class__.__name__ == "Integer"
            is_auto_increment_pk = (
                column.primary_key
                and is_integer
                and column.autoincrement in (True, "auto")
            )

            # Типы данных
            if hasattr(coltype, "length") and getattr(coltype, "length", None):
                # Например: String(50)
                coltype_sql = f'VARCHAR({coltype.length})'
            elif is_integer:
                coltype_sql = "INTEGER"
            elif coltype.__class__.__name__ == "String":
                coltype_sql = "VARCHAR"
            elif coltype.__class__.__name__ == "DateTime":
                coltype_sql = "TIMESTAMP"
            else:
                coltype_sql = str(coltype).upper()

            column_sql += f" {coltype_sql}"

            # Опции
            if is_auto_increment_pk:
                # PostgreSQL-compatible auto increment for integer PK columns.
                column_sql += " GENERATED BY DEFAULT AS IDENTITY"
            if column.primary_key:
                column_sql += " PRIMARY KEY"
            if hasattr(column, "unique") and column.unique:
                column_sql += " UNIQUE"
            if not column.nullable and not column.primary_key:
                column_sql += " NOT NULL"
            if hasattr(column, "default") and column.default is not None and column.default.arg is not None:
                default_val = column.default.arg
                if callable(default_val):
                    # например datetime.utcnow
                    default_val = "CURRENT_TIMESTAMP"
                elif isinstance(default_val, str):
                    default_val = f"'{default_val}'"
                column_sql += f" DEFAULT {default_val}"
            if column.foreign_keys:
                for fk in column.foreign_keys:
                    ref_parts = fk.target_fullname.split(".")
                    if len(ref_parts) == 2:
                        ref_table, ref_column = ref_parts
                        reference = f'"{ref_table}"("{ref_column}")'
                    elif len(ref_parts) == 3:
                        ref_schema, ref_table, ref_column = ref_parts
                        reference = f'"{ref_schema}"."{ref_table}"("{ref_column}")'
                    else:
                        reference = fk.target_fullname
                    column_sql += f" REFERENCES {reference}"

            columns_sql.append(column_sql)

        columns_str = ",\n    ".join(columns_sql)
        create_stmt = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" (\n    {columns_str}\n);"
        return create_stmt

    def create_table(self, sql_query: str) -> None:
        """Create one table using raw SQL query like CREATE TABLE ..."""
        if not sql_query or not sql_query.strip():
            raise ValueError("`sql_query` cannot be empty.")

        normalized = sql_query.strip().upper()
        if not normalized.startswith("CREATE TABLE"):
            raise ValueError("`sql_query` must start with 'CREATE TABLE'.")

        self._run_query(sql.SQL(sql_query), commit=True)

    def drop_table(self, table: str, if_exists: bool = True, cascade: bool = False) -> None:
        """Drop table by name."""
        if not table or not table.strip():
            raise ValueError("`table` cannot be empty.")

        query = sql.SQL("DROP TABLE {}{}{}").format(
            sql.SQL("IF EXISTS ") if if_exists else sql.SQL(""),
            sql.Identifier(table),
            sql.SQL(" CASCADE") if cascade else sql.SQL(""),
        )
        self._run_query(query, commit=True)

    def get_connection_info(self) -> dict[str, Any]:
        query = sql.SQL(
            """
            SELECT
                current_database() AS db_name,
                current_user AS db_user,
                current_schema() AS schema_name
            """
        )
        result = self._run_query(query, fetch="one")
        return result or {}

    def insert(
        self,
        table: str,
        data: Mapping[str, Any],
        returning: Iterable[str] | None = None,
    ) -> dict[str, Any] | None:
        """Insert a new row into table."""
        if not data:
            raise ValueError("`data` cannot be empty for insert operation.")

        columns = list(data.keys())
        values = list(data.values())
        placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(columns))
        returning_sql = self._columns_sql(returning)

        query = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({}) RETURNING {}"
        ).format(
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
            placeholders,
            returning_sql,
        )
        return self._run_query(query, values, fetch="one", commit=True)

    def read_one(
        self,
        table: str,
        filters: Mapping[str, Any] | None = None,
        columns: Iterable[str] | None = None,
    ) -> dict[str, Any] | None:
        where_clause, where_values = self._build_where_clause(filters)
        query = sql.SQL("SELECT {} FROM {}{} LIMIT 1").format(
            self._columns_sql(columns),
            sql.Identifier(table),
            where_clause,
        )
        return self._run_query(query, where_values, fetch="one")

    def read_many(
        self,
        table: str,
        filters: Mapping[str, Any] | None = None,
        columns: Iterable[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[str, Any]]:
        where_clause, where_values = self._build_where_clause(filters)
        query = sql.SQL("SELECT {} FROM {}{}").format(
            self._columns_sql(columns),
            sql.Identifier(table),
            where_clause,
        )

        values = list(where_values)
        if limit is not None:
            query += sql.SQL(" LIMIT {}").format(sql.Placeholder())
            values.append(limit)
        if offset is not None:
            query += sql.SQL(" OFFSET {}").format(sql.Placeholder())
            values.append(offset)

        result = self._run_query(query, values, fetch="all")
        return result or []

    def update(
        self,
        table: str,
        data: Mapping[str, Any],
        filters: Mapping[str, Any],
        returning: Iterable[str] | None = None,
    ) -> dict[str, Any] | None:
        if not data:
            raise ValueError("`data` cannot be empty for update operation.")
        if not filters:
            raise ValueError("`filters` cannot be empty for update operation.")

        set_columns = list(data.keys())
        set_values = list(data.values())
        set_clause = sql.SQL(", ").join(
            sql.SQL("{} = {}").format(sql.Identifier(column), sql.Placeholder())
            for column in set_columns
        )

        where_clause, where_values = self._build_where_clause(filters)
        query_params = set_values + where_values
        query = sql.SQL("UPDATE {} SET {}{}").format(
            sql.Identifier(table),
            set_clause,
            where_clause,
        )

        if returning is not None:
            query += sql.SQL(" RETURNING {}").format(self._columns_sql(returning))
            return self._run_query(query, query_params, fetch="one", commit=True)

        self._run_query(query, query_params, commit=True)
        return None

    def delete(
        self,
        table: str,
        filters: Mapping[str, Any],
        returning: Iterable[str] | None = None,
    ) -> dict[str, Any] | None:
        if not filters:
            raise ValueError("`filters` cannot be empty for delete operation.")

        where_clause, where_values = self._build_where_clause(filters)
        query = sql.SQL("DELETE FROM {}{}").format(
            sql.Identifier(table),
            where_clause,
        )

        if returning is not None:
            query += sql.SQL(" RETURNING {}").format(self._columns_sql(returning))
            return self._run_query(query, where_values, fetch="one", commit=True)

        self._run_query(query, where_values, commit=True)
        return None
