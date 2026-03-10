"""Unified database layer (psycopg2)."""

import psycopg2
from contextlib import contextmanager
from src.credentials import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD


def connect_db():
    """Creates and returns a psycopg2 connection.

    Returns:
        A psycopg2 connection object.
    """
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


@contextmanager
def get_cursor(commit=True):
    """Context manager that provides a database cursor.

    Automatically commits (or rolls back on error) and closes the connection.

    Args:
        commit: Whether to commit the transaction on success.

    Yields:
        A psycopg2 cursor.
    """
    conn = connect_db()
    cur = conn.cursor()
    try:
        yield cur
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def execute_sql(sql, params=None, message=""):
    """Executes a single SQL statement (DDL/DML).

    Args:
        sql: SQL string to execute.
        params: Optional parameters for parameterized queries.
        message: Optional message to print after execution.
    """
    with get_cursor() as cur:
        cur.execute(sql, params)
    if message:
        print(message)


def is_table_empty(table_name):
    """Returns True if the given table has no rows.

    Args:
        table_name: Name of the database table.

    Returns:
        True if the table is empty, False otherwise.
    """
    with get_cursor(commit=False) as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        return cur.fetchone()[0] == 0


def clear_table(table_name):
    """Deletes all rows and resets the SERIAL counter.

    Args:
        table_name: Name of the database table.
    """
    with get_cursor() as cur:
        cur.execute(f"DELETE FROM {table_name};")
        cur.execute(f"SELECT pg_get_serial_sequence('{table_name}', 'id');")
        seq = cur.fetchone()[0]
        if seq:
            cur.execute(f"ALTER SEQUENCE {seq} RESTART WITH 1;")
    print(f"  {table_name} cleared")


def table_row_count(table_name):
    """Returns the number of rows in the given table.

    Args:
        table_name: Name of the database table.

    Returns:
        Row count as an integer.
    """
    with get_cursor(commit=False) as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        return cur.fetchone()[0]
