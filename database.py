from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Any, Iterable, Mapping

from utils import normalize_text

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "excel_index.db"


def get_connection(db_path: str | Path | None = None) -> sqlite3.Connection:
    database_path = Path(db_path or DEFAULT_DB_PATH)
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    connection.create_function(
        "py_normalize",
        1,
        lambda value: normalize_text("" if value is None else str(value)),
    )
    connection.execute("PRAGMA foreign_keys = ON;")
    connection.execute("PRAGMA journal_mode = WAL;")
    connection.execute("PRAGMA synchronous = NORMAL;")
    connection.execute("PRAGMA temp_store = MEMORY;")
    return connection


def initialize_database(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY,
            file_name TEXT NOT NULL,
            file_path TEXT NOT NULL UNIQUE,
            folder_path TEXT NOT NULL,
            modified_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS cells_index (
            id INTEGER PRIMARY KEY,
            file_id INTEGER NOT NULL,
            sheet_name TEXT NOT NULL,
            row_number INTEGER NOT NULL,
            column_letter TEXT NOT NULL,
            cell_address TEXT NOT NULL,
            cell_value TEXT NOT NULL,
            normalized_value TEXT NOT NULL,
            FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS index_errors (
            id INTEGER PRIMARY KEY,
            file_path TEXT NOT NULL,
            error_message TEXT NOT NULL,
            logged_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_files_file_name ON files(file_name);
        CREATE INDEX IF NOT EXISTS idx_files_folder_path ON files(folder_path);
        CREATE INDEX IF NOT EXISTS idx_cells_normalized_value ON cells_index(normalized_value);
        CREATE INDEX IF NOT EXISTS idx_cells_file_id ON cells_index(file_id);
        CREATE INDEX IF NOT EXISTS idx_errors_logged_at ON index_errors(logged_at DESC);
        """
    )
    connection.commit()


def fetch_file_record(
    connection: sqlite3.Connection, file_path: str
) -> sqlite3.Row | None:
    cursor = connection.execute(
        """
        SELECT id, file_name, file_path, folder_path, modified_at
        FROM files
        WHERE file_path = ?
        """,
        (file_path,),
    )
    return cursor.fetchone()


def upsert_file_record(
    connection: sqlite3.Connection,
    *,
    file_name: str,
    file_path: str,
    folder_path: str,
    modified_at: str,
) -> int:
    existing = fetch_file_record(connection, file_path)
    if existing:
        connection.execute(
            """
            UPDATE files
            SET file_name = ?, folder_path = ?, modified_at = ?
            WHERE id = ?
            """,
            (file_name, folder_path, modified_at, existing["id"]),
        )
        return int(existing["id"])

    cursor = connection.execute(
        """
        INSERT INTO files (file_name, file_path, folder_path, modified_at)
        VALUES (?, ?, ?, ?)
        """,
        (file_name, file_path, folder_path, modified_at),
    )
    return int(cursor.lastrowid)


def delete_cells_for_file(connection: sqlite3.Connection, file_id: int) -> None:
    connection.execute("DELETE FROM cells_index WHERE file_id = ?", (file_id,))


def insert_cells_batch(
    connection: sqlite3.Connection, rows: Iterable[tuple[Any, ...]]
) -> None:
    connection.executemany(
        """
        INSERT INTO cells_index (
            file_id,
            sheet_name,
            row_number,
            column_letter,
            cell_address,
            cell_value,
            normalized_value
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def delete_file_record(connection: sqlite3.Connection, file_id: int) -> None:
    connection.execute("DELETE FROM files WHERE id = ?", (file_id,))


def list_indexed_files(
    connection: sqlite3.Connection, root_prefix: str | None = None
) -> list[sqlite3.Row]:
    if root_prefix:
        cursor = connection.execute(
            """
            SELECT id, file_path
            FROM files
            WHERE file_path LIKE ?
            ORDER BY file_path
            """,
            (f"{root_prefix}%",),
        )
    else:
        cursor = connection.execute(
            """
            SELECT id, file_path
            FROM files
            ORDER BY file_path
            """
        )
    return cursor.fetchall()


def clear_error_logs(connection: sqlite3.Connection) -> None:
    connection.execute("DELETE FROM index_errors")
    connection.commit()


def log_index_error(
    connection: sqlite3.Connection, *, file_path: str, error_message: str, logged_at: str
) -> None:
    connection.execute(
        """
        INSERT INTO index_errors (file_path, error_message, logged_at)
        VALUES (?, ?, ?)
        """,
        (file_path, error_message, logged_at),
    )
    connection.commit()


def get_error_logs(
    connection: sqlite3.Connection, limit: int = 200
) -> list[dict[str, Any]]:
    cursor = connection.execute(
        """
        SELECT logged_at, file_path, error_message
        FROM index_errors
        ORDER BY logged_at DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    )
    return [dict(row) for row in cursor.fetchall()]


def get_database_stats(connection: sqlite3.Connection) -> dict[str, int]:
    file_count = connection.execute("SELECT COUNT(*) FROM files").fetchone()[0]
    cell_count = connection.execute("SELECT COUNT(*) FROM cells_index").fetchone()[0]
    error_count = connection.execute("SELECT COUNT(*) FROM index_errors").fetchone()[0]
    return {
        "indexed_files": int(file_count),
        "indexed_cells": int(cell_count),
        "error_logs": int(error_count),
    }


def get_distinct_folders(
    connection: sqlite3.Connection, limit: int = 500
) -> list[str]:
    cursor = connection.execute(
        """
        SELECT DISTINCT folder_path
        FROM files
        ORDER BY folder_path
        LIMIT ?
        """,
        (limit,),
    )
    return [str(row[0]) for row in cursor.fetchall()]
