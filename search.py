from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Any

from database import get_connection, initialize_database
from utils import normalize_text


def _count_query(
    connection: sqlite3.Connection, sql: str, params: tuple[Any, ...]
) -> int:
    return int(connection.execute(sql, params).fetchone()[0])


def _rows_to_dicts(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    return [dict(row) for row in cursor.fetchall()]


def search_by_file_name(
    query: str,
    *,
    db_path: str | Path | None = None,
    folder_filter: str = "",
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    if not query.strip():
        return [], 0

    normalized_query = normalize_text(query)
    connection = get_connection(db_path)
    initialize_database(connection)

    conditions = ["py_normalize(file_name) LIKE ?"]
    params: list[Any] = [f"%{normalized_query}%"]

    if folder_filter.strip():
        conditions.append("py_normalize(folder_path) LIKE ?")
        params.append(f"%{normalize_text(folder_filter)}%")

    where_clause = " AND ".join(conditions)

    total = _count_query(
        connection,
        f"SELECT COUNT(*) FROM files WHERE {where_clause}",
        tuple(params),
    )

    cursor = connection.execute(
        f"""
        SELECT
            file_name AS "Назва файлу",
            folder_path AS "Папка",
            file_path AS "Повний шлях",
            modified_at AS "Дата зміни"
        FROM files
        WHERE {where_clause}
        ORDER BY file_name COLLATE NOCASE, folder_path COLLATE NOCASE
        LIMIT ? OFFSET ?
        """,
        (*params, limit, offset),
    )
    rows = _rows_to_dicts(cursor)
    connection.close()
    return rows, total


def search_by_content(
    query: str,
    *,
    db_path: str | Path | None = None,
    folder_filter: str = "",
    file_name_filter: str = "",
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    if not query.strip():
        return [], 0

    normalized_query = normalize_text(query)
    connection = get_connection(db_path)
    initialize_database(connection)

    conditions = ["c.normalized_value LIKE ?"]
    params: list[Any] = [f"%{normalized_query}%"]

    if folder_filter.strip():
        conditions.append("py_normalize(f.folder_path) LIKE ?")
        params.append(f"%{normalize_text(folder_filter)}%")

    if file_name_filter.strip():
        conditions.append("py_normalize(f.file_name) LIKE ?")
        params.append(f"%{normalize_text(file_name_filter)}%")

    where_clause = " AND ".join(conditions)

    total = _count_query(
        connection,
        f"""
        SELECT COUNT(*)
        FROM cells_index c
        INNER JOIN files f ON f.id = c.file_id
        WHERE {where_clause}
        """,
        tuple(params),
    )

    cursor = connection.execute(
        f"""
        SELECT
            c.cell_value AS "Збіг",
            f.file_name AS "Назва файлу",
            f.folder_path AS "Папка",
            c.sheet_name AS "Аркуш",
            c.row_number AS "Рядок",
            c.column_letter AS "Колонка",
            c.cell_address AS "Комірка",
            c.cell_value AS "Значення комірки",
            f.file_path AS "Повний шлях"
        FROM cells_index c
        INNER JOIN files f ON f.id = c.file_id
        WHERE {where_clause}
        ORDER BY f.file_name COLLATE NOCASE, c.sheet_name, c.row_number, c.column_letter
        LIMIT ? OFFSET ?
        """,
        (*params, limit, offset),
    )
    rows = _rows_to_dicts(cursor)
    connection.close()
    return rows, total


def search_combined(
    query: str,
    *,
    db_path: str | Path | None = None,
    folder_filter: str = "",
    file_name_filter: str = "",
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    if not query.strip():
        return [], 0

    normalized_file_query = normalize_text(query)
    normalized_query = normalize_text(query)
    params: list[Any] = [f"%{normalized_file_query}%", f"%{normalized_query}%"]
    folder_params: list[Any] = []
    file_params: list[Any] = []

    folder_condition = ""
    file_name_condition = ""

    if folder_filter.strip():
        folder_condition = " AND py_normalize(folder_path) LIKE ?"
        folder_params.append(f"%{normalize_text(folder_filter)}%")

    if file_name_filter.strip():
        file_name_condition = " AND py_normalize(file_name) LIKE ?"
        file_params.append(f"%{normalize_text(file_name_filter)}%")

    content_folder_condition = folder_condition.replace("folder_path", "f.folder_path")
    content_file_name_condition = file_name_condition.replace("file_name", "f.file_name")

    connection = get_connection(db_path)
    initialize_database(connection)

    base_sql = f"""
        SELECT
            'Назва файлу' AS "Тип збігу",
            file_name AS "Назва файлу",
            folder_path AS "Папка",
            file_path AS "Повний шлях",
            modified_at AS "Дата зміни",
            '' AS "Аркуш",
            '' AS "Комірка",
            '' AS "Значення"
        FROM files
        WHERE py_normalize(file_name) LIKE ?{folder_condition}{file_name_condition}

        UNION ALL

        SELECT
            'Вміст' AS "Тип збігу",
            f.file_name AS "Назва файлу",
            f.folder_path AS "Папка",
            f.file_path AS "Повний шлях",
            f.modified_at AS "Дата зміни",
            c.sheet_name AS "Аркуш",
            c.cell_address AS "Комірка",
            c.cell_value AS "Значення"
        FROM cells_index c
        INNER JOIN files f ON f.id = c.file_id
        WHERE c.normalized_value LIKE ?{content_folder_condition}{content_file_name_condition}
    """

    full_params = tuple(params[:1] + folder_params + file_params + params[1:] + folder_params + file_params)
    total = _count_query(
        connection,
        f"SELECT COUNT(*) FROM ({base_sql}) AS combined_results",
        full_params,
    )

    cursor = connection.execute(
        f"""
        SELECT *
        FROM ({base_sql}) AS combined_results
        ORDER BY "Назва файлу" COLLATE NOCASE, "Тип збігу", "Аркуш", "Комірка"
        LIMIT ? OFFSET ?
        """,
        (*full_params, limit, offset),
    )
    rows = _rows_to_dicts(cursor)
    connection.close()
    return rows, total
