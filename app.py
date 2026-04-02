from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import streamlit as st

from database import (
    DEFAULT_DB_PATH,
    get_connection,
    get_database_stats,
    get_distinct_folders,
    get_error_logs,
    initialize_database,
)
from indexer import ExcelIndexer
from search import search_by_content, search_by_file_name, search_combined
from utils import export_rows_to_excel, open_path_in_finder, reveal_file_in_finder

APP_TITLE = "Локальний пошук по Excel-архіву"
DEFAULT_ROOT = Path.home() / "Downloads" / "МСЕК(НАБРАНЕ)"
SEARCH_MODES = {
    "За назвою файлу": "file_name",
    "За вмістом": "content",
    "Комбінований": "combined",
}


def configure_page() -> None:
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon="📚",
        layout="wide",
    )
    st.markdown(
        """
        <style>
            :root {
                --bg: #f6f2e9;
                --panel: #fffdfa;
                --ink: #1f2933;
                --muted: #5b6875;
                --accent: #0f766e;
                --accent-strong: #0b525b;
                --accent-deep: #08363c;
                --line: rgba(31, 41, 51, 0.14);
                --accent-soft: rgba(15, 118, 110, 0.10);
                --warn: #9a3412;
            }
            .stApp {
                background:
                    radial-gradient(circle at top right, rgba(15, 118, 110, 0.10), transparent 30%),
                    linear-gradient(180deg, #fbf8f2 0%, var(--bg) 100%);
                color: var(--ink);
                font-family: "Avenir Next", "SF Pro Display", "Helvetica Neue", sans-serif;
            }
            .app-shell {
                background: var(--panel);
                border: 1px solid rgba(31, 41, 51, 0.08);
                border-radius: 18px;
                padding: 1.25rem 1.25rem 0.75rem 1.25rem;
                box-shadow: 0 20px 60px rgba(31, 41, 51, 0.06);
                margin-bottom: 1rem;
            }
            .app-caption {
                color: var(--muted);
                margin-top: -0.4rem;
                margin-bottom: 0.4rem;
            }
            .section-title {
                font-size: 1.05rem;
                font-weight: 700;
                margin-bottom: 0.5rem;
            }
            .small-note {
                color: var(--muted);
                font-size: 0.92rem;
            }
            .stTextInput label,
            .stSelectbox label,
            .stRadio label,
            .stDownloadButton label {
                color: var(--ink) !important;
                font-weight: 700 !important;
            }
            .stTextInput input,
            .stSelectbox [data-baseweb="select"] > div,
            .stSelectbox [data-baseweb="select"] input {
                background: #ffffff !important;
                color: var(--ink) !important;
                border: 1px solid var(--line) !important;
            }
            .stTextInput input::placeholder,
            .stSelectbox input::placeholder {
                color: #6c7a86 !important;
                opacity: 1 !important;
            }
            .stTextInput input:focus,
            .stSelectbox [data-baseweb="select"] > div:focus-within {
                border-color: var(--accent) !important;
                box-shadow: 0 0 0 0.2rem rgba(15, 118, 110, 0.16) !important;
            }
            div[role="radiogroup"] {
                gap: 0.6rem;
            }
            div[role="radiogroup"] > label {
                background: #ffffff;
                border: 1px solid var(--line);
                border-radius: 999px;
                padding: 0.35rem 0.9rem;
            }
            div[role="radiogroup"] > label div {
                color: var(--ink) !important;
                font-weight: 700 !important;
            }
            div[role="radiogroup"] > label:has(input:checked) {
                background: var(--accent-soft);
                border-color: rgba(15, 118, 110, 0.38);
                box-shadow: inset 0 0 0 1px rgba(15, 118, 110, 0.18);
            }
            .stButton > button,
            .stDownloadButton > button,
            .stFormSubmitButton > button {
                min-height: 2.85rem;
                border-radius: 12px;
                border: 1px solid rgba(8, 54, 60, 0.22) !important;
                background: linear-gradient(180deg, var(--accent-strong) 0%, var(--accent-deep) 100%) !important;
                color: #ffffff !important;
                font-weight: 800 !important;
                letter-spacing: 0.01em;
                box-shadow: 0 10px 24px rgba(8, 54, 60, 0.18);
                transition: transform 120ms ease, box-shadow 120ms ease, background 120ms ease;
            }
            .stButton > button *,
            .stDownloadButton > button *,
            .stFormSubmitButton > button * {
                color: #ffffff !important;
                fill: #ffffff !important;
            }
            .stButton > button:hover,
            .stDownloadButton > button:hover,
            .stFormSubmitButton > button:hover {
                background: linear-gradient(180deg, #0e6168 0%, #062c31 100%) !important;
                border-color: rgba(6, 44, 49, 0.44) !important;
                box-shadow: 0 14px 30px rgba(8, 54, 60, 0.22);
                transform: translateY(-1px);
            }
            .stButton > button:focus-visible,
            .stDownloadButton > button:focus-visible,
            .stFormSubmitButton > button:focus-visible {
                outline: none !important;
                box-shadow:
                    0 0 0 0.24rem rgba(15, 118, 110, 0.22),
                    0 12px 28px rgba(8, 54, 60, 0.18) !important;
            }
            .stButton > button:disabled,
            .stDownloadButton > button:disabled,
            .stFormSubmitButton > button:disabled {
                background: #d4dde3 !important;
                color: #52606d !important;
                border-color: rgba(82, 96, 109, 0.18) !important;
                box-shadow: none !important;
                transform: none !important;
                opacity: 1 !important;
            }
            .stButton > button:disabled *,
            .stDownloadButton > button:disabled *,
            .stFormSubmitButton > button:disabled * {
                color: #52606d !important;
                fill: #52606d !important;
            }
            .stDataFrame, .stTable {
                border: 1px solid rgba(31, 41, 51, 0.08);
                border-radius: 14px;
                overflow: hidden;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def get_db_stats() -> dict[str, int]:
    connection = get_connection(DEFAULT_DB_PATH)
    initialize_database(connection)
    stats = get_database_stats(connection)
    connection.close()
    return stats


def get_logs(limit: int = 200) -> list[dict[str, Any]]:
    connection = get_connection(DEFAULT_DB_PATH)
    initialize_database(connection)
    logs = get_error_logs(connection, limit=limit)
    connection.close()
    return logs


def build_search_results(
    mode_key: str,
    query: str,
    folder_filter: str,
    file_name_filter: str,
    limit: int,
    offset: int,
) -> tuple[list[dict[str, Any]], int]:
    if mode_key == "file_name":
        return search_by_file_name(
            query,
            db_path=DEFAULT_DB_PATH,
            folder_filter=folder_filter,
            limit=limit,
            offset=offset,
        )
    if mode_key == "content":
        return search_by_content(
            query,
            db_path=DEFAULT_DB_PATH,
            folder_filter=folder_filter,
            file_name_filter=file_name_filter,
            limit=limit,
            offset=offset,
        )
    return search_combined(
        query,
        db_path=DEFAULT_DB_PATH,
        folder_filter=folder_filter,
        file_name_filter=file_name_filter,
        limit=limit,
        offset=offset,
    )


def run_indexing(root_folder: str, *, cleanup_deleted: bool) -> None:
    progress = st.progress(0.0)
    status = st.empty()
    counters = st.empty()

    indexer = ExcelIndexer(DEFAULT_DB_PATH)

    def update_progress(current: int, total: int, message: str) -> None:
        ratio = 1.0 if total == 0 else current / total
        progress.progress(min(max(ratio, 0.0), 1.0))
        status.info(message)
        last_stats = st.session_state.get("last_index_stats", {})
        counters.caption(
            "Знайдено файлів: {files_found} | Проіндексовано: {files_indexed} | "
            "Пропущено: {files_skipped} | Додано комірок: {cells_added} | Помилок: {errors}".format(
                files_found=last_stats.get("files_found", 0),
                files_indexed=last_stats.get("files_indexed", 0),
                files_skipped=last_stats.get("files_skipped", 0),
                cells_added=last_stats.get("cells_added", 0),
                errors=last_stats.get("errors", 0),
            )
        )

    try:
        stats = indexer.index_folder(
            root_folder,
            cleanup_deleted=cleanup_deleted,
            progress_callback=update_progress,
        )
        st.session_state["last_index_stats"] = stats.to_dict()
        progress.progress(1.0)
        status.success("Індексацію завершено")
    except Exception as exc:  # noqa: BLE001
        status.error(str(exc))
    finally:
        db_stats = get_db_stats()
        st.session_state["db_stats"] = db_stats
        st.session_state["error_logs"] = get_logs()


def render_metrics() -> None:
    last_run = st.session_state.get(
        "last_index_stats",
        {
            "files_found": 0,
            "files_indexed": 0,
            "files_skipped": 0,
            "files_deleted": 0,
            "cells_added": 0,
            "errors": 0,
        },
    )
    db_stats = st.session_state.get("db_stats", get_db_stats())

    metric_columns = st.columns(6)
    metric_columns[0].metric("Знайдено файлів", last_run["files_found"])
    metric_columns[1].metric("Проіндексовано", last_run["files_indexed"])
    metric_columns[2].metric("Пропущено", last_run["files_skipped"])
    metric_columns[3].metric("Видалено з БД", last_run["files_deleted"])
    metric_columns[4].metric("Нових комірок", last_run["cells_added"])
    metric_columns[5].metric("Помилок", last_run["errors"])

    st.caption(
        "У базі зараз: {files} файлів, {cells} комірок, {errors} записів у журналі помилок".format(
            files=db_stats["indexed_files"],
            cells=db_stats["indexed_cells"],
            errors=db_stats["error_logs"],
        )
    )


def render_index_controls() -> None:
    st.markdown('<div class="app-shell">', unsafe_allow_html=True)
    st.markdown(f"## {APP_TITLE}")
    st.markdown(
        '<p class="app-caption">Локальний індекс `.xlsx` і `.xlsm` без сервера та без хмари.</p>',
        unsafe_allow_html=True,
    )

    default_root = str(DEFAULT_ROOT) if DEFAULT_ROOT.exists() else str(Path.home() / "Downloads")
    root_folder = st.text_input(
        "Коренева папка з Excel-файлами",
        value=st.session_state.get("root_folder", default_root),
        help="Вкажіть шлях до папки. Приклад: ~/Downloads/МСЕК(НАБРАНЕ)",
    )
    st.session_state["root_folder"] = root_folder

    action_columns = st.columns([1, 1, 1, 2])
    if action_columns[0].button("Індексувати", use_container_width=True):
        run_indexing(root_folder, cleanup_deleted=False)

    if action_columns[1].button("Оновити індекс", use_container_width=True):
        run_indexing(root_folder, cleanup_deleted=True)

    if action_columns[2].button("Очистити застарілі записи", use_container_width=True):
        try:
            deleted = ExcelIndexer(DEFAULT_DB_PATH).cleanup_deleted_records(root_folder)
            st.session_state["last_index_stats"] = {
                "files_found": 0,
                "files_indexed": 0,
                "files_skipped": 0,
                "files_deleted": deleted,
                "cells_added": 0,
                "errors": 0,
            }
            st.session_state["db_stats"] = get_db_stats()
            st.success(f"Прибрано записів про видалені файли: {deleted}")
        except Exception as exc:  # noqa: BLE001
            st.error(str(exc))

    action_columns[3].markdown(
        f'<p class="small-note">SQLite база: <code>{DEFAULT_DB_PATH}</code></p>',
        unsafe_allow_html=True,
    )

    render_metrics()
    st.markdown("</div>", unsafe_allow_html=True)


def render_search_section() -> None:
    st.markdown('<div class="app-shell">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Пошук</div>', unsafe_allow_html=True)

    mode_label = st.radio(
        "Режим пошуку",
        options=list(SEARCH_MODES.keys()),
        horizontal=True,
    )
    mode_key = SEARCH_MODES[mode_label]

    with st.form("search_form", clear_on_submit=False):
        query = st.text_input("Пошуковий запит")
        filter_columns = st.columns(3)
        folder_filter = filter_columns[0].text_input("Фільтр по папці")
        file_name_filter = filter_columns[1].text_input("Фільтр по імені файлу")
        page_size = int(
            filter_columns[2].selectbox("Рядків на сторінці", options=[25, 50, 100, 250], index=1)
        )
        submitted = st.form_submit_button("Знайти", use_container_width=True)

    if submitted:
        st.session_state["search_params"] = {
            "mode_key": mode_key,
            "query": query,
            "folder_filter": folder_filter,
            "file_name_filter": file_name_filter,
            "page_size": page_size,
            "page": 1,
        }

    params = st.session_state.get("search_params")
    if not params:
        st.info("Введіть запит і запустіть пошук.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    if not params["query"].strip():
        st.warning("Пошуковий запит порожній.")
        st.markdown("</div>", unsafe_allow_html=True)
        return

    page_size = int(params["page_size"])
    page = int(params.get("page", 1))
    offset = (page - 1) * page_size

    rows, total = build_search_results(
        params["mode_key"],
        params["query"],
        params["folder_filter"],
        params["file_name_filter"],
        page_size,
        offset,
    )

    total_pages = max((total - 1) // page_size + 1, 1)
    nav_columns = st.columns([1, 1, 2, 2])
    if nav_columns[0].button("← Назад", disabled=page <= 1, use_container_width=True):
        st.session_state["search_params"]["page"] = page - 1
        st.rerun()
    if nav_columns[1].button("Вперед →", disabled=page >= total_pages, use_container_width=True):
        st.session_state["search_params"]["page"] = page + 1
        st.rerun()

    nav_columns[2].caption(f"Знайдено рядків: {total}")
    nav_columns[3].caption(f"Сторінка {page} з {total_pages}")

    if rows:
        st.dataframe(rows, use_container_width=True, hide_index=True)
        export_name = (
            f"search_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        )
        st.download_button(
            "Експортувати результати в Excel",
            data=export_rows_to_excel(rows),
            file_name=export_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
        render_result_actions(rows)
    else:
        st.info("Збігів не знайдено.")

    st.markdown("</div>", unsafe_allow_html=True)


def render_result_actions(rows: list[dict[str, Any]]) -> None:
    options = {
        f"{index + 1}. {row.get('Назва файлу', row.get('Повний шлях', ''))}": row
        for index, row in enumerate(rows)
    }
    selected_label = st.selectbox("Оберіть рядок для дій", options=list(options.keys()))
    selected_row = options[selected_label]
    full_path = selected_row.get("Повний шлях", "")

    if not full_path:
        return

    action_columns = st.columns(2)
    if action_columns[0].button("Відкрити файл", use_container_width=True):
        open_path_in_finder(full_path)
    if action_columns[1].button("Показати у Finder", use_container_width=True):
        reveal_file_in_finder(full_path)


def render_error_logs() -> None:
    st.markdown('<div class="app-shell">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Журнал помилок індексації</div>', unsafe_allow_html=True)

    logs = st.session_state.get("error_logs", get_logs())
    if not logs:
        st.success("У останньому запуску індексації помилок не було.")
    else:
        st.dataframe(logs, use_container_width=True, hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)


def render_sidebar() -> None:
    with st.sidebar:
        st.markdown("### Швидкі підказки")
        st.write("1. Вкажіть кореневу папку з `.xlsx` і `.xlsm`.")
        st.write("2. Натисніть `Індексувати` для першого запуску.")
        st.write("3. Використовуйте `Оновити індекс`, якщо частина файлів змінилась або була видалена.")

        connection = get_connection(DEFAULT_DB_PATH)
        initialize_database(connection)
        folders = get_distinct_folders(connection, limit=20)
        connection.close()

        if folders:
            st.markdown("### Папки, які вже є в індексі")
            for folder in folders:
                st.code(folder, language=None)


def main() -> None:
    configure_page()
    st.session_state.setdefault("db_stats", get_db_stats())
    st.session_state.setdefault("error_logs", get_logs())

    render_sidebar()
    render_index_controls()
    render_search_section()
    render_error_logs()


if __name__ == "__main__":
    main()
