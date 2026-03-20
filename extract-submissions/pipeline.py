"""Extract IASO form submissions into a database."""

import re
import unicodedata
from datetime import datetime
from io import BytesIO
from pathlib import Path

import polars as pl
import requests
from openhexa.sdk import IASOConnection, current_run, parameter, pipeline, workspace
from openhexa.toolbox.iaso import IASO
from sqlalchemy import create_engine, text


class LocalRun:
    def log_info(self, message: str):
        print(f"INFO: {message}")

    def log_error(self, message: str):
        print(f"ERROR: {message}")

    def add_database_output(self, table_name: str):
        print(f"Database output: {table_name}")

    def add_file_output(self, file_path: str):
        print(f"File output: {file_path}")


run = current_run or LocalRun()


@pipeline("iaso-extract-submissions")
@parameter("iaso_connection", name="IASO instance", required=True, type=IASOConnection)
@parameter("form_id", name="Form id", type=int, required=True)
@parameter(
    "db_table",
    name="Output table name",
    type=str,
    required=True,
    default="form_submissions",
)
@parameter(
    "output_file",
    name="Output CSV file",
    type=str,
    required=False,
    default="pipelines/extract-submissions/form_submissions.csv",
)
@parameter(
    "force_update",
    name="Force database update",
    type=bool,
    default=False,
    required=False,
)
def iaso_extract_submissions(
    iaso_connection: IASOConnection,
    form_id: int,
    db_table: str,
    output_file: str,
    force_update: bool,
):
    sync_submissions(iaso_connection, form_id, db_table, output_file, force_update)


@iaso_extract_submissions.task
def sync_submissions(
    iaso_connection: IASOConnection,
    form_id: int,
    db_table: str,
    output_file: str,
    force_update: bool,
):
    run.log_info(f"Syncing submissions for form {form_id}")

    if not re.match(r"^[a-z_][a-z0-9_]*$", db_table):
        msg = f"Invalid table name: {db_table}"
        raise ValueError(msg)

    if workspace.database_url is None:
        msg = "Missing database connection"
        run.log_error(msg)
        raise ValueError(msg)

    db_url = workspace.database_url.replace("postgresql://", "postgresql+psycopg://")
    engine = create_engine(db_url)
    iaso = IASO(iaso_connection.url, iaso_connection.username, iaso_connection.password)
    xlsform = get_xlsform(iaso, form_id)
    table_exists = _table_exists(engine, db_table)

    csv_path = Path(workspace.files_path, output_file)

    # When the table doesn't exist yet, do a full fetch
    if not table_exists or force_update:
        run.log_info("Fetching all submissions")
        submissions = _fetch_submissions(iaso, xlsform, form_id)
        _full_replace(submissions, db_table, db_url)
        run.log_info(f"Wrote {len(submissions)} submissions into {db_table}")
        _export_csv(engine, db_table, csv_path)
        run.add_database_output(db_table)
        return

    # Incremental: only fetch what changed since the latest submission
    from_date = _get_latest_update(engine, db_table)
    run.log_info(
        f"Latest submission date in database: "
        f"{from_date.strftime('%Y-%m-%d %H:%M:%S') if from_date else 'N/A'}"
    )

    new = _fetch_submissions(iaso, xlsform, form_id, from_date, is_deleted=False)
    deleted = _fetch_submissions(iaso, xlsform, form_id, from_date, is_deleted=True)

    if new.is_empty() and deleted.is_empty():
        run.log_info("No changes detected. Skipping update.")
        return

    if not new.is_empty():
        _upsert_rows(new, db_table, engine)
        run.log_info(f"Upserted {len(new)} submissions")

    if not deleted.is_empty():
        ids = deleted["id"].to_list()
        _delete_rows(ids, db_table, engine)
        run.log_info(f"Deleted {len(ids)} submissions")

    _export_csv(engine, db_table, csv_path)
    run.add_database_output(db_table)


def get_xlsform(iaso: IASO, form_id: int) -> pl.DataFrame:
    """Fetch the XLSForm definition for a given form."""
    r = iaso.api_client.get(f"api/forms/{form_id}")
    xlsform_url = r.json()["latest_form_version"]["xls_file"]
    with requests.get(xlsform_url, stream=True) as r:
        return pl.read_excel(BytesIO(r.content))


def _fetch_submissions(
    iaso: IASO,
    xlsform: pl.DataFrame,
    form_id: int,
    from_date: datetime | None = None,
    is_deleted: bool = False,
) -> pl.DataFrame:
    """Fetch form submissions from IASO and clean them up."""
    params = {
        "csv": True,
        "form_ids": form_id,
        "modificationDateFrom": from_date.strftime("%Y-%m-%d") if from_date else None,
    }
    if is_deleted:
        params["showDeleted"] = True

    resp = iaso.api_client.get("/api/instances/", params=params)
    df = pl.read_csv(resp.content)
    df = _merge_duplicated_questions(df, xlsform)
    df = _clean_columns(df)
    return df


QUESTION_TYPES = {"calculate", "integer", "text", "decimal"}


def _is_question(type_str: str) -> bool:
    return type_str.startswith("select") or type_str in QUESTION_TYPES


def _remove_suffix(name: str) -> str:
    """Remove the trailing UID suffix from a question name."""
    return "_".join(name.split("_")[:-1])


def _merge_duplicated_questions(submissions: pl.DataFrame, xlsform: pl.DataFrame) -> pl.DataFrame:
    """Merge duplicated questions (same name, different UID suffix) into single columns."""
    questions = [row["name"] for row in xlsform.iter_rows(named=True) if _is_question(row["type"])]
    expected_vars = {_remove_suffix(q) for q in questions} - {""}

    # Map base variable name -> list of actual columns in the submissions
    merge: dict[str, list[str]] = {}
    for column in submissions.columns:
        var = _remove_suffix(column)
        if var in expected_vars:
            merge.setdefault(var, []).append(column)

    # Coalesce duplicate columns into one
    for var, columns in merge.items():
        submissions = submissions.with_columns(pl.coalesce(columns).alias(var))

    columns_to_drop = {col for cols in merge.values() for col in cols}
    submissions = submissions.select(
        [c for c in submissions.columns if c not in columns_to_drop and "note" not in c]
    )
    return submissions


def _slugify(s: str) -> str:
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = re.sub(r"[^a-zA-Z0-9_]+", "_", s.lower())
    return s.strip("_")


def _clean_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize column names and cast standard columns to correct types."""
    df = df.rename(_slugify)

    df = df.with_columns(
        pl.col("date_de_creation").str.to_datetime("%Y-%m-%d %H:%M:%S"),
        pl.col("date_de_modification").str.to_datetime("%Y-%m-%d %H:%M:%S"),
        pl.col("id_du_formulaire").cast(pl.Int64),
        pl.col("latitude").cast(pl.Float64),
        pl.col("altitude").cast(pl.Float64),
        pl.col("longitude").cast(pl.Float64),
        pl.col("precision").cast(pl.Float64),
    )

    df = df.with_columns(
        pl.col("date_de_creation").alias("created_at"),
        pl.col("date_de_modification").alias("updated_at"),
        pl.col("cree_par").alias("created_by"),
        pl.col("id_du_formulaire").alias("id"),
        pl.col("version_du_formulaire").alias("form_version"),
    )
    return df


def _table_exists(engine, table_name: str) -> bool:
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT EXISTS ("
                "  SELECT 1 FROM information_schema.tables"
                "  WHERE table_name = :name"
                ")"
            ),
            {"name": table_name},
        )
        return result.scalar()


def _get_latest_update(engine, table_name: str) -> datetime | None:
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT max(updated_at) FROM {table_name}"))
        return result.scalar()


def _export_csv(engine, table_name: str, csv_path: Path):
    """Read the full table from DB and write it as CSV."""
    with engine.connect() as conn:
        df = pl.read_database(f"SELECT * FROM {table_name}", connection=conn)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(csv_path)
    run.log_info(f"Exported {len(df)} rows to {csv_path}")
    run.add_file_output(csv_path.as_posix())


def _full_replace(df: pl.DataFrame, table_name: str, db_url: str):
    """Drop and recreate the table with the full dataset."""
    df.write_database(
        table_name,
        db_url,
        if_table_exists="replace",
    )


def _upsert_rows(df: pl.DataFrame, table_name: str, engine):
    """Insert new rows or update existing ones based on the id column."""
    if df.is_empty():
        return

    columns = df.columns
    col_list = ", ".join(columns)
    placeholders = ", ".join(f":{c}" for c in columns)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != "id")

    query = text(
        f"INSERT INTO {table_name} ({col_list}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT (id) DO UPDATE SET {updates}"
    )

    with engine.connect() as conn:
        rows = df.to_dicts()
        for row in rows:
            conn.execute(query, row)
        conn.commit()


def _delete_rows(ids: list, table_name: str, engine):
    """Delete rows by ID."""
    if not ids:
        return
    with engine.connect() as conn:
        conn.execute(
            text(f"DELETE FROM {table_name} WHERE id = ANY(:ids)"),
            {"ids": ids},
        )
        conn.commit()


if __name__ == "__main__":
    iaso_extract_submissions()
