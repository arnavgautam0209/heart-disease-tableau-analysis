"""
Tableau Hyper Extract Generator
=================================
Generates Tableau .hyper extract files from the SQLite database.
Hyper is Tableau's native high-performance data engine format.

If the 'tableauhyperapi' package is available, produces .hyper files.
Otherwise, falls back to optimized CSV + TDE-compatible output.

Usage:
  python3 generate_extract.py                  # Generate all extracts
  python3 generate_extract.py --dataset heart_disease_tableau
  python3 generate_extract.py --format hyper   # Force Hyper format
  python3 generate_extract.py --format csv     # Force CSV format
"""

import argparse
import csv
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "heart_disease.db")
EXTRACT_DIR = os.path.join(BASE_DIR, "tableau_extracts")

os.makedirs(EXTRACT_DIR, exist_ok=True)

# Check if Tableau Hyper API is available
try:
    from tableauhyperapi import (
        HyperProcess, Telemetry, Connection, CreateMode,
        TableDefinition, TableName, SqlType, Inserter, NOT_NULLABLE, NULLABLE
    )
    HYPER_AVAILABLE = True
except ImportError:
    HYPER_AVAILABLE = False

# ---------------------------------------------------------------------------
# Dataset definitions (mirrors tableau_connector.py)
# ---------------------------------------------------------------------------

DATASETS = {
    "heart_disease_tableau": {
        "name": "Heart Disease — Full Dataset",
        "sql": "SELECT * FROM heart_disease_tableau",
    },
    "prevalence_by_age_sex": {
        "name": "Prevalence by Age & Sex",
        "sql": "SELECT * FROM vw_prevalence_by_age_sex",
    },
    "bmi_distribution": {
        "name": "BMI Distribution",
        "sql": "SELECT * FROM vw_bmi_distribution",
    },
    "risk_factor_summary": {
        "name": "Risk Factor Summary",
        "sql": "SELECT * FROM vw_risk_factor_summary",
    },
    "gen_health_vs_hd": {
        "name": "General Health vs Heart Disease",
        "sql": "SELECT * FROM vw_gen_health_vs_hd",
    },
    "risk_tier_summary": {
        "name": "Risk Tier Summary",
        "sql": "SELECT * FROM vw_risk_tier_summary",
    },
    "comorbidity_impact": {
        "name": "Comorbidity Impact",
        "sql": "SELECT * FROM vw_comorbidity_impact",
    },
}

# ---------------------------------------------------------------------------
# SQLite type → Hyper SqlType mapping
# ---------------------------------------------------------------------------

def infer_column_types(conn, sql):
    """Sample data to infer accurate column types."""
    cur = conn.execute(f"{sql} LIMIT 100")
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    col_types = {}
    for i, col in enumerate(columns):
        values = [row[i] for row in rows if row[i] is not None]
        if not values:
            col_types[col] = "text"
        elif all(isinstance(v, int) for v in values):
            col_types[col] = "integer"
        elif all(isinstance(v, (int, float)) for v in values):
            col_types[col] = "double"
        else:
            col_types[col] = "text"

    return columns, col_types


# ---------------------------------------------------------------------------
# Hyper extract generation
# ---------------------------------------------------------------------------

def generate_hyper_extract(dataset_key, ds_info):
    """Generate a .hyper extract file using the Tableau Hyper API."""
    hyper_path = os.path.join(EXTRACT_DIR, f"{dataset_key}.hyper")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    columns, col_types = infer_column_types(conn, ds_info["sql"])

    # Build Hyper table definition
    hyper_columns = []
    for col in columns:
        ctype = col_types[col]
        if ctype == "integer":
            sql_type = SqlType.big_int()
        elif ctype == "double":
            sql_type = SqlType.double()
        else:
            sql_type = SqlType.text()
        hyper_columns.append(TableDefinition.Column(col, sql_type, NULLABLE))

    table_def = TableDefinition(
        table_name=TableName("Extract", dataset_key),
        columns=hyper_columns,
    )

    # Fetch data from SQLite
    rows = conn.execute(ds_info["sql"]).fetchall()
    conn.close()

    # Write to Hyper file
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(
            endpoint=hyper.endpoint,
            database=hyper_path,
            create_mode=CreateMode.CREATE_AND_REPLACE,
        ) as connection:
            connection.catalog.create_schema("Extract")
            connection.catalog.create_table(table_def)

            with Inserter(connection, table_def) as inserter:
                for row in rows:
                    inserter.add_row([row[col] for col in columns])
                inserter.execute()

    return hyper_path, len(rows)


# ---------------------------------------------------------------------------
# CSV extract generation (fallback / universal)
# ---------------------------------------------------------------------------

def generate_csv_extract(dataset_key, ds_info):
    """Generate an optimized CSV extract file for Tableau."""
    csv_path = os.path.join(EXTRACT_DIR, f"{dataset_key}.csv")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(ds_info["sql"])
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    conn.close()

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    return csv_path, len(rows)


# ---------------------------------------------------------------------------
# Metadata / manifest generation (for Tableau data source config)
# ---------------------------------------------------------------------------

def generate_manifest(generated_files):
    """Create a manifest JSON for tracking extract versions."""
    manifest = {
        "generator": "Heart Disease — Tableau Extract Generator",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database_source": DB_PATH,
        "extracts": [],
    }

    for key, info in generated_files.items():
        manifest["extracts"].append({
            "dataset": key,
            "name": DATASETS[key]["name"],
            "format": info["format"],
            "file": info["file"],
            "row_count": info["rows"],
            "file_size_kb": round(os.path.getsize(info["file"]) / 1024, 1),
        })

    manifest_path = os.path.join(EXTRACT_DIR, "extract_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    return manifest_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate Tableau extract files")
    parser.add_argument(
        "--dataset", choices=list(DATASETS.keys()),
        help="Generate extract for a specific dataset (default: all)",
    )
    parser.add_argument(
        "--format", choices=["hyper", "csv", "auto"], default="auto",
        help="Extract format: hyper, csv, or auto (default: auto)",
    )
    args = parser.parse_args()

    # Verify database
    if not os.path.exists(DB_PATH):
        print(f"[!] Database not found at {DB_PATH}")
        print("    Run 'python3 db_setup.py' first.")
        sys.exit(1)

    # Determine format
    if args.format == "auto":
        use_hyper = HYPER_AVAILABLE
    elif args.format == "hyper":
        if not HYPER_AVAILABLE:
            print("[!] tableauhyperapi not installed.")
            print("    Install with: pip install tableauhyperapi")
            print("    Falling back to CSV format.\n")
            use_hyper = False
        else:
            use_hyper = True
    else:
        use_hyper = False

    # Select datasets
    if args.dataset:
        datasets = {args.dataset: DATASETS[args.dataset]}
    else:
        datasets = DATASETS

    fmt_label = "Hyper" if use_hyper else "CSV"
    print("=" * 60)
    print("  Tableau Extract Generator")
    print("=" * 60)
    print(f"\n  Format:  {fmt_label}")
    print(f"  Output:  {EXTRACT_DIR}/")
    print(f"  Datasets: {len(datasets)}\n")

    generated = {}
    for key, ds in datasets.items():
        try:
            if use_hyper:
                path, rows = generate_hyper_extract(key, ds)
                ext = "hyper"
            else:
                path, rows = generate_csv_extract(key, ds)
                ext = "csv"

            size_kb = round(os.path.getsize(path) / 1024, 1)
            generated[key] = {"format": ext, "file": path, "rows": rows}
            print(f"  ✅  {key:30s}  {rows:>6,} rows  ({size_kb} KB)")
        except Exception as e:
            print(f"  ❌  {key:30s}  ERROR: {e}")

    # Generate manifest
    manifest_path = generate_manifest(generated)
    print(f"\n  📋  Manifest: {manifest_path}")

    print(f"""
{"=" * 60}
  ✅  {len(generated)} extracts generated successfully!

  To use in Tableau:
    1. Open Tableau Desktop or Tableau Public
    2. Connect → {"More... → Hyper file" if use_hyper else "Text File (CSV)"}
    3. Navigate to: {EXTRACT_DIR}/
    4. Select the extract file(s)
{"=" * 60}
""")


if __name__ == "__main__":
    main()
