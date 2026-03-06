"""
Tableau ↔ Database Connection Server
======================================
A lightweight Flask-based Web Data Connector (WDC) & REST API that provides:

  1. WDC HTML page — Tableau's Web Data Connector interface for live/interactive use
  2. REST API endpoints — JSON data feeds that Tableau (or any BI tool) can query
  3. Health / metadata endpoints — for monitoring and connection verification
  4. CORS-enabled — safe for Tableau Desktop, Server, and Cloud access

Usage:
  python3 tableau_connector.py                     # Start on default port 8765
  python3 tableau_connector.py --port 9000         # Custom port
  python3 tableau_connector.py --host 0.0.0.0      # Bind to all interfaces

Tableau Connection:
  1. In Tableau → Connect → Web Data Connector
  2. Enter URL: http://localhost:8765/wdc
  3. Select dataset(s) and click "Get Data"
"""

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from functools import wraps

# ---------------------------------------------------------------------------
# Minimal HTTP server (stdlib only — no external dependencies required)
# ---------------------------------------------------------------------------
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "heart_disease.db")

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db():
    """Thread-safe read-only connection."""
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON;")
    return conn


def query_to_dicts(sql, params=()):
    """Execute SQL and return list of dicts."""
    conn = get_db()
    try:
        cur = conn.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def get_table_schema(table_name):
    """Return column info for a table/view."""
    conn = get_db()
    try:
        cur = conn.execute(f"PRAGMA table_info([{table_name}]);")
        columns = []
        for row in cur.fetchall():
            col_name = row[1]
            col_type = row[2].upper() if row[2] else "TEXT"
            # Map SQLite types → Tableau WDC types
            if "INT" in col_type:
                wdc_type = "int"
            elif "REAL" in col_type or "FLOAT" in col_type or "DOUBLE" in col_type:
                wdc_type = "float"
            elif "BOOL" in col_type:
                wdc_type = "bool"
            else:
                wdc_type = "string"
            columns.append({
                "id": col_name,
                "alias": col_name.replace("_", " ").title(),
                "dataType": wdc_type,
            })
        return columns
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Available datasets (tables + views exposed to Tableau)
# ---------------------------------------------------------------------------

DATASETS = {
    "heart_disease_tableau": {
        "name": "Heart Disease — Full Dataset",
        "description": "Denormalized fact table with all dimensions, derived columns, and risk scores",
        "sql": "SELECT * FROM heart_disease_tableau",
    },
    "prevalence_by_age_sex": {
        "name": "Prevalence by Age & Sex",
        "description": "Heart disease rate aggregated by age category and sex",
        "sql": "SELECT * FROM vw_prevalence_by_age_sex",
    },
    "bmi_distribution": {
        "name": "BMI Distribution",
        "description": "BMI statistics grouped by heart disease status and BMI category",
        "sql": "SELECT * FROM vw_bmi_distribution",
    },
    "risk_factor_summary": {
        "name": "Risk Factor Summary",
        "description": "Heart disease rate among exposed vs. unexposed for each risk factor",
        "sql": "SELECT * FROM vw_risk_factor_summary",
    },
    "gen_health_vs_hd": {
        "name": "General Health vs Heart Disease",
        "description": "Self-reported health perception vs. actual heart disease rate",
        "sql": "SELECT * FROM vw_gen_health_vs_hd",
    },
    "risk_tier_summary": {
        "name": "Risk Tier Summary",
        "description": "Statistics by composite risk tier (Low / Medium / High)",
        "sql": "SELECT * FROM vw_risk_tier_summary",
    },
    "comorbidity_impact": {
        "name": "Comorbidity Impact",
        "description": "Heart disease rate by number of comorbid conditions",
        "sql": "SELECT * FROM vw_comorbidity_impact",
    },
}


# ---------------------------------------------------------------------------
# WDC HTML page (Tableau Web Data Connector interface)
# ---------------------------------------------------------------------------

def build_wdc_html(host, port):
    """Generate the Tableau WDC interactive HTML page."""
    base_url = f"http://{host}:{port}"
    dataset_js = json.dumps(
        {k: {"name": v["name"], "description": v["description"]} for k, v in DATASETS.items()},
        indent=2,
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Heart Disease — Tableau Web Data Connector</title>
    <script src="https://connectors.tableau.com/libs/tableauwdc-2.3.latest.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
               background: linear-gradient(135deg, #0f2027, #203a43, #2c5364);
               color: #e0e0e0; min-height: 100vh; display: flex; align-items: center;
               justify-content: center; }}
        .container {{ background: rgba(255,255,255,0.07); border-radius: 16px;
                     padding: 40px; max-width: 700px; width: 90%;
                     backdrop-filter: blur(12px); border: 1px solid rgba(255,255,255,0.12); }}
        h1 {{ font-size: 1.6rem; margin-bottom: 6px; color: #fff; }}
        .subtitle {{ font-size: 0.9rem; color: #8ab4f8; margin-bottom: 28px; }}
        .dataset-list {{ list-style: none; margin-bottom: 24px; }}
        .dataset-list li {{ background: rgba(255,255,255,0.05); border-radius: 10px;
                           padding: 14px 18px; margin-bottom: 10px; cursor: pointer;
                           border: 1px solid rgba(255,255,255,0.08);
                           transition: all 0.2s ease; display: flex; align-items: center; }}
        .dataset-list li:hover {{ background: rgba(138,180,248,0.15);
                                  border-color: rgba(138,180,248,0.3); }}
        .dataset-list li.selected {{ background: rgba(138,180,248,0.2);
                                     border-color: #8ab4f8; }}
        .cb {{ width: 18px; height: 18px; border-radius: 4px; border: 2px solid #8ab4f8;
              margin-right: 14px; flex-shrink: 0; display: flex; align-items: center;
              justify-content: center; }}
        .cb.checked {{ background: #8ab4f8; }}
        .cb.checked::after {{ content: '✓'; color: #0f2027; font-size: 12px; font-weight: 700; }}
        .ds-name {{ font-weight: 600; color: #fff; }}
        .ds-desc {{ font-size: 0.82rem; color: #aaa; margin-top: 2px; }}
        .btn {{ display: block; width: 100%; padding: 14px; border: none; border-radius: 10px;
               background: #8ab4f8; color: #0f2027; font-size: 1rem; font-weight: 600;
               cursor: pointer; transition: background 0.2s; }}
        .btn:hover {{ background: #aecbfa; }}
        .btn:disabled {{ opacity: 0.4; cursor: not-allowed; }}
        .status {{ text-align: center; margin-top: 16px; font-size: 0.85rem; color: #8ab4f8; }}
        .badge {{ display: inline-block; background: rgba(76,175,80,0.2); color: #81c784;
                 padding: 3px 10px; border-radius: 20px; font-size: 0.75rem; margin-bottom: 20px; }}
    </style>
</head>
<body>
<div class="container">
    <h1>❤️ Heart Disease — Tableau Connector</h1>
    <p class="subtitle">Web Data Connector for interactive dashboard analysis</p>
    <span class="badge">🟢 Connected to database</span>

    <ul class="dataset-list" id="datasetList"></ul>

    <button class="btn" id="submitBtn" disabled onclick="submitData()">
        Get Data →
    </button>
    <div class="status" id="status"></div>
</div>

<script>
    const DATASETS = {dataset_js};
    const BASE_URL = "{base_url}";
    let selected = new Set();

    // Build dataset list
    const list = document.getElementById('datasetList');
    Object.entries(DATASETS).forEach(([key, ds]) => {{
        const li = document.createElement('li');
        li.innerHTML = `<div class="cb" id="cb-${{key}}"></div>
                         <div><div class="ds-name">${{ds.name}}</div>
                              <div class="ds-desc">${{ds.description}}</div></div>`;
        li.onclick = () => {{
            if (selected.has(key)) {{ selected.delete(key); li.classList.remove('selected');
                document.getElementById('cb-'+key).classList.remove('checked');
            }} else {{ selected.add(key); li.classList.add('selected');
                document.getElementById('cb-'+key).classList.add('checked');
            }}
            document.getElementById('submitBtn').disabled = selected.size === 0;
        }};
        list.appendChild(li);
    }});

    // Tableau WDC connector
    var connector = tableau.makeConnector();

    connector.getSchema = function(schemaCallback) {{
        var tables = [];
        var selectedDs = JSON.parse(tableau.connectionData);

        var fetches = selectedDs.map(function(dsKey) {{
            return fetch(BASE_URL + '/api/schema/' + dsKey)
                .then(function(r) {{ return r.json(); }})
                .then(function(schema) {{
                    tables.push({{
                        id: dsKey,
                        alias: DATASETS[dsKey].name,
                        columns: schema.columns
                    }});
                }});
        }});

        Promise.all(fetches).then(function() {{
            schemaCallback(tables);
        }});
    }};

    connector.getData = function(table, doneCallback) {{
        fetch(BASE_URL + '/api/data/' + table.tableInfo.id)
            .then(function(r) {{ return r.json(); }})
            .then(function(result) {{
                table.appendRows(result.data);
                doneCallback();
            }});
    }};

    tableau.registerConnector(connector);

    function submitData() {{
        if (selected.size === 0) return;
        tableau.connectionData = JSON.stringify(Array.from(selected));
        tableau.connectionName = "Heart Disease Analysis";
        tableau.submit();
    }}
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# HTTP Request Handler
# ---------------------------------------------------------------------------

class TableauConnectorHandler(BaseHTTPRequestHandler):
    """Handle all HTTP routes for the Tableau connector."""

    server_host = "localhost"
    server_port = 8765

    def log_message(self, fmt, *args):
        """Custom log format."""
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        sys.stderr.write(f"  [{ts}]  {args[0]}\n")

    def _send_json(self, data, status=200):
        """Send a JSON response."""
        body = json.dumps(data, indent=2, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._send_cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html, status=200):
        """Send an HTML response."""
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._send_cors_headers()
        self.end_headers()
        self.wfile.write(body)

    def _send_cors_headers(self):
        """CORS headers for Tableau Desktop/Server/Cloud access."""
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Access-Control-Max-Age", "86400")

    def do_OPTIONS(self):
        """Handle CORS preflight."""
        self.send_response(204)
        self._send_cors_headers()
        self.end_headers()

    def do_GET(self):
        """Route GET requests."""
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")
        params = parse_qs(parsed.query)

        # ── Routes ──
        try:
            if path == "" or path == "/":
                self._handle_index()
            elif path == "/wdc":
                self._handle_wdc()
            elif path == "/api/health":
                self._handle_health()
            elif path == "/api/datasets":
                self._handle_datasets()
            elif path.startswith("/api/schema/"):
                dataset_key = path.split("/api/schema/")[1]
                self._handle_schema(dataset_key)
            elif path.startswith("/api/data/"):
                dataset_key = path.split("/api/data/")[1]
                limit = int(params.get("limit", [0])[0])
                offset = int(params.get("offset", [0])[0])
                self._handle_data(dataset_key, limit, offset)
            elif path == "/api/query":
                # Custom SQL query (read-only)
                sql = params.get("sql", [None])[0]
                self._handle_custom_query(sql)
            elif path == "/api/refresh":
                self._handle_refresh()
            else:
                self._send_json({"error": "Not found", "path": path}, 404)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    # ── Route Handlers ──

    def _handle_index(self):
        """Landing page with connection info."""
        self._send_json({
            "service": "Heart Disease — Tableau Connector",
            "version": "1.0.0",
            "status": "running",
            "database": DB_PATH,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "endpoints": {
                "GET /wdc":                "Tableau Web Data Connector page",
                "GET /api/health":         "Connection health check",
                "GET /api/datasets":       "List all available datasets",
                "GET /api/schema/<key>":   "Get column schema for a dataset",
                "GET /api/data/<key>":     "Get data (supports ?limit=N&offset=N)",
                "GET /api/query?sql=...":  "Run a custom read-only SQL query",
                "GET /api/refresh":        "Re-run pipeline & refresh all exports",
            },
            "tableau_wdc_url": f"http://{self.server_host}:{self.server_port}/wdc",
        })

    def _handle_wdc(self):
        """Serve the Tableau Web Data Connector HTML page."""
        html = build_wdc_html(self.server_host, self.server_port)
        self._send_html(html)

    def _handle_health(self):
        """Health check — verify database connectivity."""
        try:
            conn = get_db()
            row_count = conn.execute(
                "SELECT COUNT(*) FROM heart_disease_tableau;"
            ).fetchone()[0]
            tables = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
            ).fetchall()]
            views = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name;"
            ).fetchall()]

            # Last quality check status
            quality = []
            try:
                for r in conn.execute(
                    "SELECT check_name, status, checked_at FROM data_quality_log ORDER BY check_id;"
                ).fetchall():
                    quality.append({"check": r[0], "status": r[1], "at": r[2]})
            except Exception:
                pass

            conn.close()
            self._send_json({
                "status": "healthy",
                "database": DB_PATH,
                "database_exists": True,
                "row_count": row_count,
                "tables": tables,
                "views": views,
                "quality_checks": quality,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
        except Exception as e:
            self._send_json({
                "status": "unhealthy",
                "error": str(e),
                "database": DB_PATH,
                "database_exists": os.path.exists(DB_PATH),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }, 503)

    def _handle_datasets(self):
        """List all available datasets with row counts."""
        conn = get_db()
        result = []
        for key, ds in DATASETS.items():
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM ({ds['sql']})").fetchone()[0]
            except Exception:
                count = -1
            result.append({
                "key": key,
                "name": ds["name"],
                "description": ds["description"],
                "row_count": count,
                "data_url": f"/api/data/{key}",
                "schema_url": f"/api/schema/{key}",
            })
        conn.close()
        self._send_json({"datasets": result, "count": len(result)})

    def _handle_schema(self, dataset_key):
        """Return column schema for a dataset (used by WDC getSchema)."""
        if dataset_key not in DATASETS:
            self._send_json({"error": f"Unknown dataset: {dataset_key}"}, 404)
            return

        # Infer schema from the query result
        conn = get_db()
        cur = conn.execute(f"{DATASETS[dataset_key]['sql']} LIMIT 1")
        cols = []
        for desc in cur.description:
            col_name = desc[0]
            # Sample one value to infer type more accurately
            row = cur.fetchone()
            if row:
                val = row[col_name] if isinstance(row, sqlite3.Row) else None
                if isinstance(val, int):
                    wdc_type = "int"
                elif isinstance(val, float):
                    wdc_type = "float"
                else:
                    wdc_type = "string"
            else:
                wdc_type = "string"
            cols.append({
                "id": col_name,
                "alias": col_name.replace("_", " ").title(),
                "dataType": wdc_type,
            })
        conn.close()

        # Re-fetch all columns with proper type inference
        conn = get_db()
        sample_rows = conn.execute(f"{DATASETS[dataset_key]['sql']} LIMIT 10").fetchall()
        if sample_rows:
            for i, desc in enumerate(conn.execute(f"{DATASETS[dataset_key]['sql']} LIMIT 1").description):
                col_name = desc[0]
                values = [r[i] for r in sample_rows if r[i] is not None]
                if values:
                    if all(isinstance(v, int) for v in values):
                        t = "int"
                    elif all(isinstance(v, (int, float)) for v in values):
                        t = "float"
                    else:
                        t = "string"
                    for c in cols:
                        if c["id"] == col_name:
                            c["dataType"] = t
        conn.close()

        self._send_json({
            "dataset": dataset_key,
            "columns": cols,
            "column_count": len(cols),
        })

    def _handle_data(self, dataset_key, limit=0, offset=0):
        """Return dataset rows as JSON array."""
        if dataset_key not in DATASETS:
            self._send_json({"error": f"Unknown dataset: {dataset_key}"}, 404)
            return

        sql = DATASETS[dataset_key]["sql"]
        if limit > 0:
            sql += f" LIMIT {limit} OFFSET {offset}"

        data = query_to_dicts(sql)
        self._send_json({
            "dataset": dataset_key,
            "name": DATASETS[dataset_key]["name"],
            "row_count": len(data),
            "data": data,
        })

    def _handle_custom_query(self, sql):
        """Execute a read-only SQL query."""
        if not sql:
            self._send_json({"error": "Missing 'sql' parameter"}, 400)
            return

        # Security: block write operations
        blocked = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "ATTACH"]
        sql_upper = sql.strip().upper()
        for keyword in blocked:
            if sql_upper.startswith(keyword):
                self._send_json({"error": f"Write operations not allowed: {keyword}"}, 403)
                return

        try:
            data = query_to_dicts(sql)
            self._send_json({
                "query": sql,
                "row_count": len(data),
                "data": data,
            })
        except Exception as e:
            self._send_json({"error": f"Query failed: {str(e)}"}, 400)

    def _handle_refresh(self):
        """Re-run the full data pipeline to refresh the database."""
        try:
            # Import and run the pipeline
            sys.path.insert(0, BASE_DIR)
            import db_setup
            import importlib
            importlib.reload(db_setup)
            db_setup.main()

            self._send_json({
                "status": "refreshed",
                "message": "Pipeline re-run complete. Database and exports updated.",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
        except Exception as e:
            self._send_json({
                "status": "error",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }, 500)


# ---------------------------------------------------------------------------
# Server startup
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Heart Disease — Tableau Connector Server")
    parser.add_argument("--host", default="localhost", help="Bind host (default: localhost)")
    parser.add_argument("--port", type=int, default=8765, help="Bind port (default: 8765)")
    args = parser.parse_args()

    # Verify database exists
    if not os.path.exists(DB_PATH):
        print(f"[!] Database not found at {DB_PATH}")
        print("    Run 'python3 db_setup.py' first to create the database.")
        sys.exit(1)

    # Inject host/port into handler class
    TableauConnectorHandler.server_host = args.host
    TableauConnectorHandler.server_port = args.port

    server = HTTPServer((args.host, args.port), TableauConnectorHandler)

    print("=" * 60)
    print("  Heart Disease — Tableau Connector Server")
    print("=" * 60)
    print(f"""
  Status:       🟢 Running
  Database:     {DB_PATH}
  Server:       http://{args.host}:{args.port}

  ── Tableau Connection ──────────────────────────────
  WDC URL:      http://{args.host}:{args.port}/wdc
                (Tableau → Connect → Web Data Connector)

  REST API:     http://{args.host}:{args.port}/api/data/heart_disease_tableau
                (Tableau → Connect → Web Data Connector → paste URL)

  ── API Endpoints ───────────────────────────────────
  GET /             Service info & endpoint directory
  GET /wdc          Tableau Web Data Connector page
  GET /api/health   Health check & DB status
  GET /api/datasets List all datasets with row counts
  GET /api/schema/  Column schema for WDC
  GET /api/data/    JSON data feed (supports ?limit=&offset=)
  GET /api/query    Custom read-only SQL (?sql=SELECT...)
  GET /api/refresh  Re-run pipeline & refresh database

  Press Ctrl+C to stop.
""")
    print("=" * 60)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server stopped.")
        server.server_close()


if __name__ == "__main__":
    main()
