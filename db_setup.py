"""
Heart Disease Dataset — SQL Database Pipeline
================================================
This script:
  1. Creates a structured SQLite database (heart_disease.db)
  2. Ingests the raw CSV into a staging table
  3. Cleans, filters, transforms, and enriches the data using SQL
  4. Produces Tableau-ready views/tables and exported CSVs
"""

import sqlite3
import csv
import os

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(BASE_DIR, "Heart_new2.csv")
DB_PATH = os.path.join(BASE_DIR, "heart_disease.db")
EXPORT_DIR = os.path.join(BASE_DIR, "tableau_exports")

os.makedirs(EXPORT_DIR, exist_ok=True)


def connect_db():
    """Return a connection with WAL mode enabled for performance."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


# ===================================================================
# STEP 1 — Create schema & load raw CSV into staging table
# ===================================================================
def create_schema(conn):
    """Create the raw staging table and dimension/lookup tables."""
    cur = conn.cursor()

    # Drop existing tables for idempotent re-runs
    cur.executescript("""
        DROP TABLE IF EXISTS raw_heart_data;
        DROP TABLE IF EXISTS heart_disease_clean;
        DROP TABLE IF EXISTS heart_disease_tableau;
        DROP TABLE IF EXISTS dim_age_category;
        DROP TABLE IF EXISTS dim_gen_health;
        DROP TABLE IF EXISTS dim_bmi_category;
        DROP TABLE IF EXISTS dim_diabetic_status;
        DROP TABLE IF EXISTS data_quality_log;
    """)

    # ---- Raw staging table (mirrors CSV exactly) ----
    cur.execute("""
        CREATE TABLE raw_heart_data (
            row_id            INTEGER PRIMARY KEY AUTOINCREMENT,
            HeartDisease      TEXT,
            BMI               TEXT,
            Smoking           TEXT,
            AlcoholDrinking   TEXT,
            Stroke            TEXT,
            PhysicalHealth    TEXT,
            MentalHealth      TEXT,
            DiffWalking       TEXT,
            Sex               TEXT,
            AgeCategory       TEXT,
            Race              TEXT,
            Diabetic          TEXT,
            PhysicalActivity  TEXT,
            GenHealth         TEXT,
            SleepTime         TEXT,
            Asthma            TEXT,
            KidneyDisease     TEXT,
            SkinCancer        TEXT
        );
    """)

    # ---- Dimension: Age Category ----
    cur.execute("""
        CREATE TABLE dim_age_category (
            age_cat_id    INTEGER PRIMARY KEY,
            age_category  TEXT UNIQUE NOT NULL,
            age_mid       REAL NOT NULL,
            age_order     INTEGER NOT NULL
        );
    """)
    age_data = [
        (1, '18-24',        21.0, 1),
        (2, '25-29',        27.0, 2),
        (3, '30-34',        32.0, 3),
        (4, '35-39',        37.0, 4),
        (5, '40-44',        42.0, 5),
        (6, '45-49',        47.0, 6),
        (7, '50-54',        52.0, 7),
        (8, '55-59',        57.0, 8),
        (9, '60-64',        62.0, 9),
        (10, '65-69',       67.0, 10),
        (11, '70-74',       72.0, 11),
        (12, '75-79',       77.0, 12),
        (13, '80 or older', 82.0, 13),
    ]
    cur.executemany(
        "INSERT INTO dim_age_category VALUES (?,?,?,?);", age_data
    )

    # ---- Dimension: General Health ----
    cur.execute("""
        CREATE TABLE dim_gen_health (
            gen_health_id    INTEGER PRIMARY KEY,
            gen_health_label TEXT UNIQUE NOT NULL,
            health_score     INTEGER NOT NULL,
            health_order     INTEGER NOT NULL
        );
    """)
    gen_health_data = [
        (1, 'Poor',      1, 1),
        (2, 'Fair',      2, 2),
        (3, 'Good',      3, 3),
        (4, 'Very good', 4, 4),
        (5, 'Excellent', 5, 5),
    ]
    cur.executemany(
        "INSERT INTO dim_gen_health VALUES (?,?,?,?);", gen_health_data
    )

    # ---- Dimension: BMI Category (WHO classification) ----
    cur.execute("""
        CREATE TABLE dim_bmi_category (
            bmi_cat_id   INTEGER PRIMARY KEY,
            bmi_category TEXT UNIQUE NOT NULL,
            bmi_min      REAL NOT NULL,
            bmi_max      REAL NOT NULL,
            bmi_order    INTEGER NOT NULL
        );
    """)
    bmi_data = [
        (1, 'Underweight',  0.0,  18.5, 1),
        (2, 'Normal',      18.5,  25.0, 2),
        (3, 'Overweight',  25.0,  30.0, 3),
        (4, 'Obese',       30.0, 999.0, 4),
    ]
    cur.executemany(
        "INSERT INTO dim_bmi_category VALUES (?,?,?,?,?);", bmi_data
    )

    # ---- Dimension: Diabetic Status ----
    cur.execute("""
        CREATE TABLE dim_diabetic_status (
            diabetic_id    INTEGER PRIMARY KEY,
            diabetic_raw   TEXT UNIQUE NOT NULL,
            diabetic_label TEXT NOT NULL,
            is_diabetic    INTEGER NOT NULL   -- 0/1 boolean
        );
    """)
    diabetic_data = [
        (1, 'No',                          'No',                    0),
        (2, 'No, borderline diabetes',     'Borderline',            0),
        (3, 'Yes',                         'Yes',                   1),
        (4, 'Yes (during pregnancy)',      'Gestational',           1),
    ]
    cur.executemany(
        "INSERT INTO dim_diabetic_status VALUES (?,?,?,?);", diabetic_data
    )

    # ---- Data quality log ----
    cur.execute("""
        CREATE TABLE data_quality_log (
            check_id     INTEGER PRIMARY KEY AUTOINCREMENT,
            check_name   TEXT NOT NULL,
            check_sql    TEXT,
            result_value TEXT,
            status       TEXT,   -- PASS / FAIL / WARN
            checked_at   TEXT DEFAULT (datetime('now'))
        );
    """)

    conn.commit()
    print("[✓] Schema created successfully.")


def load_csv(conn):
    """Load raw CSV rows into the staging table."""
    cur = conn.cursor()
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            rows.append((
                r['HeartDisease'], r['BMI'], r['Smoking'],
                r['AlcoholDrinking'], r['Stroke'],
                r['PhysicalHealth'], r['MentalHealth'],
                r['DiffWalking'], r['Sex'], r['AgeCategory'],
                r['Race'], r['Diabetic'], r['PhysicalActivity'],
                r['GenHealth'], r['SleepTime'], r['Asthma'],
                r['KidneyDisease'], r['SkinCancer'],
            ))
    cur.executemany("""
        INSERT INTO raw_heart_data (
            HeartDisease, BMI, Smoking, AlcoholDrinking, Stroke,
            PhysicalHealth, MentalHealth, DiffWalking, Sex, AgeCategory,
            Race, Diabetic, PhysicalActivity, GenHealth, SleepTime,
            Asthma, KidneyDisease, SkinCancer
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
    """, rows)
    conn.commit()
    print(f"[✓] Loaded {len(rows):,} rows into raw_heart_data.")


# ===================================================================
# STEP 2 — Data‑quality checks (logged to data_quality_log)
# ===================================================================
QUALITY_CHECKS = [
    (
        "Row count > 0",
        "SELECT COUNT(*) FROM raw_heart_data;",
        lambda v: "PASS" if int(v) > 0 else "FAIL",
    ),
    (
        "No NULL HeartDisease",
        "SELECT COUNT(*) FROM raw_heart_data WHERE HeartDisease IS NULL OR TRIM(HeartDisease) = '';",
        lambda v: "PASS" if int(v) == 0 else "FAIL",
    ),
    (
        "BMI is numeric",
        "SELECT COUNT(*) FROM raw_heart_data WHERE CAST(BMI AS REAL) IS NULL;",
        lambda v: "PASS" if int(v) == 0 else "FAIL",
    ),
    (
        "PhysicalHealth 0-30",
        "SELECT COUNT(*) FROM raw_heart_data WHERE CAST(PhysicalHealth AS INTEGER) < 0 OR CAST(PhysicalHealth AS INTEGER) > 30;",
        lambda v: "PASS" if int(v) == 0 else "FAIL",
    ),
    (
        "MentalHealth 0-30",
        "SELECT COUNT(*) FROM raw_heart_data WHERE CAST(MentalHealth AS INTEGER) < 0 OR CAST(MentalHealth AS INTEGER) > 30;",
        lambda v: "PASS" if int(v) == 0 else "FAIL",
    ),
    (
        "SleepTime 1-24",
        "SELECT COUNT(*) FROM raw_heart_data WHERE CAST(SleepTime AS REAL) < 1 OR CAST(SleepTime AS REAL) > 24;",
        lambda v: "PASS" if int(v) == 0 else "FAIL",
    ),
    (
        "HeartDisease binary (Yes/No)",
        "SELECT COUNT(DISTINCT HeartDisease) FROM raw_heart_data;",
        lambda v: "PASS" if int(v) == 2 else "FAIL",
    ),
    (
        "No duplicate rows",
        """SELECT COUNT(*) FROM (
             SELECT HeartDisease,BMI,Smoking,AlcoholDrinking,Stroke,
                    PhysicalHealth,MentalHealth,DiffWalking,Sex,AgeCategory,
                    Race,Diabetic,PhysicalActivity,GenHealth,SleepTime,
                    Asthma,KidneyDisease,SkinCancer,
                    COUNT(*) AS cnt
             FROM raw_heart_data
             GROUP BY HeartDisease,BMI,Smoking,AlcoholDrinking,Stroke,
                      PhysicalHealth,MentalHealth,DiffWalking,Sex,AgeCategory,
                      Race,Diabetic,PhysicalActivity,GenHealth,SleepTime,
                      Asthma,KidneyDisease,SkinCancer
             HAVING cnt > 1
           );""",
        lambda v: "PASS" if int(v) == 0 else f"WARN ({v} groups with duplicates)",
    ),
]


def run_quality_checks(conn):
    """Execute quality checks and log results."""
    cur = conn.cursor()
    print("\n── Data Quality Checks ──")
    for name, sql, evaluator in QUALITY_CHECKS:
        val = str(cur.execute(sql).fetchone()[0])
        status = evaluator(val)
        cur.execute(
            "INSERT INTO data_quality_log (check_name, check_sql, result_value, status) VALUES (?,?,?,?);",
            (name, sql, val, status),
        )
        icon = "✅" if status == "PASS" else ("⚠️" if "WARN" in status else "❌")
        print(f"  {icon}  {name}: {val} → {status}")
    conn.commit()


# ===================================================================
# STEP 3 — Clean, transform, enrich → heart_disease_clean
# ===================================================================
def clean_and_transform(conn):
    """
    SQL-based cleaning & transformation:
      • Cast types (TEXT → REAL / INTEGER)
      • Map Yes/No → 1/0 for all binary columns
      • Standardize Diabetic text variations
      • Compute BMI category
      • Derive risk-score column
      • Remove exact-duplicate rows (keep lowest row_id)
    """
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE heart_disease_clean AS
        WITH deduped AS (
            -- Keep only the first occurrence of each duplicate set
            SELECT MIN(row_id) AS row_id
            FROM raw_heart_data
            GROUP BY HeartDisease, BMI, Smoking, AlcoholDrinking, Stroke,
                     PhysicalHealth, MentalHealth, DiffWalking, Sex,
                     AgeCategory, Race, Diabetic, PhysicalActivity,
                     GenHealth, SleepTime, Asthma, KidneyDisease, SkinCancer
        )
        SELECT
            r.row_id                                        AS record_id,

            -- Target variable (binary)
            CASE WHEN r.HeartDisease = 'Yes' THEN 1 ELSE 0 END
                                                            AS heart_disease,

            -- Numeric columns (cast + round)
            ROUND(CAST(r.BMI AS REAL), 2)                   AS bmi,
            CAST(r.PhysicalHealth AS INTEGER)                AS physical_health_days,
            CAST(r.MentalHealth AS INTEGER)                  AS mental_health_days,
            ROUND(CAST(r.SleepTime AS REAL), 1)             AS sleep_hours,

            -- Binary Yes/No → 1/0
            CASE WHEN r.Smoking        = 'Yes' THEN 1 ELSE 0 END AS smoking,
            CASE WHEN r.AlcoholDrinking= 'Yes' THEN 1 ELSE 0 END AS alcohol_drinking,
            CASE WHEN r.Stroke         = 'Yes' THEN 1 ELSE 0 END AS stroke,
            CASE WHEN r.DiffWalking    = 'Yes' THEN 1 ELSE 0 END AS diff_walking,
            CASE WHEN r.PhysicalActivity='Yes' THEN 1 ELSE 0 END AS physical_activity,
            CASE WHEN r.Asthma         = 'Yes' THEN 1 ELSE 0 END AS asthma,
            CASE WHEN r.KidneyDisease  = 'Yes' THEN 1 ELSE 0 END AS kidney_disease,
            CASE WHEN r.SkinCancer     = 'Yes' THEN 1 ELSE 0 END AS skin_cancer,

            -- Categorical columns (cleaned)
            TRIM(r.Sex)                                      AS sex,
            TRIM(r.AgeCategory)                              AS age_category,
            TRIM(r.Race)                                     AS race,
            TRIM(r.GenHealth)                                AS gen_health,

            -- Standardize Diabetic categories
            CASE
                WHEN TRIM(r.Diabetic) = 'Yes'                           THEN 'Yes'
                WHEN TRIM(r.Diabetic) = 'No'                            THEN 'No'
                WHEN TRIM(r.Diabetic) LIKE '%borderline%'               THEN 'Borderline'
                WHEN TRIM(r.Diabetic) LIKE '%pregnan%'                  THEN 'Gestational'
                ELSE TRIM(r.Diabetic)
            END                                              AS diabetic_status,

            -- ── Derived / Enriched Columns ──

            -- BMI Category (WHO)
            CASE
                WHEN CAST(r.BMI AS REAL) < 18.5  THEN 'Underweight'
                WHEN CAST(r.BMI AS REAL) < 25.0  THEN 'Normal'
                WHEN CAST(r.BMI AS REAL) < 30.0  THEN 'Overweight'
                ELSE                                   'Obese'
            END                                              AS bmi_category,

            -- General Health Score (1–5)
            CASE r.GenHealth
                WHEN 'Poor'      THEN 1
                WHEN 'Fair'      THEN 2
                WHEN 'Good'      THEN 3
                WHEN 'Very good' THEN 4
                WHEN 'Excellent' THEN 5
                ELSE NULL
            END                                              AS gen_health_score,

            -- Age Midpoint (for numeric analysis)
            CASE
                WHEN r.AgeCategory = '18-24'        THEN 21
                WHEN r.AgeCategory = '25-29'        THEN 27
                WHEN r.AgeCategory = '30-34'        THEN 32
                WHEN r.AgeCategory = '35-39'        THEN 37
                WHEN r.AgeCategory = '40-44'        THEN 42
                WHEN r.AgeCategory = '45-49'        THEN 47
                WHEN r.AgeCategory = '50-54'        THEN 52
                WHEN r.AgeCategory = '55-59'        THEN 57
                WHEN r.AgeCategory = '60-64'        THEN 62
                WHEN r.AgeCategory = '65-69'        THEN 67
                WHEN r.AgeCategory = '70-74'        THEN 72
                WHEN r.AgeCategory = '75-79'        THEN 77
                WHEN r.AgeCategory = '80 or older'  THEN 82
                ELSE NULL
            END                                              AS age_midpoint,

            -- Comorbidity count (sum of chronic conditions)
            (CASE WHEN r.Stroke        = 'Yes' THEN 1 ELSE 0 END)
          + (CASE WHEN r.Diabetic IN ('Yes','Yes (during pregnancy)') THEN 1 ELSE 0 END)
          + (CASE WHEN r.Asthma        = 'Yes' THEN 1 ELSE 0 END)
          + (CASE WHEN r.KidneyDisease = 'Yes' THEN 1 ELSE 0 END)
          + (CASE WHEN r.SkinCancer    = 'Yes' THEN 1 ELSE 0 END)
                                                             AS comorbidity_count,

            -- Composite risk score (higher = more risk factors)
            ROUND(
                (CASE WHEN r.Smoking         = 'Yes' THEN 1 ELSE 0 END) * 1.0
              + (CASE WHEN r.AlcoholDrinking = 'Yes' THEN 1 ELSE 0 END) * 0.5
              + (CASE WHEN r.Stroke          = 'Yes' THEN 2 ELSE 0 END)
              + (CASE WHEN r.DiffWalking     = 'Yes' THEN 1 ELSE 0 END)
              + (CASE WHEN r.Diabetic IN ('Yes','Yes (during pregnancy)') THEN 1.5 ELSE 0 END)
              + (CASE WHEN r.KidneyDisease   = 'Yes' THEN 1.5 ELSE 0 END)
              + (CASE WHEN CAST(r.BMI AS REAL) >= 30 THEN 1 ELSE 0 END)
              + (CASE WHEN CAST(r.PhysicalHealth AS INTEGER) >= 15 THEN 1 ELSE 0 END)
              + (CASE WHEN CAST(r.MentalHealth AS INTEGER)   >= 15 THEN 1 ELSE 0 END)
              + (CASE WHEN r.PhysicalActivity = 'No' THEN 0.5 ELSE 0 END)
              + (CASE r.GenHealth
                    WHEN 'Poor' THEN 2  WHEN 'Fair' THEN 1
                    ELSE 0 END)
            , 2)                                             AS risk_score

        FROM raw_heart_data r
        INNER JOIN deduped d ON r.row_id = d.row_id;
    """)

    clean_count = cur.execute("SELECT COUNT(*) FROM heart_disease_clean;").fetchone()[0]
    raw_count = cur.execute("SELECT COUNT(*) FROM raw_heart_data;").fetchone()[0]
    removed = raw_count - clean_count

    conn.commit()
    print(f"\n[✓] Cleaned table created: {clean_count:,} rows  "
          f"({removed:,} duplicates removed).")


# ===================================================================
# STEP 4 — Create Tableau-optimised flat table with joined dimensions
# ===================================================================
def create_tableau_table(conn):
    """Build the final denormalized table Tableau will read."""
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE heart_disease_tableau AS
        SELECT
            c.record_id,
            c.heart_disease,
            CASE WHEN c.heart_disease = 1 THEN 'Yes' ELSE 'No' END AS heart_disease_label,

            c.bmi,
            c.bmi_category,
            bc.bmi_order,

            c.smoking,
            c.alcohol_drinking,
            c.stroke,
            c.diff_walking,
            c.physical_activity,
            c.asthma,
            c.kidney_disease,
            c.skin_cancer,

            c.physical_health_days,
            c.mental_health_days,
            c.sleep_hours,

            c.sex,
            c.age_category,
            ac.age_mid   AS age_midpoint,
            ac.age_order AS age_order,

            c.race,
            c.gen_health,
            gh.health_score AS gen_health_score,
            gh.health_order AS gen_health_order,

            c.diabetic_status,
            COALESCE(ds.is_diabetic, 0)  AS is_diabetic,

            c.comorbidity_count,
            c.risk_score,

            -- Risk tier for easy Tableau color coding
            CASE
                WHEN c.risk_score >= 6  THEN 'High'
                WHEN c.risk_score >= 3  THEN 'Medium'
                ELSE                         'Low'
            END AS risk_tier

        FROM heart_disease_clean c
        LEFT JOIN dim_age_category   ac ON c.age_category  = ac.age_category
        LEFT JOIN dim_gen_health     gh ON c.gen_health     = gh.gen_health_label
        LEFT JOIN dim_bmi_category   bc ON c.bmi_category   = bc.bmi_category
        LEFT JOIN dim_diabetic_status ds ON c.diabetic_status = ds.diabetic_label;
    """)

    conn.commit()
    count = cur.execute("SELECT COUNT(*) FROM heart_disease_tableau;").fetchone()[0]
    print(f"[✓] Tableau-ready table created: {count:,} rows.")


# ===================================================================
# STEP 5 — Build analytical SQL views (Tableau can connect to these)
# ===================================================================
def create_views(conn):
    """Create reusable SQL views for common Tableau analyses."""
    cur = conn.cursor()

    # 1. Heart disease prevalence by age & sex
    cur.execute("""
        CREATE VIEW IF NOT EXISTS vw_prevalence_by_age_sex AS
        SELECT
            age_category,
            age_order,
            sex,
            COUNT(*)                                       AS total_count,
            SUM(heart_disease)                             AS hd_count,
            ROUND(100.0 * SUM(heart_disease) / COUNT(*), 2) AS hd_rate_pct
        FROM heart_disease_tableau
        GROUP BY age_category, age_order, sex
        ORDER BY age_order, sex;
    """)

    # 2. BMI distribution by heart disease status
    cur.execute("""
        CREATE VIEW IF NOT EXISTS vw_bmi_distribution AS
        SELECT
            heart_disease_label,
            bmi_category,
            bmi_order,
            COUNT(*)                AS count,
            ROUND(AVG(bmi), 2)      AS avg_bmi,
            ROUND(MIN(bmi), 2)      AS min_bmi,
            ROUND(MAX(bmi), 2)      AS max_bmi
        FROM heart_disease_tableau
        GROUP BY heart_disease_label, bmi_category, bmi_order
        ORDER BY bmi_order;
    """)

    # 3. Risk factor correlation summary
    cur.execute("""
        CREATE VIEW IF NOT EXISTS vw_risk_factor_summary AS
        SELECT
            'Smoking'           AS risk_factor, ROUND(100.0*SUM(CASE WHEN smoking=1 AND heart_disease=1 THEN 1 ELSE 0 END)/NULLIF(SUM(smoking),0),2) AS hd_rate_among_exposed, ROUND(100.0*SUM(CASE WHEN smoking=0 AND heart_disease=1 THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN smoking=0 THEN 1 ELSE 0 END),0),2) AS hd_rate_among_unexposed FROM heart_disease_tableau
        UNION ALL SELECT 'AlcoholDrinking', ROUND(100.0*SUM(CASE WHEN alcohol_drinking=1 AND heart_disease=1 THEN 1 ELSE 0 END)/NULLIF(SUM(alcohol_drinking),0),2), ROUND(100.0*SUM(CASE WHEN alcohol_drinking=0 AND heart_disease=1 THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN alcohol_drinking=0 THEN 1 ELSE 0 END),0),2) FROM heart_disease_tableau
        UNION ALL SELECT 'Stroke',          ROUND(100.0*SUM(CASE WHEN stroke=1 AND heart_disease=1 THEN 1 ELSE 0 END)/NULLIF(SUM(stroke),0),2),           ROUND(100.0*SUM(CASE WHEN stroke=0 AND heart_disease=1 THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN stroke=0 THEN 1 ELSE 0 END),0),2) FROM heart_disease_tableau
        UNION ALL SELECT 'DiffWalking',     ROUND(100.0*SUM(CASE WHEN diff_walking=1 AND heart_disease=1 THEN 1 ELSE 0 END)/NULLIF(SUM(diff_walking),0),2),     ROUND(100.0*SUM(CASE WHEN diff_walking=0 AND heart_disease=1 THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN diff_walking=0 THEN 1 ELSE 0 END),0),2) FROM heart_disease_tableau
        UNION ALL SELECT 'KidneyDisease',   ROUND(100.0*SUM(CASE WHEN kidney_disease=1 AND heart_disease=1 THEN 1 ELSE 0 END)/NULLIF(SUM(kidney_disease),0),2),   ROUND(100.0*SUM(CASE WHEN kidney_disease=0 AND heart_disease=1 THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN kidney_disease=0 THEN 1 ELSE 0 END),0),2) FROM heart_disease_tableau
        UNION ALL SELECT 'Diabetic',        ROUND(100.0*SUM(CASE WHEN is_diabetic=1 AND heart_disease=1 THEN 1 ELSE 0 END)/NULLIF(SUM(is_diabetic),0),2),        ROUND(100.0*SUM(CASE WHEN is_diabetic=0 AND heart_disease=1 THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN is_diabetic=0 THEN 1 ELSE 0 END),0),2) FROM heart_disease_tableau;
    """)

    # 4. General health vs heart disease
    cur.execute("""
        CREATE VIEW IF NOT EXISTS vw_gen_health_vs_hd AS
        SELECT
            gen_health,
            gen_health_order,
            COUNT(*)                                        AS total,
            SUM(heart_disease)                              AS hd_count,
            ROUND(100.0 * SUM(heart_disease) / COUNT(*), 2) AS hd_rate_pct,
            ROUND(AVG(risk_score), 2)                       AS avg_risk_score
        FROM heart_disease_tableau
        GROUP BY gen_health, gen_health_order
        ORDER BY gen_health_order;
    """)

    # 5. Risk tier summary
    cur.execute("""
        CREATE VIEW IF NOT EXISTS vw_risk_tier_summary AS
        SELECT
            risk_tier,
            COUNT(*)                                        AS total,
            SUM(heart_disease)                              AS hd_count,
            ROUND(100.0 * SUM(heart_disease) / COUNT(*), 2) AS hd_rate_pct,
            ROUND(AVG(bmi), 2)                              AS avg_bmi,
            ROUND(AVG(sleep_hours), 2)                      AS avg_sleep,
            ROUND(AVG(physical_health_days), 2)             AS avg_phys_health_days,
            ROUND(AVG(mental_health_days), 2)               AS avg_mental_health_days
        FROM heart_disease_tableau
        GROUP BY risk_tier
        ORDER BY CASE risk_tier WHEN 'Low' THEN 1 WHEN 'Medium' THEN 2 WHEN 'High' THEN 3 END;
    """)

    # 6. Comorbidity impact
    cur.execute("""
        CREATE VIEW IF NOT EXISTS vw_comorbidity_impact AS
        SELECT
            comorbidity_count,
            COUNT(*)                                        AS total,
            SUM(heart_disease)                              AS hd_count,
            ROUND(100.0 * SUM(heart_disease) / COUNT(*), 2) AS hd_rate_pct
        FROM heart_disease_tableau
        GROUP BY comorbidity_count
        ORDER BY comorbidity_count;
    """)

    conn.commit()
    print("[✓] 6 analytical views created.")


# ===================================================================
# STEP 6 — Export Tableau-ready CSVs
# ===================================================================
def export_csv(conn, query, filename):
    """Export a query result to CSV."""
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    path = os.path.join(EXPORT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)
    print(f"   → {filename}  ({len(rows):,} rows)")


def export_all(conn):
    """Export the main table and all views to CSV for Tableau."""
    print("\n── Exporting CSVs to tableau_exports/ ──")
    exports = [
        ("SELECT * FROM heart_disease_tableau;",       "heart_disease_tableau.csv"),
        ("SELECT * FROM vw_prevalence_by_age_sex;",    "prevalence_by_age_sex.csv"),
        ("SELECT * FROM vw_bmi_distribution;",         "bmi_distribution.csv"),
        ("SELECT * FROM vw_risk_factor_summary;",      "risk_factor_summary.csv"),
        ("SELECT * FROM vw_gen_health_vs_hd;",         "gen_health_vs_hd.csv"),
        ("SELECT * FROM vw_risk_tier_summary;",        "risk_tier_summary.csv"),
        ("SELECT * FROM vw_comorbidity_impact;",       "comorbidity_impact.csv"),
    ]
    for query, fname in exports:
        export_csv(conn, query, fname)


# ===================================================================
# STEP 7 — Print summary statistics
# ===================================================================
def print_summary(conn):
    """Print a quick summary of the database contents."""
    cur = conn.cursor()
    print("\n" + "=" * 60)
    print("  DATABASE SUMMARY")
    print("=" * 60)

    # Table counts
    tables = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
    ).fetchall()
    print(f"\n  Tables: {len(tables)}")
    for (t,) in tables:
        cnt = cur.execute(f"SELECT COUNT(*) FROM [{t}];").fetchone()[0]
        print(f"    • {t:35s}  {cnt:>7,} rows")

    views = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name;"
    ).fetchall()
    print(f"\n  Views: {len(views)}")
    for (v,) in views:
        print(f"    • {v}")

    # Key stats from final table
    stats = cur.execute("""
        SELECT
            COUNT(*)                                         AS total_records,
            SUM(heart_disease)                               AS hd_yes,
            ROUND(100.0 * SUM(heart_disease) / COUNT(*), 2) AS hd_pct,
            ROUND(AVG(bmi), 2)                               AS avg_bmi,
            ROUND(AVG(sleep_hours), 2)                       AS avg_sleep,
            ROUND(AVG(risk_score), 2)                        AS avg_risk,
            COUNT(DISTINCT age_category)                     AS n_age_groups,
            COUNT(DISTINCT race)                             AS n_races
        FROM heart_disease_tableau;
    """).fetchone()

    print(f"\n  ── Key Statistics ──")
    print(f"    Total records ........... {stats[0]:,}")
    print(f"    Heart disease (Yes) ..... {stats[1]:,}  ({stats[2]}%)")
    print(f"    Avg BMI ................. {stats[3]}")
    print(f"    Avg Sleep (hrs) ......... {stats[4]}")
    print(f"    Avg Risk Score .......... {stats[5]}")
    print(f"    Age groups .............. {stats[6]}")
    print(f"    Race categories ......... {stats[7]}")

    # Quality log
    print(f"\n  ── Data Quality Log ──")
    logs = cur.execute("SELECT check_name, result_value, status FROM data_quality_log;").fetchall()
    for name, val, status in logs:
        icon = "✅" if status == "PASS" else ("⚠️" if "WARN" in status else "❌")
        print(f"    {icon}  {name}: {val} → {status}")

    print("\n" + "=" * 60)
    print(f"  Database saved to: {DB_PATH}")
    print(f"  Tableau CSVs in:   {EXPORT_DIR}/")
    print("=" * 60 + "\n")


# ===================================================================
# MAIN — Run the full pipeline
# ===================================================================
def main():
    print("=" * 60)
    print("  Heart Disease SQL Database Pipeline")
    print("=" * 60 + "\n")

    conn = connect_db()
    try:
        create_schema(conn)
        load_csv(conn)
        run_quality_checks(conn)
        clean_and_transform(conn)
        create_tableau_table(conn)
        create_views(conn)
        export_all(conn)
        print_summary(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
