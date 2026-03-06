# Heart Disease Indicators - Tableau Analysis Project

## Problem Statement
Analyze key health indicators that contribute to heart disease using the Behavioral Risk Factor Surveillance System (BRFSS) dataset. The goal is to identify patterns, correlations, and risk factors associated with heart disease through interactive Tableau visualizations.

## Dataset Overview
- **File:** `Heart_new2.csv`
- **Records:** ~4,000+ survey responses
- **Target Variable:** `HeartDisease` (Yes/No)

## Column Descriptions

| Column | Description |
|--------|-------------|
| **HeartDisease** | Target variable - whether the respondent has heart disease (Yes/No) |
| **BMI** | Body Mass Index - ratio of weight to height indicating body mass category |
| **Smoking** | Whether the respondent has smoked at least 100 cigarettes in their lifetime |
| **AlcoholDrinking** | Heavy alcohol consumption (adult men >14 drinks/week, women >7 drinks/week) |
| **Stroke** | Whether the respondent has ever had a stroke |
| **PhysicalHealth** | Number of days in the past 30 days of poor physical health (0-30) |
| **MentalHealth** | Number of days in the past 30 days of poor mental health (0-30) |
| **DiffWalking** | Difficulty walking or climbing stairs |
| **Sex** | Gender of the respondent (Male/Female) |
| **AgeCategory** | Age group of the respondent (e.g., 18-24, 25-29, ..., 80 or older) |
| **Race** | Race/ethnicity of the respondent |
| **Diabetic** | Diabetes status (Yes / No / No borderline diabetes / Yes during pregnancy) |
| **PhysicalActivity** | Physical activity or exercise in the past 30 days (outside regular job) |
| **GenHealth** | Self-reported general health (Excellent/Very good/Good/Fair/Poor) |
| **SleepTime** | Average hours of sleep per 24-hour period |
| **Asthma** | Whether the respondent has asthma |
| **KidneyDisease** | Whether the respondent has kidney disease |
| **SkinCancer** | Whether the respondent has skin cancer |

## Data Validation Summary

| Check | Status |
|-------|--------|
| No missing/null values | ✅ Passed |
| Target variable (HeartDisease) is binary (Yes/No) | ✅ Passed |
| BMI values are numeric and within reasonable range (12-95) | ✅ Passed |
| PhysicalHealth & MentalHealth values are 0-30 | ✅ Passed |
| SleepTime values are 1-24 | ✅ Passed |
| All categorical columns have valid entries | ✅ Passed |
| No duplicate header rows in data | ✅ Passed |
| Dataset is Tableau-ready (CSV format, UTF-8 encoding) | ✅ Passed |

## Data Quality Notes
- All boolean-type columns (Smoking, AlcoholDrinking, Stroke, etc.) use consistent Yes/No encoding
- The `Diabetic` column contains 4 categories: `Yes`, `No`, `No, borderline diabetes`, `Yes (during pregnancy)`
- `AgeCategory` uses binned ranges (e.g., "55-59", "80 or older")
- No data imputation was needed - dataset is complete

## Tools Used
- **Python + SQLite** — Structured database storage, SQL-based cleaning & transformation
- **Tableau** — Data visualization and dashboard creation
- **GitHub** — Version control and project hosting

## Project Structure
```
├── Heart_new2.csv                          # Original raw dataset
├── heart_disease.db                        # SQLite database (all tables & views)
├── db_setup.py                             # Full SQL pipeline script
├── tableau_exports/                        # Tableau-ready CSV exports
│   ├── heart_disease_tableau.csv           #   Main denormalized fact table
│   ├── prevalence_by_age_sex.csv           #   HD prevalence by age & sex
│   ├── bmi_distribution.csv                #   BMI stats by HD status
│   ├── risk_factor_summary.csv             #   Risk factor comparison
│   ├── gen_health_vs_hd.csv                #   General health vs HD
│   ├── risk_tier_summary.csv               #   Low/Medium/High risk tiers
│   └── comorbidity_impact.csv              #   Comorbidity count vs HD
├── README.md                               # Project documentation
```

## SQL Database Pipeline

Run the pipeline with:
```bash
python3 db_setup.py
```

### Pipeline Steps
| Step | Description |
|------|-------------|
| **1. Schema & Ingest** | Creates staging table (`raw_heart_data`) + dimension tables and loads all 4,500 CSV rows |
| **2. Quality Checks** | Runs 8 automated checks (nulls, ranges, types, duplicates) and logs results to `data_quality_log` |
| **3. Clean & Transform** | Casts types, maps Yes/No → 1/0, deduplicates, derives BMI category, risk score, comorbidity count → `heart_disease_clean` |
| **4. Tableau Table** | Joins with dimension tables to build denormalized `heart_disease_tableau` with sort orders & risk tiers |
| **5. Analytical Views** | Creates 6 SQL views for common Tableau analyses |
| **6. CSV Export** | Exports the main table and all views to `tableau_exports/` |

### Database Tables
| Table | Description |
|-------|-------------|
| `raw_heart_data` | Original CSV data (staging) — 4,500 rows |
| `heart_disease_clean` | Cleaned & transformed — 4,494 rows (6 duplicates removed) |
| `heart_disease_tableau` | Final denormalized table with dimension joins — 4,494 rows |
| `dim_age_category` | Age category → midpoint & sort order |
| `dim_gen_health` | General health → numeric score & sort order |
| `dim_bmi_category` | WHO BMI classification ranges |
| `dim_diabetic_status` | Standardized diabetic status mapping |
| `data_quality_log` | Audit trail of all quality checks |

### Derived Columns (SQL Transformations)
| Column | Logic |
|--------|-------|
| `bmi_category` | WHO classification: Underweight / Normal / Overweight / Obese |
| `gen_health_score` | Poor=1, Fair=2, Good=3, Very good=4, Excellent=5 |
| `age_midpoint` | Numeric midpoint of each age bin (e.g., "55-59" → 57) |
| `comorbidity_count` | Sum of stroke + diabetes + asthma + kidney disease + skin cancer |
| `risk_score` | Weighted composite of all risk factors (0–12 scale) |
| `risk_tier` | Low (< 3) / Medium (3–5.9) / High (≥ 6) |

### SQL Views (for Tableau)
| View | Purpose |
|------|---------|
| `vw_prevalence_by_age_sex` | Heart disease rate by age group & sex |
| `vw_bmi_distribution` | BMI statistics grouped by HD status & BMI category |
| `vw_risk_factor_summary` | HD rate among exposed vs. unexposed for each risk factor |
| `vw_gen_health_vs_hd` | General health perception vs. actual HD rate |
| `vw_risk_tier_summary` | Avg BMI, sleep, health days by risk tier |
| `vw_comorbidity_impact` | HD rate by number of comorbidities |

## How to Use with Tableau
1. Clone or download this repository
2. Run `python3 db_setup.py` to build the database and exports
3. **Option A — Connect to SQLite directly:**
   - In Tableau → Connect → To a Server → Other Databases (ODBC) → select `heart_disease.db`
   - Use the `heart_disease_tableau` table or any SQL view
4. **Option B — Use exported CSVs:**
   - In Tableau → Connect → Text File → open files from `tableau_exports/`
5. Build visualizations to explore heart disease risk factors

## Key Analysis Areas
- Heart disease prevalence by age group and gender
- Impact of BMI, smoking, and alcohol on heart disease
- Correlation between physical/mental health days and heart disease
- Role of comorbidities (diabetes, stroke, kidney disease) in heart disease risk
- General health perception vs. actual heart disease status
