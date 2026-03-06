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
- **Tableau** - Data visualization and dashboard creation
- **GitHub** - Version control and project hosting

## Project Structure
```
├── Heart_new2.csv          # Clean dataset for Tableau analysis
├── README.md               # Project documentation
```

## How to Use
1. Clone or download this repository
2. Open Tableau Desktop or Tableau Public
3. Connect to `Heart_new2.csv` as a text file data source
4. Build visualizations to explore heart disease risk factors

## Key Analysis Areas
- Heart disease prevalence by age group and gender
- Impact of BMI, smoking, and alcohol on heart disease
- Correlation between physical/mental health days and heart disease
- Role of comorbidities (diabetes, stroke, kidney disease) in heart disease risk
- General health perception vs. actual heart disease status
