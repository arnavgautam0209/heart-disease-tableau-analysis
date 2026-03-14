# Tableau Workbooks

## Heart Disease Analysis Dashboard

### Visualizations Included:
1. **Age Distribution** - Bar chart showing disease prevalence by age group
2. **Cholesterol Analysis** - Histogram of cholesterol levels
3. **Risk Factor Heat Map** - Correlation between age and cholesterol
4. **Gender Analysis** - Pie chart of disease by gender
5. **BP vs Heart Rate** - Scatter plot showing relationship
6. **Chest Pain Analysis** - Stacked bar chart

### Calculation Fields:
1. Disease Rate: `SUM([target]) / COUNT([patient_id])`
2. Risk Score: Categorizes patients as High/Medium/Low risk
3. Age Category: Young/Middle-aged/Senior
4. Cholesterol Status: Normal/Borderline/High
5. BP Category: Normal/Prehypertension/Hypertension

### Filters Applied:
- Age Range (20-80)
- Gender (Male/Female)
- Cholesterol Range (100-400)
- Blood Pressure Range (80-200)
- Disease Status (Yes/No)

### Dashboard Features:
- Responsive design (works on desktop, tablet, mobile)
- Interactive filters
- Hover tooltips
- Click-to-filter actions

### Story Scenes:
1. **Overview** - Disease statistics and prevalence
2. **Risk Factors** - Cholesterol and BP analysis
3. **Demographics** - Age and gender patterns
4. **Clinical Insights** - Medical indicators
5. **Recommendations** - Key findings and actions

## Files:
- `HeartDiseaseAnalysis.twbx` - Main Tableau workbook (to be created)
- Connected to MySQL database: heart_disease_db

## Publishing:
Dashboard published to Tableau Public/Server for web access and Flask integration.
