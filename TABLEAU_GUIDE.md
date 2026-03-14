# Tableau Dashboard Creation Guide

## Author: Atul Raj Gautam & Arpit Pandey

## Overview

This guide covers creating comprehensive Tableau dashboards for heart disease analysis.

## Data Connection

### Option 1: Connect to MySQL Database

1. Open Tableau Desktop
2. Click "Connect" → "MySQL"
3. Enter connection details:
   - Server: localhost (or your DB host)
   - Port: 3306
   - Database: heart_disease_db
   - Username: your_username
   - Password: your_password
4. Click "Sign In"

### Option 2: Use Prepared CSV Files

1. Open Tableau Desktop
2. Click "Connect" → "Text file"
3. Navigate to `data/` folder
4. Select `tableau_master_data.csv`
5. For multiple data sources, add other CSV files

## Required Visualizations

### 1. Disease Prevalence by Age Group
- **Type**: Bar Chart
- **Rows**: Age Group
- **Columns**: Count of Patients
- **Color**: Has Disease (Yes/No)
- **Filter**: Gender

### 2. Cholesterol Distribution
- **Type**: Histogram
- **Rows**: Cholesterol
- **Columns**: Count
- **Color**: Cholesterol Category
- **Calculation**: Bin cholesterol into ranges

### 3. Risk Factors Correlation
- **Type**: Heat Map
- **Rows**: Age Group
- **Columns**: Cholesterol Category
- **Color**: Disease Rate
- **Size**: Patient Count

### 4. Gender-based Analysis
- **Type**: Pie Chart / Donut Chart
- **Angle**: Count of Patients
- **Color**: Gender
- **Label**: Percentage

### 5. Blood Pressure vs Heart Rate
- **Type**: Scatter Plot
- **Rows**: Max Heart Rate
- **Columns**: Resting BP
- **Color**: Has Disease
- **Size**: Age

### 6. Trend Analysis
- **Type**: Line Chart
- **Rows**: Disease Count
- **Columns**: Age
- **Color**: Gender
- **Trend Line**: Add trend line

## Calculation Fields

### 1. Disease Rate
```
SUM([Has Disease]) / COUNT([Patient ID])
```

### 2. Risk Score
```
IF [Cholesterol] > 240 AND [Resting BP] > 140 THEN "High Risk"
ELSEIF [Cholesterol] > 200 OR [Resting BP] > 120 THEN "Medium Risk"
ELSE "Low Risk"
END
```

### 3. Age Category
```
IF [Age] < 40 THEN "Young"
ELSEIF [Age] <= 60 THEN "Middle-aged"
ELSE "Senior"
END
```

### 4. BMI Category (if BMI data available)
```
IF [BMI] < 18.5 THEN "Underweight"
ELSEIF [BMI] < 25 THEN "Normal"
ELSEIF [BMI] < 30 THEN "Overweight"
ELSE "Obese"
END
```

### 5. Cholesterol Status
```
IF [Cholesterol] < 200 THEN "Normal"
ELSEIF [Cholesterol] < 240 THEN "Borderline High"
ELSE "High"
END
```

## Data Filters

### Essential Filters to Add:

1. **Age Range Slider**
   - Type: Range
   - Show: Slider
   - Apply to: All worksheets

2. **Gender Filter**
   - Type: Single Value (dropdown)
   - Include "All" option
   - Apply to: All worksheets

3. **Cholesterol Range**
   - Type: Range
   - Show: Slider
   - Apply to: Relevant worksheets

4. **Disease Status**
   - Type: Single Value (list)
   - Options: All, With Disease, Without Disease

5. **Risk Level**
   - Type: Multiple Values (list)
   - Based on calculated Risk Score field

## Dashboard Design

### Layout Structure

```
+----------------------------------+
|         Dashboard Title          |
+----------------------------------+
|  Filters (Age, Gender, etc.)     |
+----------------------------------+
|  KPI Cards (4 metrics)           |
+----------------------------------+
|  Main Viz 1  |  Main Viz 2       |
|              |                   |
+----------------------------------+
|  Main Viz 3  |  Main Viz 4       |
|              |                   |
+----------------------------------+
```

### Responsive Design Tips

1. Use "Automatic" sizing for dashboard
2. Set minimum size: 800x600
3. Test on different screen sizes
4. Use containers for grouping
5. Enable "Fit to" options appropriately

### Dashboard Actions

1. **Filter Action**: Click on age group to filter other charts
2. **Highlight Action**: Hover to highlight related data
3. **URL Action**: Link to external resources (optional)

## Story Creation

### Story Points (Minimum 4 scenes)

#### Scene 1: Overview
- Title: "Heart Disease: The Numbers"
- Content: Overall statistics dashboard
- Key metrics: Total patients, disease rate, demographics

#### Scene 2: Risk Factors
- Title: "Understanding Risk Factors"
- Content: Cholesterol, BP, and lifestyle factors
- Visualizations: Correlation charts, distributions

#### Scene 3: Demographics
- Title: "Age and Gender Patterns"
- Content: How disease affects different groups
- Visualizations: Age/gender breakdowns

#### Scene 4: Insights & Recommendations
- Title: "Key Findings"
- Content: Summary of insights
- Text boxes with recommendations

### Story Navigation

- Add captions to each story point
- Use consistent formatting
- Include navigation buttons
- Add annotations for key insights

## Performance Optimization

1. **Data Extracts**: Use extracts instead of live connections for better performance
2. **Aggregation**: Pre-aggregate data where possible
3. **Filters**: Use context filters for large datasets
4. **Calculations**: Minimize complex calculations in views

## Publishing to Tableau Server

### Steps:

1. **Prepare Workbook**
   - Save workbook with meaningful name
   - Test all dashboards and stories
   - Verify filters work correctly

2. **Publish**
   - Server → Publish Workbook
   - Select project location
   - Choose authentication method
   - Set permissions

3. **Configure**
   - Set refresh schedule (if using extracts)
   - Configure user access
   - Test published version

4. **Embed Code**
   - Get embed code from Tableau Server
   - Update Flask templates with embed code
   - Test in web application

## Checklist

- [ ] At least 6 unique visualizations
- [ ] Minimum 5 calculation fields
- [ ] Data filters implemented
- [ ] Responsive dashboard design
- [ ] Story with 4+ scenes
- [ ] Published to Tableau Server
- [ ] Embedded in Flask application
- [ ] Tested on multiple devices

## Tips for Success

1. Keep visualizations simple and clear
2. Use color consistently (e.g., red for disease, green for healthy)
3. Add tooltips with additional information
4. Include legends and labels
5. Test interactivity thoroughly
6. Document your calculations
7. Get feedback from team members

## Resources

- [Tableau Public Gallery](https://public.tableau.com/gallery)
- [Tableau Training Videos](https://www.tableau.com/learn/training)
- [Best Practices for Dashboard Design](https://help.tableau.com/current/pro/desktop/en-us/dashboards_best_practices.htm)
