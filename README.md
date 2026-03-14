# Heart Disease Analysis - Tableau Project

## Project Overview
Heart disease remains one of the leading causes of mortality worldwide. This project uses **Tableau** as a powerful data visualization and business intelligence tool to analyze heart disease data, transform raw data into meaningful dashboards, and identify correlations that support better decision-making for healthcare providers and policymakers.

This project applies descriptive and diagnostic analytics techniques using Tableau to examine large-scale heart disease data. Through segmentation, correlation analysis, and interactive visualization, the dashboards reveal patterns between lifestyle factors, demographic variables, and clinical indicators contributing to cardiovascular risk.

## Technical Architecture
```
Google Drive/Database → Tableau → Dashboard → Tableau Server → Flask UI → End User
```

## Team Members & Tasks

### Data Collection & Storage
- **Atul Singh**: Downloading the dataset
- **Cyrus Prakash Tiwari**: Storing Data in DB & Perform SQL Operations medium

### Database Integration
- **Arpit Pandey**: Connect DB with Tableau, Amount of Data to DB, Publishing

### Data Preparation & Visualization
- **Atul Raj Gautam**: Prepare Data for Visualization, Responsive Design of Dashboard, No of Scenes of Story
- **Cyrus Prakash Tiwari**: No of Unique Visualizations, No of Calculation Fields

### Performance & Web Integration
- **Arpit Pandey**: Utilization of Data Filters, No of Visualizations/Graphs
- **Arpit Pandey**: Dashboard and Story embed with UI With Flask

### Documentation
- **Atul Raj Gautam**: Project Documentation-Step by step project development procedure
- **Atul Singh**: Record explanation Video for the project's end-to-end solution

## Project Scenarios

### Scenario 1: Senior Cardiologist Analysis
Dr. Sharma analyzes patient data segmented by age, gender, BMI, cholesterol levels, and smoking habits to identify high-risk groups and design targeted awareness campaigns.

### Scenario 2: Government Health Department
Ramesh studies trends in heart disease prevalence across different regions, comparing rural and urban populations to develop preventive health policies.

### Scenario 3: Personal Health Monitoring
Anita, a 45-year-old professional, monitors her health risks using simplified Tableau dashboards to make informed lifestyle decisions.

## Skills Required
- Data Analysis
- Data Visualization
- Flask (Web Framework)
- Dashboard Design
- Tableau (Business Intelligence Software)

## Project Status
✅ Database schema and SQL queries complete
✅ Python scripts for data processing complete
✅ Flask web application complete
✅ Data preparation and analysis complete
✅ Tableau dashboard design documented
✅ All documentation complete

## Setup Instructions

### Prerequisites
- Python 3.8+
- MySQL/PostgreSQL Database
- Tableau Desktop
- Tableau Server (for publishing)

### Installation
```bash
# Clone the repository
git clone https://github.com/arpitpandey0307/Heart-disease-analysis.git
cd Heart-disease-analysis

# Install Python dependencies
pip install -r requirements.txt

# Setup database
python scripts/setup_database.py
```

## Project Structure
```
heart-disease-analysis/
├── data/                      # Dataset files
├── sql/                       # SQL scripts
├── scripts/                   # Python scripts for data processing
├── tableau/                   # Tableau workbooks
├── flask_app/                 # Flask web application
├── docs/                      # Documentation
└── README.md
```

## License
MIT License
