# Heart Disease Analysis - Setup Guide

## Prerequisites

Before starting, ensure you have the following installed:

- Python 3.8 or higher
- MySQL Server 8.0 or higher
- Tableau Desktop (for creating visualizations)
- Tableau Server or Tableau Public (for publishing)
- Git

## Step-by-Step Setup

### 1. Clone the Repository

```bash
git clone <your-repository-url>
cd heart-disease-analysis
```

### 2. Set Up Python Environment

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configure Environment Variables

```bash
# Copy the example environment file
copy .env.example .env

# Edit .env file with your database credentials
# Update the following:
# - DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
# - TABLEAU_SERVER_URL, TABLEAU_USERNAME, TABLEAU_PASSWORD
```

### 4. Download Dataset

```bash
cd scripts
python download_dataset.py
```

This will download the heart disease dataset from UCI ML Repository and save it to `data/heart_disease_raw.csv`.

### 5. Set Up Database

```bash
# Make sure MySQL server is running
# Then run the setup script
python setup_database.py
```

This script will:
- Create the database
- Create necessary tables
- Load data from CSV into the database

### 6. Prepare Data for Tableau

```bash
python prepare_data_for_tableau.py
```

This will create several CSV files optimized for Tableau visualization in the `data/` directory.

### 7. Create Tableau Visualizations

1. Open Tableau Desktop
2. Connect to MySQL database using credentials from .env file
3. Or import the prepared CSV files from `data/` directory
4. Create dashboards following the project requirements:
   - Multiple unique visualizations
   - Calculation fields
   - Data filters
   - Responsive design

### 8. Publish to Tableau Server

1. In Tableau Desktop, go to Server → Publish Workbook
2. Enter your Tableau Server credentials
3. Choose the project and set permissions
4. Note the published dashboard URL

### 9. Configure Flask Application

Update the `.env` file with your Tableau Server details:

```
TABLEAU_SERVER_URL=https://your-tableau-server.com
TABLEAU_SITE_ID=your_site_id
```

### 10. Run Flask Application

```bash
cd flask_app
python app.py
```

The application will be available at `http://localhost:5000`

## Verification

1. Check database: Run SQL queries from `sql/02_analysis_queries.sql`
2. Verify Tableau connection: Open Tableau and connect to the database
3. Test Flask app: Navigate to all pages (Home, Dashboard, Story, About)

## Troubleshooting

### Database Connection Issues
- Verify MySQL is running: `mysql --version`
- Check credentials in .env file
- Ensure database user has proper permissions

### Tableau Connection Issues
- Verify Tableau Server URL is correct
- Check authentication credentials
- Ensure workbook is published and accessible

### Flask Application Issues
- Check if port 5000 is available
- Verify all dependencies are installed
- Check Flask logs for error messages

## Next Steps

1. Customize Tableau dashboards based on requirements
2. Add more visualizations and calculations
3. Implement additional filters
4. Create story points for narrative
5. Update Flask templates with actual Tableau embed codes

## Team Member Tasks

Refer to the main README.md for specific task assignments for each team member.
