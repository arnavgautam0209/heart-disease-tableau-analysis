"""
Prepare Data for Tableau Visualization
Author: Atul Raj Gautam
Task: Prepare the Data for Visualization
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

load_dotenv()

def connect_to_database():
    """Connect to MySQL database"""
    try:
        connection = mysql.connector.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            user=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', ''),
            database=os.getenv('DB_NAME', 'heart_disease_db')
        )
        print("✓ Connected to database")
        return connection
    except Error as e:
        print(f"✗ Error connecting to database: {e}")
        print("Note: Make sure to run setup_database.py first!")
        return None

def prepare_visualization_data(connection):
    """Prepare and export data for Tableau"""
    
    # Query 1: Patient Demographics
    demographics_query = """
    SELECT 
        patient_id,
        age,
        gender,
        CASE 
            WHEN age < 40 THEN 'Under 40'
            WHEN age BETWEEN 40 AND 50 THEN '40-50'
            WHEN age BETWEEN 51 AND 60 THEN '51-60'
            ELSE 'Over 60'
        END as age_group,
        target as has_disease
    FROM patients
    """
    
    # Query 2: Risk Factors
    risk_factors_query = """
    SELECT 
        patient_id,
        age,
        gender,
        cholesterol,
        resting_bp,
        max_heart_rate,
        fasting_blood_sugar,
        exercise_induced_angina,
        CASE 
            WHEN chest_pain_type = 0 THEN 'Typical Angina'
            WHEN chest_pain_type = 1 THEN 'Atypical Angina'
            WHEN chest_pain_type = 2 THEN 'Non-anginal Pain'
            ELSE 'Asymptomatic'
        END as chest_pain_category,
        target as has_disease
    FROM patients
    """
    
    # Query 3: Disease Statistics by Demographics
    stats_query = """
    SELECT 
        gender,
        FLOOR(age/10)*10 as age_decade,
        COUNT(*) as total_patients,
        SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) as disease_count,
        ROUND(AVG(cholesterol), 2) as avg_cholesterol,
        ROUND(AVG(resting_bp), 2) as avg_bp,
        ROUND(AVG(max_heart_rate), 2) as avg_heart_rate
    FROM patients
    GROUP BY gender, age_decade
    ORDER BY age_decade, gender
    """
    
    try:
        # Export demographics data
        df_demographics = pd.read_sql(demographics_query, connection)
        df_demographics.to_csv('../data/tableau_demographics.csv', index=False)
        print(f"✓ Exported demographics data: {len(df_demographics)} records")
        
        # Export risk factors data
        df_risk = pd.read_sql(risk_factors_query, connection)
        df_risk.to_csv('../data/tableau_risk_factors.csv', index=False)
        print(f"✓ Exported risk factors data: {len(df_risk)} records")
        
        # Export statistics data
        df_stats = pd.read_sql(stats_query, connection)
        df_stats.to_csv('../data/tableau_statistics.csv', index=False)
        print(f"✓ Exported statistics data: {len(df_stats)} records")
        
        # Create a master dataset for Tableau
        master_query = """
        SELECT 
            patient_id,
            age,
            gender,
            chest_pain_type,
            resting_bp,
            cholesterol,
            fasting_blood_sugar,
            resting_ecg,
            max_heart_rate,
            exercise_induced_angina,
            oldpeak,
            slope,
            num_major_vessels,
            thal,
            target as has_disease,
            CASE 
                WHEN age < 40 THEN 'Under 40'
                WHEN age BETWEEN 40 AND 50 THEN '40-50'
                WHEN age BETWEEN 51 AND 60 THEN '51-60'
                ELSE 'Over 60'
            END as age_group,
            CASE 
                WHEN cholesterol < 200 THEN 'Normal'
                WHEN cholesterol BETWEEN 200 AND 239 THEN 'Borderline High'
                ELSE 'High'
            END as cholesterol_category,
            CASE 
                WHEN resting_bp < 120 THEN 'Normal'
                WHEN resting_bp BETWEEN 120 AND 139 THEN 'Prehypertension'
                ELSE 'Hypertension'
            END as bp_category
        FROM patients
        """
        
        df_master = pd.read_sql(master_query, connection)
        df_master.to_csv('../data/tableau_master_data.csv', index=False)
        print(f"✓ Exported master dataset: {len(df_master)} records")
        
        print("\n=== Data Preparation Complete ===")
        print("Files created in data/ directory:")
        print("  - tableau_demographics.csv")
        print("  - tableau_risk_factors.csv")
        print("  - tableau_statistics.csv")
        print("  - tableau_master_data.csv")
        
    except Exception as e:
        print(f"✗ Error preparing data: {e}")

def main():
    """Main function"""
    print("=== Preparing Data for Tableau ===\n")
    
    connection = connect_to_database()
    if connection:
        prepare_visualization_data(connection)
        connection.close()
        print("\n✓ Database connection closed")

if __name__ == "__main__":
    main()
