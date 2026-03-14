"""
Setup Database and Load Data
Author: Cyrus Prakash Tiwari & Arpit Pandey
Task: Storing Data in DB & Connect DB with Tableau
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.host = os.getenv('DB_HOST', 'localhost')
        self.user = os.getenv('DB_USER', 'root')
        self.password = os.getenv('DB_PASSWORD', '')
        self.database = os.getenv('DB_NAME', 'heart_disease_db')
        self.connection = None
    
    def connect(self):
        """Create database connection"""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            print("✓ Connected to MySQL server")
            return True
        except Error as e:
            print(f"✗ Error connecting to MySQL: {e}")
            return False
    
    def create_database(self):
        """Create database if not exists"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            cursor.execute(f"USE {self.database}")
            print(f"✓ Database '{self.database}' created/selected")
            cursor.close()
            return True
        except Error as e:
            print(f"✗ Error creating database: {e}")
            return False
    
    def create_tables(self):
        """Create tables from SQL file"""
        try:
            cursor = self.connection.cursor()
            
            # Read and execute SQL file
            with open('../sql/01_create_database.sql', 'r') as file:
                sql_commands = file.read().split(';')
                for command in sql_commands:
                    if command.strip():
                        cursor.execute(command)
            
            self.connection.commit()
            print("✓ Tables created successfully")
            cursor.close()
            return True
        except Error as e:
            print(f"✗ Error creating tables: {e}")
            return False
    
    def load_data(self, csv_path):
        """Load data from CSV to database"""
        try:
            # Read CSV
            df = pd.read_csv(csv_path)
            
            # Clean data
            df = df.dropna()
            
            # Convert target to binary (0 or 1)
            df['target'] = (df['target'] > 0).astype(int)
            
            # Convert gender (1=male, 0=female)
            df['gender'] = df['gender'].map({1: 'Male', 0: 'Female'})
            
            cursor = self.connection.cursor()
            cursor.execute(f"USE {self.database}")
            
            # Insert data
            insert_query = """
            INSERT INTO patients (
                age, gender, chest_pain_type, resting_bp, cholesterol,
                fasting_blood_sugar, resting_ecg, max_heart_rate,
                exercise_induced_angina, oldpeak, slope, num_major_vessels,
                thal, target
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            records_inserted = 0
            for _, row in df.iterrows():
                try:
                    cursor.execute(insert_query, tuple(row))
                    records_inserted += 1
                except Error as e:
                    print(f"Warning: Skipping row due to error: {e}")
            
            self.connection.commit()
            print(f"✓ Loaded {records_inserted} records into database")
            cursor.close()
            return True
            
        except Exception as e:
            print(f"✗ Error loading data: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("✓ Database connection closed")

def main():
    """Main setup function"""
    print("=== Heart Disease Database Setup ===\n")
    
    db = DatabaseManager()
    
    # Connect to MySQL
    if not db.connect():
        return
    
    # Create database
    if not db.create_database():
        db.close()
        return
    
    # Create tables
    if not db.create_tables():
        db.close()
        return
    
    # Load data - try multiple file names
    csv_paths = ['../data/Heart_new2.csv', '../data/heart_disease_raw.csv', 'data/Heart_new2.csv', 'Heart_new2.csv']
    
    loaded = False
    for csv_path in csv_paths:
        if os.path.exists(csv_path):
            print(f"Found data file: {csv_path}")
            db.load_data(csv_path)
            loaded = True
            break
    
    if not loaded:
        print(f"✗ Data file not found. Tried: {csv_paths}")
        print("Please ensure Heart_new2.csv is in the data/ directory")
    
    # Close connection
    db.close()
    
    print("\n=== Setup Complete ===")

if __name__ == "__main__":
    main()
