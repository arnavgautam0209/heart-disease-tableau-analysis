"""
Download Heart Disease Dataset
Author: Atul Singh
Task: Downloading the dataset
"""

import pandas as pd
import requests
import os

def download_heart_disease_data():
    """
    Download heart disease dataset from UCI ML Repository
    """
    print("Downloading heart disease dataset...")
    
    # UCI Heart Disease Dataset URL
    url = "https://archive.ics.uci.edu/ml/machine-learning-databases/heart-disease/processed.cleveland.data"
    
    # Column names for the dataset
    column_names = [
        'age', 'gender', 'chest_pain_type', 'resting_bp', 'cholesterol',
        'fasting_blood_sugar', 'resting_ecg', 'max_heart_rate',
        'exercise_induced_angina', 'oldpeak', 'slope', 'num_major_vessels',
        'thal', 'target'
    ]
    
    try:
        # Download the data
        df = pd.read_csv(url, names=column_names, na_values='?')
        
        # Create data directory if it doesn't exist
        os.makedirs('../data', exist_ok=True)
        
        # Save to CSV
        output_path = '../data/heart_disease_raw.csv'
        df.to_csv(output_path, index=False)
        
        print(f"✓ Dataset downloaded successfully!")
        print(f"✓ Saved to: {output_path}")
        print(f"✓ Total records: {len(df)}")
        print(f"✓ Total columns: {len(df.columns)}")
        
        # Display basic info
        print("\nDataset Info:")
        print(df.info())
        print("\nFirst few rows:")
        print(df.head())
        
        return df
        
    except Exception as e:
        print(f"✗ Error downloading dataset: {e}")
        return None

if __name__ == "__main__":
    download_heart_disease_data()
