-- Create database for Heart Disease Analysis
-- Author: Cyrus Prakash Tiwari
-- Task: Storing Data in DB & Perform SQL Operations

CREATE DATABASE IF NOT EXISTS heart_disease_db;
USE heart_disease_db;

-- Create patients table
CREATE TABLE IF NOT EXISTS patients (
    patient_id INT PRIMARY KEY AUTO_INCREMENT,
    age INT NOT NULL,
    gender VARCHAR(10) NOT NULL,
    chest_pain_type INT,
    resting_bp INT,
    cholesterol INT,
    fasting_blood_sugar BOOLEAN,
    resting_ecg INT,
    max_heart_rate INT,
    exercise_induced_angina BOOLEAN,
    oldpeak DECIMAL(3,1),
    slope INT,
    num_major_vessels INT,
    thal INT,
    target INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for better query performance
CREATE INDEX idx_age ON patients(age);
CREATE INDEX idx_gender ON patients(gender);
CREATE INDEX idx_target ON patients(target);
CREATE INDEX idx_cholesterol ON patients(cholesterol);
