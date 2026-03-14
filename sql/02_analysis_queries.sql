-- SQL Analysis Queries for Heart Disease Data
-- Author: Cyrus Prakash Tiwari

USE heart_disease_db;

-- Query 1: Count of patients by heart disease status
SELECT 
    target,
    COUNT(*) as patient_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM patients), 2) as percentage
FROM patients
GROUP BY target;

-- Query 2: Average cholesterol by age group and gender
SELECT 
    CASE 
        WHEN age < 40 THEN 'Under 40'
        WHEN age BETWEEN 40 AND 50 THEN '40-50'
        WHEN age BETWEEN 51 AND 60 THEN '51-60'
        ELSE 'Over 60'
    END as age_group,
    gender,
    AVG(cholesterol) as avg_cholesterol,
    COUNT(*) as patient_count
FROM patients
WHERE cholesterol > 0
GROUP BY age_group, gender
ORDER BY age_group, gender;

-- Query 3: High-risk patients (multiple risk factors)
SELECT 
    patient_id,
    age,
    gender,
    cholesterol,
    resting_bp,
    max_heart_rate,
    target
FROM patients
WHERE cholesterol > 240 
    AND resting_bp > 140
    AND age > 50
ORDER BY age DESC;

-- Query 4: Heart disease prevalence by chest pain type
SELECT 
    chest_pain_type,
    COUNT(*) as total_patients,
    SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) as with_disease,
    ROUND(SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as disease_rate
FROM patients
GROUP BY chest_pain_type
ORDER BY disease_rate DESC;

-- Query 5: Exercise-induced angina analysis
SELECT 
    exercise_induced_angina,
    gender,
    AVG(age) as avg_age,
    COUNT(*) as patient_count,
    SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) as disease_count
FROM patients
GROUP BY exercise_induced_angina, gender;

-- Query 6: Maximum heart rate analysis by age group
SELECT 
    FLOOR(age/10)*10 as age_decade,
    AVG(max_heart_rate) as avg_max_hr,
    MIN(max_heart_rate) as min_max_hr,
    MAX(max_heart_rate) as max_max_hr,
    COUNT(*) as patient_count
FROM patients
GROUP BY age_decade
ORDER BY age_decade;

-- Query 7: Risk factor correlation
SELECT 
    fasting_blood_sugar,
    exercise_induced_angina,
    COUNT(*) as patient_count,
    SUM(CASE WHEN target = 1 THEN 1 ELSE 0 END) as disease_count,
    ROUND(AVG(cholesterol), 2) as avg_cholesterol
FROM patients
GROUP BY fasting_blood_sugar, exercise_induced_angina;
