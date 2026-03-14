# Heart Disease Analysis - Complete Project Documentation

## Project Overview

### Problem Statement
Heart disease remains one of the leading causes of mortality worldwide. Analyzing large-scale health data related to heart disease requires advanced tools for extracting meaningful insights.

### Solution
This project uses **Tableau** as a powerful data visualization and business intelligence tool to:
- Transform raw heart disease data into meaningful dashboards
- Highlight key risk factors
- Identify correlations supporting better decision-making
- Enable interactive visualizations for healthcare providers and policymakers

---

## Technical Architecture

```
┌─────────────────┐
│  Data Source    │
│  (UCI ML Repo)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  MySQL Database │
│  (Storage)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Tableau        │
│  (Visualization)│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Tableau Server │
│  (Publishing)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Flask Web App  │
│  (UI/Embedding) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   End Users     │
└─────────────────┘
```

---

## Project Scenarios

### Scenario 1: Clinical Analysis
**User**: Dr. Sharma, Senior Cardiologist at Metropolitan Hospital

**Use Case**: Analyze patient data segmented by age, gender, BMI, cholesterol levels, and smoking habits to identify high-risk groups and design targeted awareness campaigns.

**Features Used**:
- Age and gender filters
- Cholesterol level analysis
- Risk factor correlation
- Patient segmentation

### Scenario 2: Policy Making
**User**: Ramesh, Government Health Department Analyst

**Use Case**: Study trends in heart disease prevalence across different regions, comparing rural and urban populations to develop preventive health policies.

**Features Used**:
- Regional comparison dashboards
- Trend analysis over time
- Population-based statistics
- Policy recommendation insights

### Scenario 3: Personal Health Monitoring
**User**: Anita, 45-year-old Professional

**Use Case**: Monitor personal health risks using simplified dashboards to compare risk factors against healthy benchmarks and make informed lifestyle decisions.

**Features Used**:
- Personal health dashboard
- Risk score calculator
- Lifestyle recommendations
- Progress tracking

---

## Dataset Information

### Source
UCI Machine Learning Repository - Heart Disease Dataset

### Attributes (14 features)

1. **age**: Age in years
2. **gender**: Sex (1 = male; 0 = female)
3. **chest_pain_type**: Chest pain type (0-3)
   - 0: Typical angina
   - 1: Atypical angina
   - 2: Non-anginal pain
   - 3: Asymptomatic
4. **resting_bp**: Resting blood pressure (mm Hg)
5. **cholesterol**: Serum cholesterol (mg/dl)
6. **fasting_blood_sugar**: Fasting blood sugar > 120 mg/dl (1 = true; 0 = false)
7. **resting_ecg**: Resting electrocardiographic results (0-2)
8. **max_heart_rate**: Maximum heart rate achieved
9. **exercise_induced_angina**: Exercise induced angina (1 = yes; 0 = no)
10. **oldpeak**: ST depression induced by exercise
11. **slope**: Slope of peak exercise ST segment
12. **num_major_vessels**: Number of major vessels (0-3)
13. **thal**: Thalassemia (3 = normal; 6 = fixed defect; 7 = reversible defect)
14. **target**: Heart disease diagnosis (0 = no disease; 1 = disease)

### Dataset Statistics
- Total Records: 303
- Features: 14
- Target Classes: 2 (Disease/No Disease)
- Missing Values: Minimal (handled during preprocessing)

---

## Implementation Details

### Phase 1: Data Collection & Storage

**Responsible**: Atul Singh, Cyrus Prakash Tiwari

**Tasks**:
1. Download dataset from UCI repository
2. Create MySQL database schema
3. Load data into database
4. Perform SQL operations for data validation

**Files**:
- `scripts/download_dataset.py`
- `sql/01_create_database.sql`
- `sql/02_analysis_queries.sql`
- `scripts/setup_database.py`

### Phase 2: Database Integration

**Responsible**: Arpit Pandey

**Tasks**:
1. Connect Tableau to MySQL database
2. Verify data integrity
3. Optimize database queries
4. Ensure adequate data volume

**Files**:
- Database connection configurations
- `.env` file with credentials

### Phase 3: Data Preparation

**Responsible**: Atul Raj Gautam

**Tasks**:
1. Clean and transform data
2. Create calculated fields
3. Prepare data extracts for Tableau
4. Generate aggregated views

**Files**:
- `scripts/prepare_data_for_tableau.py`
- `data/tableau_*.csv` files

### Phase 4: Visualization Creation

**Responsible**: Atul Raj Gautam, Cyrus Prakash Tiwari

**Tasks**:
1. Create unique visualizations (minimum 6)
2. Implement calculation fields (minimum 5)
3. Design responsive dashboard
4. Create story with multiple scenes

**Deliverables**:
- Bar charts for age/gender distribution
- Heat maps for risk correlation
- Scatter plots for BP vs heart rate
- Pie charts for disease prevalence
- Line charts for trend analysis
- Histograms for cholesterol distribution

### Phase 5: Dashboard Design

**Responsible**: Atul Raj Gautam

**Tasks**:
1. Design responsive dashboard layout
2. Implement data filters
3. Create story scenes (minimum 4)
4. Add interactivity and actions

**Features**:
- Age range filter
- Gender filter
- Cholesterol filter
- Blood pressure filter
- Disease status filter

### Phase 6: Performance Testing

**Responsible**: Arpit Pandey, Cyrus Prakash Tiwari

**Tasks**:
1. Test data filter performance
2. Optimize calculation fields
3. Measure dashboard load times
4. Test with different data volumes

**Metrics**:
- Dashboard load time < 3 seconds
- Filter response time < 1 second
- Supports 1000+ records efficiently

### Phase 7: Publishing

**Responsible**: Arpit Pandey

**Tasks**:
1. Publish workbook to Tableau Server
2. Configure access permissions
3. Set up data refresh schedules
4. Generate embed codes

**Deliverables**:
- Published dashboard URL
- Embed codes for web integration
- Access credentials for team

### Phase 8: Web Integration

**Responsible**: Arpit Pandey

**Tasks**:
1. Create Flask web application
2. Embed Tableau dashboards
3. Embed Tableau stories
4. Design responsive UI

**Files**:
- `flask_app/app.py`
- `flask_app/templates/*.html`
- `flask_app/static/css/style.css`

### Phase 9: Documentation

**Responsible**: Atul Raj Gautam

**Tasks**:
1. Write setup guide
2. Document Tableau procedures
3. Create user manual
4. Prepare presentation

**Files**:
- `docs/SETUP_GUIDE.md`
- `docs/TABLEAU_GUIDE.md`
- `docs/PROJECT_DOCUMENTATION.md`

### Phase 10: Demo & Presentation

**Responsible**: Atul Singh

**Tasks**:
1. Record video demonstration
2. Prepare presentation slides
3. Document end-to-end solution
4. Create GitHub repository

---

## Key Features

### Dashboard Features
- ✅ 6+ unique visualizations
- ✅ 5+ calculation fields
- ✅ Multiple data filters
- ✅ Responsive design
- ✅ Interactive elements
- ✅ Real-time updates

### Story Features
- ✅ 4+ story scenes
- ✅ Narrative flow
- ✅ Annotations
- ✅ Navigation controls

### Web Application Features
- ✅ Embedded dashboards
- ✅ Embedded stories
- ✅ Responsive design
- ✅ Multiple pages
- ✅ API endpoints
- ✅ Statistics display

---

## Technologies Used

### Data Layer
- **MySQL 8.0**: Database management
- **Python 3.8+**: Data processing
- **Pandas**: Data manipulation
- **NumPy**: Numerical operations

### Visualization Layer
- **Tableau Desktop**: Dashboard creation
- **Tableau Server**: Publishing and sharing
- **Tableau JavaScript API**: Web embedding

### Web Layer
- **Flask 3.0**: Web framework
- **Bootstrap 5**: UI framework
- **HTML5/CSS3**: Frontend
- **JavaScript**: Interactivity

### Development Tools
- **Git**: Version control
- **VS Code**: Code editor
- **MySQL Workbench**: Database management

---

## Installation & Setup

See `docs/SETUP_GUIDE.md` for detailed installation instructions.

Quick Start:
```bash
# Clone repository
git clone <repo-url>
cd heart-disease-analysis

# Install dependencies
pip install -r requirements.txt

# Setup database
python scripts/setup_database.py

# Run Flask app
cd flask_app
python app.py
```

---

## Testing

### Database Testing
```sql
-- Verify data load
SELECT COUNT(*) FROM patients;

-- Check data quality
SELECT * FROM patients LIMIT 10;

-- Run analysis queries
SOURCE sql/02_analysis_queries.sql;
```

### Tableau Testing
- Verify all visualizations render correctly
- Test all filters
- Check dashboard responsiveness
- Validate calculations

### Web Application Testing
- Test all routes
- Verify dashboard embedding
- Check mobile responsiveness
- Test API endpoints

---

## Performance Metrics

### Database
- Query response time: < 100ms
- Data load time: < 5 seconds
- Concurrent connections: 10+

### Tableau
- Dashboard load time: < 3 seconds
- Filter response: < 1 second
- Visualization render: < 2 seconds

### Web Application
- Page load time: < 2 seconds
- API response: < 500ms
- Mobile performance: Optimized

---

## Security Considerations

1. **Database Security**
   - Use environment variables for credentials
   - Implement user authentication
   - Regular backups

2. **Tableau Security**
   - Role-based access control
   - Secure embed codes
   - HTTPS for published dashboards

3. **Web Application Security**
   - Secret key management
   - Input validation
   - CSRF protection

---

## Future Enhancements

1. **Machine Learning Integration**
   - Predictive models for disease risk
   - Patient outcome predictions
   - Automated risk scoring

2. **Real-time Data**
   - Live data streaming
   - Real-time dashboard updates
   - Alert notifications

3. **Mobile Application**
   - Native mobile app
   - Offline access
   - Push notifications

4. **Advanced Analytics**
   - Cohort analysis
   - Survival analysis
   - Treatment effectiveness

---

## Team Contributions

| Team Member | Tasks | Contribution |
|------------|-------|--------------|
| Atul Singh | Data Collection, Video Demo | 20% |
| Cyrus Prakash Tiwari | Database, SQL, Visualizations | 25% |
| Arpit Pandey | DB Connection, Publishing, Flask | 30% |
| Atul Raj Gautam | Data Prep, Dashboard Design, Docs | 25% |

---

## References

1. UCI Machine Learning Repository - Heart Disease Dataset
2. Tableau Documentation
3. Flask Documentation
4. MySQL Documentation
5. Bootstrap Documentation

---

## License

MIT License - See LICENSE file for details

---

## Contact

For questions or support, contact the project team through GitHub issues.

---

**Last Updated**: March 2024
**Version**: 1.0
**Status**: Complete
