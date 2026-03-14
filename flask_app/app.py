"""
Flask Web Application for Heart Disease Dashboard
Author: Arpit Pandey
Task: Dashboard and Story embed with UI With Flask
"""

from flask import Flask, render_template, jsonify
import os

app = Flask(__name__)
app.config['SECRET_KEY'] = 'dev-secret-key-heart-disease-2024'

# Tableau Server Configuration
TABLEAU_SERVER_URL = 'https://public.tableau.com'
TABLEAU_SITE_ID = 'heart-disease-analysis'

@app.route('/')
def index():
    """Home page with dashboard overview"""
    return render_template('index.html')

@app.route('/dashboard')
def dashboard():
    """Main dashboard page with embedded Tableau"""
    tableau_config = {
        'server_url': TABLEAU_SERVER_URL,
        'site_id': TABLEAU_SITE_ID,
        'dashboard_name': 'HeartDiseaseAnalysis'
    }
    return render_template('dashboard.html', config=tableau_config)

@app.route('/story')
def story():
    """Story page with embedded Tableau Story"""
    tableau_config = {
        'server_url': TABLEAU_SERVER_URL,
        'site_id': TABLEAU_SITE_ID,
        'story_name': 'HeartDiseaseStory'
    }
    return render_template('story.html', config=tableau_config)

@app.route('/api/stats')
def get_stats():
    """API endpoint for dashboard statistics"""
    # This would typically query the database
    stats = {
        'total_patients': 303,
        'disease_prevalence': 54.5,
        'avg_age': 54.4,
        'high_risk_patients': 87
    }
    return jsonify(stats)

@app.route('/about')
def about():
    """About page with project information"""
    return render_template('about.html')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
