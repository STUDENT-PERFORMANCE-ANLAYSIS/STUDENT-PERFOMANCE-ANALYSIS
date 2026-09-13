# src/main.py
"""
Flask Backend application serving as the project coordinator and REST controller.

Why this file exists:
Apache Spark is a data processing library, not a web server. To expose our Spark 
analysis as an interactive web app, we build a Flask application. This script acts 
as the server. It handles file uploads, routes API requests from the frontend, coordinates 
data ingestion, cleaning, and analytics, and serializes Spark DataFrame outputs into 
JSON payloads that the browser can display.

Communication flow:
1. User uploads a CSV. Flask saves the file, invokes 'utils.py' to get/start a SparkSession, 
   'data_loader.py' to read/map headers, and 'data_cleaning.py' to clean it.
2. The cleaned DataFrame is cached globally in memory.
3. When the user clicks an analysis button, JavaScript fetches the specific '/analyze/<query>' route.
4. Flask executes the corresponding function in 'analytics.py', converts the output into Python 
   dictionaries, and returns it as a JSON API response.
5. If the user clicks export, Flask runs 'exporter.py' to save the report as a single CSV.

Viva Tips:
- How does Flask serve static assets? By setting `static_folder` pointing to the 'ui' directory, 
  Flask can serve files like `style.css` and `script.js` directly to the client.
- Why is the DataFrame cached in a global variable? So we don't reload and re-clean the CSV file 
  from disk for every click, which saves significant computing overhead.
- What does `.collect()` do? It is a Spark ACTION that pulls the distributed results from worker 
  nodes and returns them as a local list to the driver (Flask server memory).
"""

import os
import sys

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Set the current working directory to the project base folder.
# This prevents Spark from writing temporary files (like derby.log or spark-warehouse)
# in system directories like C:\Windows\System32 when launched via startup VBS scripts.
os.chdir(base_dir)

# Ensure stdout and stderr are valid streams (crucial when running via pythonw.exe without a console)
for stream_name in ('stdout', 'stderr'):
    stream = getattr(sys, stream_name, None)
    if stream is None:
        setattr(sys, stream_name, open(os.devnull, 'w'))
    else:
        try:
            stream.write('')
            stream.flush()
        except Exception:
            setattr(sys, stream_name, open(os.devnull, 'w'))

# Automatically locate and bind portable Java JDK if present in local candidate directories
candidates = [
    os.path.join(base_dir, ".jdk-11", "jdk-11.0.21+9"),
]
for portable_java in candidates:
    if os.path.exists(os.path.join(portable_java, "bin", "java.exe")):
        os.environ["JAVA_HOME"] = portable_java
        os.environ["PATH"] = os.path.join(portable_java, "bin") + os.path.pathsep + os.environ["PATH"]
        break

# Sanitize JAVA_HOME to remove trailing slashes/backslashes and outer quotes.
# Trailing backslashes can escape quotes in Windows command invocation, crashing the PySpark Java gateway.
if "JAVA_HOME" in os.environ:
    os.environ["JAVA_HOME"] = os.environ["JAVA_HOME"].strip().rstrip(r"\/").strip('"')

# Ensure Spark workers use the exact same Python executable as the Flask server
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


from flask import Flask, request, jsonify

# Import custom Spark components
from utils import get_spark_session
from data_loader import load_dataset
from data_cleaning import clean_student_data
import analytics
from exporter import export_to_csv

# Setup Flask application
# Explicitly set static_folder to 'ui' so that static HTML/CSS/JS can be served easily at root.
app = Flask(
    __name__, 
    static_folder=os.path.join(os.path.dirname(__file__), '../ui'),
    static_url_path=''
)

# Global variables for SparkSession, processed DataFrame, and analytics cache
SPARK = None
CLEANED_DF = None
ANALYTICS_CACHE = {}

# Folder for holding uploaded dataset files
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), '../dataset')
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Mapping of route query names to their corresponding analytical functions
ANALYTICS_ROUTING = {
    'top10': analytics.get_top_10_students,
    'highest': analytics.get_highest_scorer,
    'lowest': analytics.get_lowest_scorer,
    'subject_avg': analytics.get_subject_wise_average,
    'dept_avg': analytics.get_department_wise_average,
    'sem_perf': analytics.get_semester_performance,
    'pass_fail': analytics.get_pass_fail_stats,
    'attendance': analytics.get_attendance_analysis,
    'grade_dist': analytics.get_grade_distribution,
    'ranking': analytics.get_student_ranking,
    'class_stats': analytics.get_overall_class_stats
}

def precompute_analytics_cache(df):
    """
    Runs Spark analytics once on dataset load/upload and stores aggregated results in memory.
    Ensures every subsequent dashboard button click responds instantly (< 1 ms).
    """
    global ANALYTICS_CACHE
    cache = {}
    for query_key, func in ANALYTICS_ROUTING.items():
        try:
            res_df = func(df)
            rows = res_df.collect()
            data = [row.asDict() for row in rows]
            cache[query_key] = {
                "columns": res_df.columns,
                "data": data
            }
        except Exception as e:
            print(f"[WARN] Failed to precompute query {query_key}: {e}")
    ANALYTICS_CACHE = cache

@app.route('/')
def index():
    """
    Serves the main HTML5 dashboard layout page.
    """
    return app.send_static_file('index.html')

@app.route('/status', methods=['GET'])
def get_status():
    """
    Endpoint checking if a dataset is already loaded in Spark Session memory,
    or if a default local file (like dataset/student.csv or dataset/students.csv)
    exists to auto-load it on startup.
    """
    global SPARK, CLEANED_DF
    
    if CLEANED_DF is not None:
        return jsonify({
            "status": "ready",
            "filename": "Cached Dataset",
            "columns": CLEANED_DF.columns,
            "count": CLEANED_DF.count()
        })
        
    # Check if user has uploaded a file directly to the dataset/ folder
    default_files = ['student.csv', 'students.csv']
    found_file = None
    for filename in default_files:
        path = os.path.join(UPLOAD_FOLDER, filename)
        if os.path.exists(path):
            found_file = path
            break
            
    if found_file:
        try:
            if SPARK is None:
                SPARK = get_spark_session()
            raw_df = load_dataset(SPARK, found_file)
            CLEANED_DF = clean_student_data(raw_df)
            CLEANED_DF.cache()
            record_count = CLEANED_DF.count()  # Materialize Spark RAM cache immediately
            precompute_analytics_cache(CLEANED_DF)  # Precompute all 11 reports into RAM cache
            return jsonify({
                "status": "ready",
                "filename": os.path.basename(found_file),
                "columns": CLEANED_DF.columns,
                "count": record_count
            })
        except Exception as e:
            return jsonify({"status": "no_data", "message": f"Auto-load of {os.path.basename(found_file)} failed: {str(e)}"})
            
    return jsonify({"status": "no_data", "message": "No dataset loaded. Please upload a CSV."})

@app.route('/upload', methods=['POST'])
def upload_file():
    """
    Endpoint to receive and process a uploaded student dataset.
    """
    global SPARK, CLEANED_DF
    
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded in the request."}), 400
        
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file."}), 400
        
    if not file.filename.endswith('.csv'):
        return jsonify({"error": "Invalid file format. Please upload a CSV file."}), 400
        
    # Save the file locally to the dataset folder
    file_path = os.path.join(UPLOAD_FOLDER, 'students.csv')
    try:
        file.save(file_path)
    except Exception as e:
        return jsonify({"error": f"Failed to save uploaded file: {str(e)}"}), 500
        
    # Execute Spark ingestion and cleaning pipeline
    try:
        # Initialize Spark Session if it doesn't exist yet
        if SPARK is None:
            SPARK = get_spark_session()
            
        # Ingest CSV and map schemas dynamically
        raw_df = load_dataset(SPARK, file_path)
        
        # Clean duplicates, null values, and apply logic
        CLEANED_DF = clean_student_data(raw_df)
        CLEANED_DF.cache()
        record_count = CLEANED_DF.count()  # Materialize Spark RAM cache immediately
        precompute_analytics_cache(CLEANED_DF)  # Precompute all 11 reports into RAM cache
        
        # Return information about the loaded dataset
        return jsonify({
            "status": "success",
            "filename": file.filename,
            "columns": CLEANED_DF.columns,
            "count": record_count
        })
        
    except Exception as e:
        # Clear cached reference if validation failed
        CLEANED_DF = None
        return jsonify({"error": f"Spark Pipeline Initialization Error: {str(e)}"}), 500

@app.route('/preview', methods=['GET'])
def get_preview():
    """
    Endpoint that returns a preview of the first 15 records in the active dataset.
    """
    global CLEANED_DF
    if CLEANED_DF is None:
        return jsonify({"error": "No active dataset loaded in Spark."}), 400
        
    try:
        # Limit rows and fetch them as dictionary models
        # collect() is a Spark action that retrieves all rows from cluster partitions to the driver.
        preview_rows = CLEANED_DF.limit(15).collect()
        data = [row.asDict() for row in preview_rows]
        
        return jsonify({
            "columns": CLEANED_DF.columns,
            "data": data
        })
    except Exception as e:
        return jsonify({"error": f"Failed to generate preview: {str(e)}"}), 500

@app.route('/analyze/<query_type>', methods=['GET'])
def analyze(query_type):
    """
    Endpoint to execute specific analytical queries.
    Utilizes precomputed memory cache for sub-millisecond performance.
    """
    global CLEANED_DF, ANALYTICS_CACHE
    if CLEANED_DF is None:
        return jsonify({"error": "No active dataset loaded. Please upload a CSV first."}), 400
        
    if query_type not in ANALYTICS_ROUTING:
        return jsonify({"error": f"Unknown query type: {query_type}"}), 404
        
    # Instant sub-millisecond response from precomputed memory cache
    if query_type in ANALYTICS_CACHE:
        return jsonify(ANALYTICS_CACHE[query_type])
        
    try:
        # Fallback compute if missing from cache
        analysis_func = ANALYTICS_ROUTING[query_type]
        result_df = analysis_func(CLEANED_DF)
        
        # Convert distributed Spark DataFrame to list of local dictionaries
        rows = result_df.collect()
        data = [row.asDict() for row in rows]
        payload = {"columns": result_df.columns, "data": data}
        ANALYTICS_CACHE[query_type] = payload
        
        return jsonify(payload)
    except Exception as e:
        return jsonify({"error": f"Spark execution failed: {str(e)}"}), 500

@app.route('/export/<query_type>', methods=['GET'])
def export(query_type):
    """
    Endpoint that triggers query calculations and writes output directly to CSV file.
    """
    global CLEANED_DF
    if CLEANED_DF is None:
        return jsonify({"error": "No active dataset loaded."}), 400
        
    if query_type not in ANALYTICS_ROUTING:
        return jsonify({"error": f"Unknown query type: {query_type}"}), 404
        
    try:
        analysis_func = ANALYTICS_ROUTING[query_type]
        result_df = analysis_func(CLEANED_DF, limit=None)
        
        # Define path inside local output folder
        target_filename = f"{query_type}_report.csv"
        target_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../output', target_filename))
        
        # Export as a single CSV
        export_to_csv(result_df, target_path)
        
        return jsonify({
            "status": "success",
            "path": target_path
        })
    except Exception as e:
        return jsonify({"error": f"Export process failed: {str(e)}"}), 500

@app.after_request
def add_cors_headers(response):
    """
    Appends CORS headers to every response. This allows frontend files served 
    by static web servers (like port 61547 / VS Code Live Server) to successfully 
    fetch API results from the Flask Spark backend on port 5000.
    """
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type,Authorization'
    response.headers['Access-Control-Allow-Methods'] = 'GET,PUT,POST,DELETE,OPTIONS'
    return response

if __name__ == '__main__':
    # Start the Flask development server on localhost:5000
    # debug=False is recommended when running Spark applications in same process
    # to avoid thread-safety restart loops.
    app.run(host='127.0.0.1', port=5000, debug=False)
