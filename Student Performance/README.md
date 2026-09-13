# Student Performance Analysis Using Apache Spark (PySpark)

A complete B.Tech S5 KTU Big Data Processing Mini-Project. This project demonstrates how to ingest, clean, analyze, and export student academic datasets dynamically using the Apache Spark DataFrame API and present it through a modern, responsive web dashboard built with Python (Flask) and Glassmorphic Vanilla CSS3/JS.

---

## 📂 Project Folder Structure & Communication Flow

### Folder Layout
```text
StudentPerformanceAnalysis/
│
├── dataset/                    # Stores uploaded and raw datasets (students.csv)
│
├── src/                        # Python Backend Engine
│   ├── main.py                 # Flask server & REST endpoint coordinator
│   ├── data_loader.py          # Dynamic header mapper & structural validator
│   ├── data_cleaning.py        # Data sanitization, duplicate remover, & grader
│   ├── analytics.py            # PySpark DataFrame API analytics queries
│   ├── exporter.py             # Coalesced CSV report writer
│   └── utils.py                # Singleton SparkSession configurations
│
├── ui/                         # Glassmorphic Frontend Dashboard
│   ├── index.html              # Dashboard semantic layout
│   ├── style.css               # Glassmorphism dark-mode responsive skin
│   └── script.js               # Async AJAX client-side API script
│
├── output/                     # Destination folder for exported CSV reports
│
├── requirements.txt            # Project libraries dependency list
│
├── .gitignore                  # Exclusion file for Spark runtime artifacts
│
└── README.md                   # Complete installation, execution, and viva guide
```

### Module Communication Architecture
1. **User Interface (`ui/`)**: The client browser loads `index.html`, styled with `style.css` for a premium glassmorphic UI. When the user interacts (uploads a CSV or clicks query buttons), `script.js` captures the event.
2. **REST Endpoints (`src/main.py`)**: `script.js` fires an asynchronous `fetch()` API request to the running Python Flask backend.
3. **Ingestion (`src/data_loader.py` & `src/utils.py`)**: Flask passes the CSV file to the data loader. It starts a configured local Spark Session (`utils.py`), scans the dataset headers, maps potential aliases (e.g. `roll no` to `Student_ID`), and validates mandatory fields.
4. **Data Cleaning (`src/data_cleaning.py`)**: The loaded Spark DataFrame is cleaned of duplicates, types are cast, out-of-bounds fields are dropped, and missing grades are calculated. The output is cached in memory (`.cache()`) for speed.
5. **Data Analytics (`src/analytics.py`)**: When the user clicks an analysis button, Flask calls `analytics.py`, which applies Spark transformations and returns results using `.collect()`.
6. **Data Export (`src/exporter.py`)**: When the export command is triggered, Spark coalesces the query results into 1 partition and writes a single clean report to the `output/` folder.

---

## 🛠️ Windows Installation & Setup Guide

To run Apache Spark on Windows, you must set up Python, Java (JDK), and the Hadoop winutils binaries.

### Phase 1: Python Installation (Windows)
1. Download Python 3.10 or 3.11 (64-bit) from the official website. *Note: Avoid 3.12+ as some older PySpark dependencies require stability on 3.10/3.11.*
2. Run the installer and **Check the box: "Add Python to PATH"**.
3. Complete the installation. Verify in Command Prompt:
   ```cmd
   python --version
   pip --version
   ```

### Phase 2: Java JDK Setup (Required by Spark)
Apache Spark runs on top of the Java Virtual Machine (JVM). It requires Java 8 or 11.
1. Download **Java SE Development Kit (JDK) 11** or **JDK 8** (Windows x64 Installer) from Oracle or OpenJDK.
2. Install JDK to a path without spaces (e.g., `C:\Java\jdk-11`). *Avoid Program Files as spaces in folders can break Hadoop paths.*
3. Set Environment Variables:
   - Search for **"Edit the system environment variables"** in Windows.
   - Click **Environment Variables...**
   - Click **New...** under System Variables:
     - Variable Name: `JAVA_HOME`
     - Variable Value: `C:\Java\jdk-11` (or your exact JDK folder path)
   - Find the `Path` system variable, edit it, and click **New**:
     - Add: `%JAVA_HOME%\bin`
4. Verify JVM in CMD:
   ```cmd
   java -version
   ```

### Phase 3: Apache Spark & Hadoop Winutils Setup (Windows)
1. Download **Apache Spark 3.5.1** (Pre-built for Apache Hadoop 3.3 and later) from [spark.apache.org](https://spark.apache.org/downloads.html).
2. Extract the downloaded `.tgz` archive to a folder (e.g., `C:\Spark\spark-3.5.1`).
3. Set environment variables:
   - Variable Name: `SPARK_HOME`
   - Variable Value: `C:\Spark\spark-3.5.1`
   - Edit the system `Path` variable and add: `%SPARK_HOME%\bin`
4. **Hadoop Winutils Config (Windows Specific)**:
   Spark requires Hadoop filesystem binaries to access files on Windows.
   - Create a folder directory `C:\Hadoop\bin`.
   - Download the file `winutils.exe` and `hadoop.dll` for Hadoop version 3.3.0 (or match your Spark dependency) from a trusted GitHub repository (e.g., [cdarlint/winutils](https://github.com/cdarlint/winutils)).
   - Place both files inside `C:\Hadoop\bin`.
   - Set system environment variables:
     - Variable Name: `HADOOP_HOME`
     - Variable Value: `C:\Hadoop`
     - Edit the system `Path` variable and add: `%HADOOP_HOME%\bin`
5. Verify Spark Shell runs locally:
   - Open a fresh Command Prompt and type:
     ```cmd
     spark-shell
     ```
   - (Type `:quit` or press Ctrl+C to exit the shell).

### Phase 4: Project Dependencies Setup
Navigate to the directory of the project in VS Code or CMD:
```cmd
cd "c:\Users\fawaz\OneDrive\Desktop\Student Performance"
pip install -r requirements.txt
```

---

## 🚀 Execution & Background Server Management

To make the application extremely easy to run, a server management utility [manage_server.bat](file:///c:/Users/fawaz/OneDrive/Desktop/Student%20Performance/manage_server.bat) is provided in the project root. This utility runs the Flask backend silently in the background and registers the server to run automatically when Windows boots.

### Background Server Commands (Recommended)

Run `manage_server.bat` from Command Prompt with one of the following arguments:
```cmd
manage_server.bat start             - Starts the Flask server in the background using pythonw.
manage_server.bat status            - Checks if the Flask server is running on port 5000.
manage_server.bat stop              - Stops the Flask server (terminates the process on port 5000).
manage_server.bat restart           - Stops and restarts the Flask server.
manage_server.bat install-startup   - Registers the server to run automatically on Windows startup.
manage_server.bat uninstall-startup - Deregisters the server from Windows startup.
manage_server.bat open-ui           - Opens the application dashboard in your default browser.
```

### Manual Execution (Alternative)

If you prefer to run the server interactively in a visible command window:
1. Start the Flask application backend server:
   ```cmd
   python src/main.py
   ```
2. Once the server starts, you will see output like:
   * `* Running on http://127.0.0.1:5000/`
3. Open your browser and go to `http://127.0.0.1:5000/` (or open [index.html](file:///c:/Users/fawaz/OneDrive/Desktop/Student%20Performance/ui/index.html) directly).

### Demonstration Walkthrough:
- Drag or click to upload the sample dataset file: `dataset/students.csv`.
- Observe the Spark initialization logs (if running manually) or click analytics buttons.
- The status bar will show "Dataset Loaded: students.csv" and populate a preview table automatically.
- Click any button in the Control Panel (e.g. *Student Ranking*, *Subject Averages*, *Class Statistics*) to see immediate, cleaned calculations.
- Click the green **"Export Report to CSV"** button to generate single-partition CSV output inside the `output/` directory.

---

## 🎓 KTU S5 Viva Voce Questions & Answers

These questions are compiled specifically for B.Tech students facing the Big Data Processing laboratory external examination and viva:

### Q1: What is Apache Spark? How does it differ from MapReduce?
**Answer**: Apache Spark is an open-source, distributed general-purpose cluster-computing framework. The key difference is that MapReduce reads and writes intermediate data back to physical disks (HDFS), while Spark processes data **in-memory** using RDDs (Resilient Distributed Datasets). This makes Spark up to 100 times faster for iterative algorithms.

### Q2: What is the entry point of a Spark application in Spark 2.x and 3.x?
**Answer**: `SparkSession`. It was introduced in Spark 2.0 to replace the older, separate contexts (`SparkContext` for RDDs, `SQLContext` for DataFrames, and `HiveContext` for external databases) with a single, unified coding interface.

### Q3: What are RDDs, and what does "Resilient Distributed Dataset" mean?
**Answer**: RDD is Spark's core abstraction.
- **Resilient**: Fault-tolerant. It can reconstruct data partitions using a "Lineage Graph" if a node fails.
- **Distributed**: Data is partitioned and spread across multiple nodes in a cluster.
- **Dataset**: A collection of objects.

### Q4: Explain the difference between Spark Transformations and Actions.
**Answer**:
- **Transformations**: Operations that create a new dataset from an existing one (e.g., `filter()`, `select()`, `groupBy()`, `map()`). They are **lazy**, meaning they only build an execution plan (DAG) and do not run immediately.
- **Actions**: Operations that trigger calculation and return results to the driver program or write data to storage (e.g., `show()`, `collect()`, `count()`, `first()`, `write()`).

### Q5: What is Lazy Evaluation? What are its benefits?
**Answer**: Lazy Evaluation means Spark does not execute transformations immediately. It records them in a Directed Acyclic Graph (DAG). Spark compiles and optimizes the entire execution path (through the Catalyst Optimizer) only when an **Action** is called, reducing data shuffling and disk writes.

### Q6: Why did you use `coalesce(1)` before writing output files?
**Answer**: Spark writes data partitions in parallel, resulting in a directory of multiple `part-*.csv` files. Using `.coalesce(1)` merges all distributed partitions into a single partition, forcing Spark to write a single CSV output file.

### Q7: What is the difference between `.coalesce()` and `.repartition()`?
**Answer**:
- `repartition()` creates a specified number of new partitions by performing a full data shuffle across the network. It can increase or decrease partitions.
- `coalesce()` merges adjacent partitions on the same nodes without causing a full shuffle. It can only be used to **decrease** partitions, making it far more efficient.

### Q8: What are Window functions in PySpark? When are they useful?
**Answer**: Window functions compute aggregates over a group of records (a window) while maintaining the granularity of individual rows. We use them for ranking (e.g., `rank().over(windowSpec)`) or running totals, where group-by would collapse the individual rows.

### Q9: Why did you cache the DataFrame using `.cache()` inside `main.py`?
**Answer**: Due to lazy evaluation, Spark re-evaluates all cleaning operations every time a user clicks a button. Caching (`.cache()`) stores the cleaned DataFrame in RAM, so subsequent queries fetch the data directly from memory.

---

## 📈 KTU Mini-Project Presentation Outline (Slides Guide)

Here is a recommended structure for your project defense slides:

1. **Slide 1: Title Slide**
   - Project Title: Student Performance Analysis Using Apache Spark
   - Details: Name, Roll No, S5 B.Tech CSE, College logo
2. **Slide 2: Introduction**
   - Explain Big Data context in academic systems.
   - Describe how traditional databases struggle with processing large datasets (like statewide university board marks) in real time.
3. **Slide 3: Problem Statement**
   - High administrative latency in analyzing multi-college student records.
   - Missing fields, duplicate data, and schema drift in raw CSV records.
4. **Slide 4: Objectives**
   - Build a local, modular Big Data pipeline.
   - Clean anomalies and calculate metrics using Spark DataFrame API.
   - Create a clean visual UI dashboard.
5. **Slide 5: Architecture & Data Flow**
   - *Include the Mermaid flow diagram from the implementation plan.*
6. **Slide 6: Proposed System vs Existing System**
   - Existing: Excel/SQL (slow, manual, limited scale).
   - Proposed: Apache Spark (highly scalable, dynamic, automatic).
7. **Slide 7: Ingestion & Inferred Schemas**
   - Highlight the dynamic column-mapping dictionary, preventing failure when column names vary slightly.
8. **Slide 8: Data Cleaning & Sanitization**
   - Show code snippets of `dropDuplicates()`, `filter()`, and conditional KTU grading logic.
9. **Slide 9: Analytical Implementation**
   - Detail the 11 analytical features.
   - Explain the Window function implementation for student ranking.
10. **Slide 10: System Performance & Results**
    - Show screenshots of the dashboard.
    - Show output reports exported to the `output/` folder.
11. **Slide 11: Advantages & Future Scope**
    - High horizontal scalability (can run on multi-node AWS EMR clusters without code changes).
    - Future: Real-time streaming logs (Spark Streaming) and predicting dropouts (Spark MLlib).
12. **Slide 12: Conclusion & References**
    - Summarize key takeaways and cite Spark documentation.
