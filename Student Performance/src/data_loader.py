# src/data_loader.py
"""
Data Loader Module for reading and validating CSV files in Apache Spark.

Why this file exists:
In data engineering pipelines, ingestion is the first step. This module is responsible 
for loading raw CSV data into a PySpark DataFrame. Because student datasets can come 
from different colleges or systems, column names might differ slightly (e.g., 'Roll No' 
instead of 'Student_ID', or 'Score' instead of 'Marks'). This module inspects the headers 
dynamically, maps aliases to standard internal names, validates that mandatory columns 
exist, and adds missing optional columns with defaults. This makes our project robust 
and schema-agnostic.

Viva Tips:
- What is schema inference? In Spark, 'inferSchema=True' triggers an extra scan of the 
  dataset to guess data types (Integer, String, etc.) instead of reading everything as String.
- What is the difference between Transformation and Action in Spark? 
  - Transformations (like withColumn, withColumnRenamed) are lazy; they define the plan but do not execute it.
  - Actions (like count, show, collect) trigger the actual execution.
- What is 'lit()'? It stands for 'literal' and is a Spark SQL function used to add a constant/literal value as a column.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

# Dictionary of standard internal column names mapped to possible variations/aliases
COLUMN_ALIASES = {
    "Student_ID": ["student_id", "studentid", "student id", "roll_no", "roll no", "rollno", "id"],
    "Student_Name": ["student_name", "studentname", "student name", "name", "full_name", "fullname"],
    "Department": ["department", "dept", "branch", "stream"],
    "Semester": ["semester", "sem", "term"],
    "Subject": ["subject", "sub", "course", "paper"],
    "Marks": ["marks", "mark", "score", "total_marks", "total marks", "total", "total_score"],
    "Attendance": ["attendance", "attendance%", "attendance (%)", "att", "present_percentage"],
    "Grade": ["grade", "letter_grade", "letter grade", "class_grade"]
}

# Define which fields are absolutely required for basic processing
MANDATORY_COLUMNS = ["Student_ID", "Subject", "Marks"]

def detect_and_map_schema(df: DataFrame) -> DataFrame:
    """
    Inspects the columns of the input DataFrame and renames them to standard names
    if they match any alias. It warns/raises errors for missing mandatory columns
    and adds missing optional columns with default values.
    
    Parameters:
    df (DataFrame): Raw PySpark DataFrame read from CSV.
    
    Returns:
    DataFrame: Renamed and aligned DataFrame.
    """
    original_cols = df.columns
    mapped_columns = {}
    
    # Iterate through our standard expected columns and check for matches in the dataset
    for standard_name, aliases in COLUMN_ALIASES.items():
        for original_col in original_cols:
            cleaned_original = original_col.strip().lower().replace("_", " ").replace("-", " ")
            if cleaned_original == standard_name.lower().replace("_", " ") or cleaned_original in [a.lower().replace("_", " ") for a in aliases]:
                mapped_columns[original_col] = standard_name
                break # Found a match, move to next standard column
                
    # Apply column renames to the DataFrame
    # Note: withColumnRenamed is a Spark TRANSFORMATION. It returns a new DataFrame.
    for original, standard in mapped_columns.items():
        df = df.withColumnRenamed(original, standard)
        
    # Check if all mandatory columns were successfully mapped
    resolved_cols = df.columns
    missing_mandatory = [col for col in MANDATORY_COLUMNS if col not in resolved_cols]
    
    if missing_mandatory:
        raise ValueError(
            f"Missing required columns in dataset: {missing_mandatory}. "
            f"Please ensure your dataset contains equivalent fields for: {MANDATORY_COLUMNS}"
        )
        
    # Inject optional columns with default null/lit values if they don't exist in the CSV
    # This prevents the downstream code from crashing when these columns are referenced.
    # lit() is a Spark Transformation that creates a column with a constant literal value.
    if "Student_Name" not in resolved_cols:
        df = df.withColumn("Student_Name", lit("Unknown"))
    if "Department" not in resolved_cols:
        df = df.withColumn("Department", lit("General"))
    if "Semester" not in resolved_cols:
        df = df.withColumn("Semester", lit("All Semesters"))
    if "Attendance" not in resolved_cols:
        df = df.withColumn("Attendance", lit(None).cast("double"))
    if "Grade" not in resolved_cols:
        df = df.withColumn("Grade", lit(None).cast("string"))
        
    # Rearrange and select only the standard columns to keep the dataset clean
    standard_fields = list(COLUMN_ALIASES.keys())
    df = df.select(*standard_fields)
    
    return df

def load_dataset(spark, file_path: str) -> DataFrame:
    """
    Loads a CSV file from a path, infers its schema, maps columns, and validates it.
    
    Parameters:
    spark (SparkSession): The active Spark Session.
    file_path (str): Absolute or relative path to the CSV file.
    
    Returns:
    DataFrame: Mapped and validated Spark DataFrame.
    """
    import os
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Dataset file not found at: {file_path}")
        
    # Read CSV using Spark DataFrameReader API
    # options:
    # - header=True: Treats first line of CSV as header names.
    # - inferSchema=True: Spark reads data to automatically guess data types.
    raw_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("mode", "FAILFAST") \
        .csv(file_path)
        
    # Map and validate schemas dynamically
    processed_df = detect_and_map_schema(raw_df)
    
    return processed_df
