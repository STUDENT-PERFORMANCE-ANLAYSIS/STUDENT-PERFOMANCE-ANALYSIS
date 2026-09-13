# src/data_cleaning.py
"""
Data Cleaning Module for sanitizing student records using PySpark DataFrame API.

Why this file exists:
Raw data is frequently contaminated with duplicates, missing (null) values, 
and out-of-range numerical fields (such as marks > 100 or attendance < 0).
Running statistical analysis on dirty data leads to skewed results. This module 
sanitizes the DataFrame by casting column types, dropping duplicate records, 
filtering out logical anomalies, and filling in missing optional fields (like recalculating 
grades when they are empty but marks are available).

Viva Tips:
- How does PySpark handle duplicates? `dropDuplicates()` is a transformation that compares 
  rows across specified columns (e.g., Student_ID & Subject) and retains only unique instances.
- What is `when().otherwise()`? It is PySpark's equivalent of SQL's CASE WHEN statement, 
  allowing conditional column values.
- Why cast data types explicitly? CSV reads all fields as strings or general types. 
  Casting fields like Marks and Attendance to double/integer ensures arithmetic operations 
  (like finding averages) work correctly.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, trim

def clean_student_data(df: DataFrame) -> DataFrame:
    """
    Cleans and standardizes the Spark DataFrame.
    
    Cleaning steps:
    1. Removes leading/trailing spaces from string fields.
    2. Drops rows where crucial mandatory fields (Student_ID, Subject) are null or empty.
    3. Casts Marks and Attendance to DoubleType.
    4. Filters out rows with invalid/impossible marks (< 0 or > 100) or attendance (< 0 or > 100).
    5. Deduplicates student records based on (Student_ID, Subject).
    6. Fills missing optional fields with logical defaults.
    7. Recalculates student Grade if it is null/empty based on Marks.
    
    Parameters:
    df (DataFrame): Validated Spark DataFrame from the data loader.
    
    Returns:
    DataFrame: Cleaned Spark DataFrame.
    """
    # 1. Clean whitespace from text columns
    # trim() is a Spark Transformation that removes leading/trailing spaces.
    df = df.withColumn("Student_ID", trim(col("Student_ID"))) \
           .withColumn("Student_Name", trim(col("Student_Name"))) \
           .withColumn("Department", trim(col("Department"))) \
           .withColumn("Semester", trim(col("Semester"))) \
           .withColumn("Subject", trim(col("Subject"))) \
           .withColumn("Grade", trim(col("Grade")))
           
    # 2. Filter out records where mandatory identifiers are null or empty strings
    df = df.filter(
        (col("Student_ID").isNotNull()) & (col("Student_ID") != "") &
        (col("Subject").isNotNull()) & (col("Subject") != "")
    )
    
    # 3. Explicitly cast Marks and Attendance to DoubleType
    df = df.withColumn("Marks", col("Marks").cast("double")) \
           .withColumn("Attendance", col("Attendance").cast("double"))
           
    # 4. Handle invalid/out-of-range numerical fields
    # Clean marks to be within [0, 100] and attendance within [0, 100]
    df = df.filter(
        (col("Marks").isNotNull()) & (col("Marks") >= 0) & (col("Marks") <= 100) &
        (col("Attendance").isNotNull()) & (col("Attendance") >= 0) & (col("Attendance") <= 100)
    )
    
    # 5. Deduplicate records on the primary business key: (Student_ID, Subject)
    # A student should have exactly one mark for a particular course.
    df = df.dropDuplicates(["Student_ID", "Subject"])
    
    # 6. Fill missing text fields with logical defaults
    df = df.fillna({
        "Student_Name": "Unknown",
        "Department": "General",
        "Semester": "All Semesters"
    })
    
    # 7. Auto-calculate/Standardize Grade column
    # Define grading criteria if missing or invalid.
    # We use when().otherwise() to map marks to KTU-like letter grades:
    # >=90: O, >=80: A+, >=70: A, >=60: B+, >=50: B, >=40: C, >=35: D, <35: F
    calculated_grade = when(col("Marks") >= 90, "O") \
                      .when(col("Marks") >= 80, "A+") \
                      .when(col("Marks") >= 70, "A") \
                      .when(col("Marks") >= 60, "B+") \
                      .when(col("Marks") >= 50, "B") \
                      .when(col("Marks") >= 40, "C") \
                      .when(col("Marks") >= 35, "D") \
                      .otherwise("F")
                      
    # We apply this calculated grade ONLY when the existing grade is null, empty, or invalid.
    valid_grades = ["S", "O", "A+", "A", "B+", "B", "C+", "C", "D", "P", "F"]
    df = df.withColumn(
        "Grade",
        when(
            (col("Grade").isNull()) | (col("Grade") == "") | (~col("Grade").isin(valid_grades)),
            calculated_grade
        ).otherwise(col("Grade"))
    )
    
    return df
