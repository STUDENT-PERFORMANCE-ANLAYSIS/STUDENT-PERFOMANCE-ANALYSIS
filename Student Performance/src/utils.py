# src/utils.py
"""
Utility module for initializing and managing the Apache Spark session.

Why this file exists:
In Apache Spark, the SparkSession is the unified entry point for programming with 
Spark's Dataset and DataFrame APIs. Since initializing a SparkSession is resource-intensive 
and should only be done once per application runtime (singleton pattern), this module 
ensures a single, configured SparkSession is shared across all other modules.

Viva Tips:
- What is SparkSession? It is the entry point introduced in Spark 2.0 that combines SparkContext, SQLContext, and HiveContext.
- What does 'local[*]' mean? It tells Spark to run locally using all available CPU cores on your machine.
- What is spark.driver.host configuration? It binds the driver to localhost, which avoids networking/firewall issues on Windows machines.
"""

from pyspark.sql import SparkSession

def get_spark_session(app_name="StudentPerformanceAnalysis"):
    """
    Creates or retrieves an existing SparkSession configured for local execution.
    
    Parameters:
    app_name (str): Name of the Spark application visible on the Spark Web UI (port 4040).
    
    Returns:
    SparkSession: The active Spark Session.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.warehouse.dir", "spark-warehouse") \
        .getOrCreate()
    
    # Enforce 8 shuffle partitions at runtime to eliminate local 200-task thread overhead
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    
    # Set log level to WARN to prevent the console from being flooded with INFO logs.
    # This keeps our terminal clean during student viva demonstrations.
    spark.sparkContext.setLogLevel("WARN")
    
    return spark
