import pandas as pd
import numpy as np
import os
import sys
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, regexp_replace
from pyspark.sql.types import FloatType, StringType, StructField, StructType
try:
    from distutils.version import LooseVersion
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "setuptools"])
    from distutils.version import LooseVersion

def read_excel(file_path):
    """Read Excel file into Spark DataFrame with proper null handling"""
    spark = SparkSession.builder \
        .appName("DataValidation") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()
    
    # Read with pandas
    pandas_df = pd.read_excel(file_path, dtype=str)
    
    # Replace NaN with None for proper null handling
    pandas_df = pandas_df.replace({np.nan: None})
    
    # Create explicit schema - all string types initially
    fields = [StructField(str(col_name), StringType(), True) for col_name in pandas_df.columns]
    schema = StructType(fields)
    
    # Convert to Spark DataFrame with explicit schema
    spark_df = spark.createDataFrame(pandas_df, schema=schema)
    
    return spark_df, pandas_df

def check_missing_values(spark_df):
    """Calculate missing values for each column"""
    missing_counts = {}
    for column in spark_df.columns:
        count_val = spark_df.filter(col(column).isNull()).count()
        missing_counts[column] = count_val
    
    return pd.DataFrame(missing_counts, index=[0])

def check_duplicates(spark_df):
    """Count duplicate rows"""
    return spark_df.count() - spark_df.distinct().count()

def check_schema(spark_df):
    """Validate numeric columns can be cast to FloatType"""
    schema_errors = []
    for column in spark_df.columns:
        try:
            # Attempt to cast to float after cleaning
            cleaned = spark_df.withColumn("cleaned", regexp_replace(col(column), "[^0-9.]", ""))
            error_count = cleaned.filter(
                (col("cleaned").cast(FloatType()).isNull()) & 
                (col(column).isNotNull()) & 
                (col(column) != "")
            ).count()
            
            if error_count > 0:
                schema_errors.append(f"{column}: Contains {error_count} non-numeric values")
        except Exception as e:
            schema_errors.append(f"{column}: Type validation failed - {str(e)}")
    
    return schema_errors

def generate_report(file_path, spark_df, missing_values, duplicate_count, schema_errors):
    """Generate reports with proper path handling"""
    dataset_name = os.path.basename(file_path)
    total_records = spark_df.count()

    # Calculate total missing values
    total_missing = sum(missing_values.iloc[0])

    # Excel Report
    report_data = {
        'Dataset Name': [dataset_name],
        'Total Records': [total_records],
        'Missing Values': [total_missing],
        'Duplicate Records': [duplicate_count],
        'Schema Errors': ["; ".join(schema_errors) if schema_errors else "None"],
        'Status': ['FAILED' if any([total_missing > 0, duplicate_count > 0, schema_errors]) else 'PASSED']
    }
    
    report_dir = os.path.join(os.path.dirname(file_path), 'reports')
    os.makedirs(report_dir, exist_ok=True)
    
    # Save Excel report
    excel_path = os.path.join(report_dir, 'data_quality_report.xlsx')
    pd.DataFrame(report_data).to_excel(excel_path, index=False)
    
    # Save detailed missing values report
    missing_path = os.path.join(report_dir, 'missing_values_report.xlsx')
    missing_values.to_excel(missing_path, index=False)
    
    # Save Text report
    txt_path = os.path.join(report_dir, 'data_quality_report.txt')
    with open(txt_path, 'w') as f:
        f.write(f"Data Quality Report - {dataset_name}\n")
        f.write("----------------------------------------\n")
        f.write(f"Total Records: {total_records}\n")
        f.write(f"Missing Values: {report_data['Missing Values'][0]}\n")
        f.write(f"Duplicate Records: {report_data['Duplicate Records'][0]}\n")
        f.write(f"Schema Errors: {report_data['Schema Errors'][0]}\n")
        f.write(f"Status: {report_data['Status'][0]}\n")
        
        if schema_errors:
            f.write("\nSchema Error Details:\n")
            for error in schema_errors:
                f.write(f"- {error}\n")
    
    print(f"Reports generated at: {report_dir}")
    return excel_path, txt_path

def main(file_path):
    """Main function to run data validation"""
    print(f"Validating file: {file_path}")
    
    # Read data
    spark_df, pandas_df = read_excel(file_path)
    print(f"Successfully loaded data with {spark_df.count()} rows and {len(spark_df.columns)} columns")
    
    # Run checks
    print("Checking for missing values...")
    missing = check_missing_values(spark_df)
    
    print("Checking for duplicates...")
    duplicates = check_duplicates(spark_df)
    
    print("Checking schema integrity...")
    schema_issues = check_schema(spark_df)
    
    # Generate report
    print("Generating reports...")
    excel_path, txt_path = generate_report(file_path, spark_df, missing, duplicates, schema_issues)
    
    print(f"Validation complete! Reports saved to:")
    print(f"- Excel Report: {excel_path}")
    print(f"- Text Report: {txt_path}")
    
    # Stop Spark session
    spark_df.sparkSession.stop()
    print("Spark session stopped.")

if __name__ == "__main__":
    #input_file = "/Users/dilshanperera/Desktop/data_quality_pipeline/input_data/test1.xlsx"
    # Change this in your main function
    input_file = "input_data/test1.xlsx"
    main(input_file) 




