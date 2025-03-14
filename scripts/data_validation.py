import sys
import argparse
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import zipfile
import os

def is_valid_xlsx(file_path):
    """Check if the file is a valid .xlsx file by trying to open it as a ZIP archive."""
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            # Check if it contains the expected structure for an .xlsx file
            file_list = zip_ref.namelist()
            if 'xl/workbook.xml' in file_list:
                return True, "Valid XLSX file"
            else:
                return False, "Missing 'xl/workbook.xml' in XLSX structure"
    except zipfile.BadZipFile:
        return False, "File is not a valid ZIP archive (corrupt or not an XLSX)"
    except Exception as e:
        return False, f"Error checking XLSX validity: {str(e)}"

def read_excel(file_path):
    """Read Excel file into Spark DataFrame with proper null handling"""
    spark = SparkSession.builder \
        .appName("DataValidation") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()

    # Check if the file exists
    if not os.path.exists(file_path):
        print(f"Error: The file {file_path} does not exist.")
        sys.exit(1)

    # Check if the file is a valid .xlsx file
    valid_xlsx, message = is_valid_xlsx(file_path)
    if not valid_xlsx:
        print(f"Error: {message}")
        sys.exit(1)

    try:
        # Try to read the file with pandas
        pandas_df = pd.read_excel(file_path, dtype=str, engine='openpyxl')  # Specify the engine
    except ValueError as e:
        print(f"Error reading the Excel file: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error reading Excel file: {e}")
        sys.exit(1)

    # Replace NaN with None for proper null handling
    pandas_df = pandas_df.replace({np.nan: None})

    # Create explicit schema - all string types initially
    fields = [StructField(str(col_name), StringType(), True) for col_name in pandas_df.columns]
    schema = StructType(fields)

    # Convert to Spark DataFrame with explicit schema
    spark_df = spark.createDataFrame(pandas_df, schema=schema)

    return spark_df, pandas_df

def validate_data(spark_df, pandas_df, output_dir):
    """Perform data validation checks and generate reports"""
    print("Performing data validation...")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Example data validation: Check for missing values in Spark DataFrame
    missing_values_spark = spark_df.filter(spark_df.isNull().any()).count()
    missing_values_pandas = pandas_df.isnull().sum()

    print(f"Missing values in Spark DataFrame: {missing_values_spark}")
    print(f"Missing values in Pandas DataFrame:\n{missing_values_pandas}")

    # Generate report
    report_path = os.path.join(output_dir, "validation_report.txt")
    with open(report_path, 'w') as f:
        f.write(f"Missing values count (Spark): {missing_values_spark}\n")
        f.write("Missing values summary (Pandas):\n")
        f.write(missing_values_pandas.to_string())
    
    print(f"Validation report generated at: {report_path}")

def main():
    """Main entry point for data validation pipeline"""
    parser = argparse.ArgumentParser(description='Data validation tool for XLSX files')
    parser.add_argument('--input', required=True, help='Input XLSX file path')
    parser.add_argument('--output', required=True, help='Output directory for reports')
    args = parser.parse_args()

    print(f"Validating file: {args.input}")
    
    # Read Excel file
    spark_df, pandas_df = read_excel(args.input)

    # Validate the data and generate reports
    validate_data(spark_df, pandas_df, args.output)

if __name__ == "__main__":
    main()