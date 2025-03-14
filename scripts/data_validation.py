import sys
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import zipfile

def is_valid_xlsx(file_path):
    """Check if the file is a valid .xlsx file by trying to open it as a ZIP archive."""
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            # Check if it contains the expected structure for an .xlsx file
            file_list = zip_ref.namelist()
            if 'xl/workbook.xml' in file_list:
                return True
            else:
                return False
    except zipfile.BadZipFile:
        return False

def read_excel(file_path):
    """Read Excel file into Spark DataFrame with proper null handling"""
    spark = SparkSession.builder \
        .appName("DataValidation") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()

    # Check if the file is a valid .xlsx file
    if not is_valid_xlsx(file_path):
        print(f"Error: The file {file_path} is not a valid .xlsx file.")
        sys.exit(1)

    try:
        # Try to read the file with pandas
        pandas_df = pd.read_excel(file_path, dtype=str, engine='openpyxl')  # Specify the engine
    except ValueError as e:
        print(f"Error reading the Excel file: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

    # Replace NaN with None for proper null handling
    pandas_df = pandas_df.replace({np.nan: None})

    # Create explicit schema - all string types initially
    fields = [StructField(str(col_name), StringType(), True) for col_name in pandas_df.columns]
    schema = StructType(fields)

    # Convert to Spark DataFrame with explicit schema
    spark_df = spark.createDataFrame(pandas_df, schema=schema)

    return spark_df, pandas_df

def validate_data(spark_df, pandas_df):
    """Perform data validation checks"""
    print("Performing data validation...")

    # Example data validation: Check for missing values in Spark DataFrame
    missing_values_spark = spark_df.filter(spark_df.isNull().any()).count()
    missing_values_pandas = pandas_df.isnull().sum()

    print(f"Missing values in Spark DataFrame: {missing_values_spark}")
    print(f"Missing values in Pandas DataFrame:\n{missing_values_pandas}")
    
    # Add other validation checks here as needed

def main(input_file):
    """Main entry point for data validation pipeline"""
    print(f"Validating file: {input_file}")
    
    # Read Excel file
    spark_df, pandas_df = read_excel(input_file)

    # Validate the data
    validate_data(spark_df, pandas_df)

if __name__ == "__main__":
    # Input file for validation
    input_file = "input_data/test1.xlsx"  # Update path as necessary
    main(input_file)
