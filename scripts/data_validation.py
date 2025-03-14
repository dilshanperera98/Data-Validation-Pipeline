import zipfile
import pandas as pd
import os
import logging
import argparse

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def is_valid_xlsx(file_path):
    """Check if the file is a valid .xlsx file by trying to open it as a ZIP archive."""
    if not os.path.isfile(file_path):
        logging.error(f"The file {file_path} does not exist.")
        return False

    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            # Check if it contains the expected structure for an .xlsx file
            file_list = zip_ref.namelist()
            if 'xl/workbook.xml' in file_list:
                return True
            else:
                logging.error(f"The file {file_path} is not a valid .xlsx file.")
                return False
    except zipfile.BadZipFile:
        logging.error(f"The file {file_path} is not a valid ZIP archive or is corrupted.")
        return False
    except Exception as e:
        logging.error(f"Unexpected error while validating {file_path}: {e}")
        return False

def read_excel(file_path):
    """Read an Excel file and return a Pandas DataFrame."""
    if not is_valid_xlsx(file_path):
        logging.error(f"Invalid .xlsx file: {file_path}")
        return None  # Return None if the file is invalid

    try:
        # Load Excel using pandas
        pandas_df = pd.read_excel(file_path, dtype=str, engine='openpyxl')
        return pandas_df
    except Exception as e:
        logging.error(f"Error while reading the Excel file {file_path}: {e}")
        return None

def generate_reports(df, output_dir):
    """Generate the data quality reports (both Excel and TXT)."""
    try:
        # Save the DataFrame to Excel
        excel_report_path = os.path.join(output_dir, 'data_quality_report.xlsx')
        df.to_excel(excel_report_path, index=False)
        logging.info(f"Excel report saved to {excel_report_path}")
        
        # Generate a text report of missing values
        missing_values = df.isnull().sum()
        text_report_path = os.path.join(output_dir, 'data_quality_report.txt')
        with open(text_report_path, 'w') as f:
            f.write("Data Quality Report\n")
            f.write("==================\n\n")
            f.write(f"Total rows: {len(df)}\n")
            f.write(f"Total columns: {len(df.columns)}\n\n")
            f.write("Missing values per column:\n")
            f.write(missing_values.to_string() + "\n\n")
            
            # Add percentage of missing values
            missing_percent = (missing_values / len(df) * 100).round(2)
            f.write("Percentage of missing values per column:\n")
            f.write(missing_percent.to_string() + "%\n\n")
            
            # Determine validation status
            threshold = 10  # If any column has more than 10% missing values, fail validation
            validation_failed = any(missing_percent > threshold)
            f.write(f"Status: {'FAILED' if validation_failed else 'PASSED'}\n")
            if validation_failed:
                f.write(f"Reason: One or more columns have more than {threshold}% missing values.\n")
        
        # Create a missing values specific report
        missing_df = pd.DataFrame({
            'Column': df.columns,
            'Missing Values': missing_values.values,
            'Missing Percentage': missing_percent.values
        })
        missing_report_path = os.path.join(output_dir, 'missing_values_report.xlsx')
        missing_df.to_excel(missing_report_path, index=False)
        logging.info(f"Missing values report saved to {missing_report_path}")
        
        logging.info(f"Text report saved to {text_report_path}")
    except Exception as e:
        logging.error(f"Error generating reports: {e}")
        raise

def main(input_file, output_dir):
    """Main function for validating the Excel file and processing it."""
    logging.info(f"Validating file: {input_file}")

    # Read Excel data
    pandas_df = read_excel(input_file)
    if pandas_df is None:
        logging.error("Exiting due to error while reading file.")
        return 1

    # Process the DataFrame
    logging.info(f"File loaded successfully with {len(pandas_df)} rows and {len(pandas_df.columns)} columns.")

    # Generate the reports (both Excel and text)
    generate_reports(pandas_df, output_dir)
    logging.info("Data validation completed successfully.")
    return 0

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Validate Excel data file and generate reports.')
    parser.add_argument('--input', default='input_data/test1.xlsx', help='Path to the input Excel file')
    parser.add_argument('--output', default='reports/', help='Directory to save reports')
    args = parser.parse_args()
    
    # Ensure the output directory exists
    if not os.path.exists(args.output):
        os.makedirs(args.output)
        logging.info(f"Created reports directory at {args.output}")

    exit_code = main(args.input, args.output)
    exit(exit_code)
    