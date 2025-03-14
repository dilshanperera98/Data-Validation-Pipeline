import zipfile
import pandas as pd
import os
import logging
import argparse
from openpyxl import load_workbook

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def is_valid_xlsx(file_path):
    """Check if the file is a valid .xlsx file using multiple methods."""
    if not os.path.isfile(file_path):
        logging.error(f"The file {file_path} does not exist.")
        return False

    # Method 1: Check ZIP structure
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            if 'xl/workbook.xml' not in zip_ref.namelist():
                logging.error(f"Invalid .xlsx structure in {file_path}")
                return False
    except zipfile.BadZipFile:
        logging.error(f"File is not a valid ZIP archive: {file_path}")
        return False
    except Exception as e:
        logging.error(f"ZIP validation error: {str(e)}")
        return False

    # Method 2: Try opening with openpyxl
    try:
        load_workbook(file_path).close()
        return True
    except Exception as e:
        logging.error(f"Openpyxl validation failed: {str(e)}")
        return False

def read_excel(file_path):
    """Read an Excel file and return a Pandas DataFrame."""
    if not is_valid_xlsx(file_path):
        logging.error(f"Invalid .xlsx file: {file_path}")
        return None

    try:
        return pd.read_excel(file_path, dtype=str, engine='openpyxl')
    except Exception as e:
        logging.error(f"Error reading Excel: {str(e)}")
        return None

def generate_reports(df, output_dir):
    """Generate data quality reports with enhanced validation."""
    try:
        # Excel Report
        excel_report_path = os.path.join(output_dir, 'data_quality_report.xlsx')
        df.to_excel(excel_report_path, index=False)
        logging.info(f"Excel report generated: {excel_report_path}")
        
        # Text Report
        text_report_path = os.path.join(output_dir, 'data_quality_report.txt')
        missing_values = df.isnull().sum()
        missing_percent = (missing_values / len(df) * 100).round(2)
        
        with open(text_report_path, 'w') as f:
            f.write("Comprehensive Data Quality Report\n")
            f.write("=================================\n\n")
            f.write(f"Dataset Dimensions: {len(df)} rows x {len(df.columns)} columns\n")
            f.write("\nMissing Values Analysis:\n")
            f.write(missing_values.to_string() + "\n")
            f.write("\nMissing Values Percentage:\n")
            f.write(missing_percent.to_string() + "%\n")
            
            # Validation Rules
            threshold = 10
            failed_columns = missing_percent[missing_percent > threshold]
            status = "FAILED" if not failed_columns.empty else "PASSED"
            
            f.write(f"\nValidation Status: {status}\n")
            if not failed_columns.empty:
                f.write("\nColumns Exceeding Threshold:\n")
                f.write(failed_columns.to_string() + "\n")

        # Missing Values Report
        missing_df = pd.DataFrame({
            'Column': df.columns,
            'Missing Values': missing_values.values,
            'Missing Percentage (%)': missing_percent.values
        })
        missing_report_path = os.path.join(output_dir, 'missing_values_report.xlsx')
        missing_df.to_excel(missing_report_path, index=False)
        
        logging.info(f"Reports generated successfully in {output_dir}")
        return True
    except Exception as e:
        logging.error(f"Report generation failed: {str(e)}")
        raise

def main(input_file, output_dir):
    """Main validation workflow with enhanced error handling."""
    try:
        logging.info(f"Starting validation for: {input_file}")
        
        if not os.path.exists(input_file):
            logging.error(f"Input file not found: {input_file}")
            return 1

        df = read_excel(input_file)
        if df is None:
            return 1

        logging.info(f"Data loaded successfully. Columns: {list(df.columns)}")
        
        if not generate_reports(df, output_dir):
            return 1
            
        return 0
    except Exception as e:
        logging.error(f"Critical error in main process: {str(e)}")
        return 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Advanced Data Validation Tool')
    parser.add_argument('--input', default='input_data/test1.xlsx', 
                       help='Input Excel file path')
    parser.add_argument('--output', default='reports/', 
                       help='Output directory for reports')
    args = parser.parse_args()
    
    os.makedirs(args.output, exist_ok=True)
    
    exit_code = main(args.input, args.output)
    exit(exit_code)