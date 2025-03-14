import zipfile
import pandas as pd
import os
import logging

# Set up logging
logging.basicConfig(level=logging.WARN, format='%(asctime)s - %(levelname)s - %(message)s')

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

def main(input_file):
    """Main function for validating the Excel file and processing it."""
    logging.info(f"Validating file: {input_file}")

    # Read Excel data
    pandas_df = read_excel(input_file)
    if pandas_df is None:
        logging.error("Exiting due to error while reading file.")
        return

    # Process the DataFrame (this is just an example, adjust it as needed)
    logging.info(f"File loaded successfully with {len(pandas_df)} rows.")

    # You can add your logic for processing the DataFrame here (e.g., checking for missing values)
    missing_values = pandas_df.isnull().sum()
    logging.info(f"Missing values per column: {missing_values}")

    # Output your report (example of saving a report)
    output_report_path = "reports/data_quality_report.xlsx"
    pandas_df.to_excel(output_report_path, index=False)
    logging.info(f"Data quality report saved to {output_report_path}")

if __name__ == "__main__":
    # Example input file path, you can adjust this as needed
    input_file = 'input_data/test1.xlsx'
    main(input_file)
