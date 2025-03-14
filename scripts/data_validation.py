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
            f.write("Missing values per column:\n")
            f.write(missing_values.to_string())
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
        return

    # Process the DataFrame (this is just an example, adjust it as needed)
    logging.info(f"File loaded successfully with {len(pandas_df)} rows.")

    # Generate the reports (both Excel and text)
    generate_reports(pandas_df, output_dir)

if __name__ == "__main__":
    # Example input file path, you can adjust this as needed
    input_file = 'input_data/test1.xlsx'
    output_dir = 'reports/'
    
    # Ensure the output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Created reports directory at {output_dir}")

    main(input_file, output_dir)
