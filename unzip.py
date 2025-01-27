import os
import zipfile
import logging

# Configure simple logging to terminal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

def extract_zip_recursive(source_dir, output_dir):
    """
    Recursively extracts all .zip files from the source directory to the output directory.
    If a .zip file contains another .zip file, it will also be extracted.
    
    Args:
        source_dir (str): The directory containing the .zip files to extract.
        output_dir (str): The directory where the extracted files will be saved.
    """
    if not os.path.exists(source_dir):
        raise FileNotFoundError(f"Source directory {source_dir} does not exist.")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.info(f"Created output directory: {output_dir}")

    # Function to process a single zip file
    def process_zip(zip_path, current_output_dir):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            logging.info(f"Extracting {zip_path} into {current_output_dir}")
            zip_ref.extractall(current_output_dir)
            extracted_files = zip_ref.namelist()

        # Check if any of the extracted files are zips and process them
        for file_name in extracted_files:
            extracted_path = os.path.join(current_output_dir, file_name)
            if zipfile.is_zipfile(extracted_path):
                logging.info(f"Found nested zip: {extracted_path}")
                nested_output_dir = os.path.splitext(extracted_path)[0]  # Remove .zip from name
                if not os.path.exists(nested_output_dir):
                    os.makedirs(nested_output_dir)
                process_zip(extracted_path, nested_output_dir)
                # Once nested zip is processed, remove the zip file itself (don't want it in extracted)
                os.remove(extracted_path)
                logging.info(f"Deleted nested zip file: {extracted_path}")

    # Process all zip files in the source directory
    for root, _, files in os.walk(source_dir):
        for file in files:
            if file.endswith(".zip"):
                zip_path = os.path.join(root, file)
                relative_path = os.path.relpath(root, source_dir)
                current_output_dir = os.path.join(output_dir, relative_path)
                if not os.path.exists(current_output_dir):
                    os.makedirs(current_output_dir)
                process_zip(zip_path, current_output_dir)

    logging.info("Extraction process completed successfully.")


if __name__ == "__main__":
    # For standalone execution
    source_directory = "data"  # Directory containing zip files
    output_directory = "extracted"  # Directory to save extracted files
    try:
        logging.info("Starting extraction process...")
        extract_zip_recursive(source_directory, output_directory)
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
