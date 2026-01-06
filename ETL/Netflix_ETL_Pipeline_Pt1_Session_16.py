"""
Importing Libraries/Modules below
"""
import logging
import pandas as pd
import chardet
import sys


"""
Initializing Global Constant LOGGER_NAME underneath
"""
LOGGER_NAME='netflix_etl'


"""
Creating a Class named Netflix_ETL below
which has its attributes initialized inside init constructor
It further has methods/functions created inside class.
Whenever we want to access any attribute or method of class inside itself,
we self. with that attribute or method name
"""
class Netflix_ETL:
    def __init__(self):
        """ Initialize attributes like logging and file paths below"""
        self._logger=logging.getLogger(LOGGER_NAME)
        self.input_file_path=r"E:\Aena Maryam\Work\AI_Datayard\Projects\Netflix_Recommendation_System\Files\netflix_titles.csv"

    def detect_encoding(self):
        """
        Detects the file encoding of input file using chardet library
        
        This function attempts to open a file specified by self.input_file_path
        and analyze its encoding by reading a portion of its content (10,000 bytes).
        If successful, it logs the detected encoding and returns it. If the file path
        is invalid or error occurs, appropriate warnings or error messages are logged
        
        Returns:
            str: The detected encoding of the file if successful or None if an error occurs
        """
        if not self.input_file_path:
            self._logger.warning("Input file path is empty. Returning from detect_encoding()\n")
            return None
        try:
            #Attemps to open the file in binary mode below and read up to 10,000 bytes for encoding detection
            with open(self.input_file_path, 'rb') as f:
                result=chardet.detect(f.read(10000))

            #Logs the detected encoding and returns it
            self._logger.info(f"Encoding of the csv file in detect_encoding() is: {result['encoding']}\n")
            return result['encoding']
        except FileNotFoundError:
            #This exception logs the error message if the file is not found
            self._logger.error(f"File not found in detect_encoding(): {self.input_file_path}\n")
        except PermissionError:
            #This exception logs an error message if permission to read the file is denied
            self._logger.error(f"Permission denied to read file in detect_encoding(): {self.input_file_path}\n")
        except Exception as e:
            #This exception logs a critical error message for any unexpected exception
            self._logger.critical(f"Unexpected error in detect_encoding(): {e}\n")

        #Returns None if an error occurred
        return None


    def run(self):
        """
        Performs the Netflix ETL Process by loading, processing and transforming a CSV File into a pandas DataFrame

        This function executes the following steps
        1. Configures logging settings
        2. Detects the encoding of the input CSV File
        3. Extracts the CSV Data into Pandas DataFrame
        4. Transforms the DataFrame by removing duplicates, dropping unnecessary columns
           and ensuring that expected columns exist
        5. Logs the transformed DataFrame or warnings if the DataFrame is empty.

        Returns:
            None
        """
        try:
            #Configure logging to include timestamp, logger name and log level
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            #Logs the start of the ETL Process
            self._logger.info("Started netflix etl in run()\n")

            #Detects the encoding of the input CSV File
            encd=self.detect_encoding()
        except Exception as e:
            #Logs a critical error message for any unexpected exception
            self._logger.critical(f"Exception occurred in run(): {e}\n")


if __name__ == '__main__':
    netflix=Netflix_ETL()
    netflix.run()