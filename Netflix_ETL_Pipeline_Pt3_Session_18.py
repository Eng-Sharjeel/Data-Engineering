"""
Importing Libraries/Modules below
"""
import logging
import pandas as pd
import chardet
import sys
import time
import os
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.exceptions import SnowparkSQLException


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
        self.output_file_path=r"E:\Aena Maryam\Work\AI_Datayard\Projects\Netflix_Recommendation_System\Files\reencoded_netflix_titles.csv"
        self.db_user='aenamaryam'
        self.db_password='Snowflake_0299'
        self.db_account='rx18818.central-india.azure'
        self.db_warehouse='COMPUTE_WH'
        self.db_database_name='TRAINING_DB'
        self.db_schema_name='NETFLIX_SC'
        self.db_role_name='ACCOUNTADMIN'
        self.db_table_name='netflix_dataset'

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


    def write_csv_data(self, encd:str):
        """
        Re-encodes the CSV file using the specified encoding and writes it to a new output file

        This function reads the content of a CSV file specified by self.input_file_path
        using the given encoding and writes the content to a new file specified by
        self.output_file_path in the same encoding. It handles common errors like
        missing file paths, file not found, permission issues and decoding errors.

        Args:
            encd (str): The encoding to be used for reading and writing the csv file.

        Returns:
            None
        """
        #Check if the input file path is provided; log a warning and return if not
        if not self.input_file_path:
            self._logger.warning("Input file path is empty. Returning from write_csv_data()\n")
            return
        #Check if the output file path is provided; log a warning and return if not
        elif not self.output_file_path:
            self._logger.warning("Output file path is empty. Returning from write_csv_data()\n")
            return
        try:
            #Open the input file in read mode with the specified encoding and error handling
            with open(self.input_file_path, 'r', encoding=encd, errors='replace') as infile:
                #Open the output file in write mode with the specified encoding
                with open(self.output_file_path, 'w', encoding=encd) as outfile:
                    #Iterate through each line of the input file and write it to the output file
                    for line in infile:
                        outfile.write(line)
        except FileNotFoundError:
            #This exception logs an error message if the input file is not found
            self._logger.error(f"File not found in write_csv_data(): {self.input_file_path}\n") 
        except PermissionError:
            #This exception logs an error message if permission to access the input or output file is denied
            self._logger.error(f"Permission denied to read or write file in write_csv_data(): {self.input_file_path}\n")
        except UnicodeDecodeError:
            #Logs an error message if there is an issue decoding the file content with the specified encoding
            self._logger.error(f"Error while reading the file in write_csv_data(): {encd}\n")
        except Exception as e:
            #Logs a critical error message for any unexpected exceptions
            self._logger.critical(f"Unexpected error in write_csv_data(): {e}\n")

    
    def extract_csv_data(self, encd:str):
        """
        Extracts data from a CSV file after re-encoding it

        This function first re-encodes the CSV file using the specified encoding by calling
        self.write_csv_data(encd). It then reads the re-encoded CSV file into pandas DataFrame
        and returns it. If an error occurs during the process, an appropriate error message
        is logged and an empty DataFrame is returned.

        Args:
            encd (str): The encoding to be used for re-encoding the CSV file
        Returns:
            pandas.DataFrame: The data from the CSV file as a DaraFrame or an
            empty DataFrame if an error occurs

        """
        try:
            #Calling the write_csv_data function below to write the re-encoded CSV file on output_file_path
            self.write_csv_data(encd)
            #Reads the re-encoded CSV file into a pandas DataFrame
            df=pd.read_csv(self.output_file_path)

            #Returns the DataFrame containing the CSV data
            return df
        except FileNotFoundError:
            #Logs an error message if the output file is not found
            self._logger.error(f"File not found in extract_csv_data(): {self.output_file_path}\n") 
        except Exception as e:
            #Logs a critical error message for any unexpected exceptions
            self._logger.critical(f"Unexpected error in extract_csv_data(): {e}\n")
        #Returns an empty DataFrame if an error occurs
        return pd.DataFrame

    
    def transform(self, df:pd.DataFrame) ->pd.DataFrame:
        """
        Performs data transformation on the given pandas DataFrame

        This function performs the following operations:
        1. Identifies duplicated rows in a DataFrame and logs a warning if any are found.
        2. Removes duplicated rows and logs the action.
        3. Drops columns with names starting with Unnamed.
        4. Checks for missing expected columns in dataframe and adds them with None values if they are absent.
        5. Identifies unexpected columns within dataframe, logs their presence, and integrates them into the schema dynamically.
        6. Handles rows with all null values by removing them.
        7. Converts date columns to a consistent format and columns of object datatype to string format.
        8. Replaces NaN values with None to maintain schema consistency.
        9. Returns the transformed DataFrame.

        Args:
            df (pd.DataFrame): The input DataFrame to transform.
        Returns:
            pd.DataFrame: The transformed DataFrame. Returns an empty DataFrame 
            if an error occurs during transformation.
        """
        try:
            #Identifies duplicated rows in the DataFrame and logs a warning if any duplicated rows are found
            df_dupes=df[df.duplicated(keep=False, subset=df.columns)]
            if not df_dupes.empty:
                #If duplicated rows are found, we go into this if condition block
                self._logger.warning(f"Duplicated rows found in transform():\n\n{df_dupes}\n\n")

                #Removes duplicated rows and logs the action
                df=df.drop_duplicates()
                self._logger.info("Duplicated rows removed in transform()\n")

            #Drops columns with names starting with Unnamed because they are unnecessary for us then it logs the action
            df_d=df.loc[:, ~df.columns.str.contains('^Unnamed')]

            #Defines the expected columns in the dataframe
            expected_cols=["show_id", "type", "title", "director", "cast", "country", "date_added", "release_year", "rating", "duration", "listed_in", "description"]
            
            #Identifies missing columns by comparing the columns of dataframe with the expected columns.
            missing_cols=[col for col in expected_cols if col not in df_d.columns]
            if not missing_cols:
                #Logs a message if no columns are missing
                self._logger.info("No column missing in dataframe in transform()\n\n")
            else:
                #Log a warning for missing columns and adds them to the DataFrame with None values
                #We are placing None as values for the missing columns so our schema does not get affected
                #while loading data into Snowflake. This is a dynamic syntax and logic error handling
                self._logger.warning(f"Extra columns found in csv file:\n\n{missing_cols}\m\m")
                for col in missing_cols:
                    df_d[col]=None

            # Identifies unexpected columns that are not part of the expected schema
            #We are also checking the unexpected columns incase client sends any csv file which has an additional schema
            #So, if we receive any extra columns, we will store them in unexpected_cols and log a warning
            unexpected_cols=[col for col in df_d.columns if col not in expected_cols]
            
            if unexpected_cols:
                # Log a warning for unexpected columns and update the expected schema dynamically
                self._logger.warning(f"Unexpected columns in dataframe in transform():\n\n{unexpected_cols}\n\n")
                #updating our expected_cols list with the new columns we have received in our file by using extend function of list
                expected_cols.extend(unexpected_cols)
                self._logger.info(f"Expected columns in transform():\n\n{expected_cols}\n\n")
                #Followng statement creates dataframe by including every column which matches the list of expected_cols if it also exists in the dataframe
                df_d=df_d[[col for col in expected_cols if col in df_d.columns]]
                
            # Identifies rows where all columns have null values    
            df_all_null=df_d[df_d.isnull().all(axis=1)]
            
            if not df_all_null.empty:
                # Logs a warning and remove rows with all null values
                self._logger.warning(f"Records with all columns null found in transform()\n\n{df_all_null}\n\n")
                #Dopping records which have null values throughout in all columns
                #Below dropna function will drop records with null values and how='all' inside this func. means those records where all columns are Null
                df_d=df_d.dropna(how='all')
                self._logger.info("Records with all columns null dropped in transform()\n\n:{df_all_null}\n\n")
            else:
                self._logger.info("No Records found with all columns null in transform()\n")

            # Logs current data types of all columns for review
            self._logger.info(f"Current Datatypes of all columns in transform():\n\n{df_d.dtypes}\n\n")

            # Converts date columns to a consistent 'YYYY-MM-DD' format
            #For mixed format columns, pandas can infer the format using errors='coerce'. 
            #So, we don't explicitly need to mention the existing date format of date columns inside our dataframe
            #dt.strftime('%Y-%m-%d') converts the format of all date columns inside dataframe to yyyy-mm-dd
            datecols=[col for col in df_d.columns if "date" in col.lower()]
            for col in datecols:
                df_d[col]=pd.to_datetime(df_d[col], errors='coerce').dt.strftime('%Y-%m-%d')
                
            # Logs data types after converting date columns
            self._logger.info(f"Current Datatypes of all columns in transform():\n\n{df_d.dtypes}\n\n")

            # Converts object columns to string type dynamically
            df_d[df_d.select_dtypes(include=['object']).columns]=df_d.select_dtypes(include=['object']).astype('string')
            #Release_year has a wrong datatype of int64. So, we will convert that as well to string
            df_d['release_year'] = df_d['release_year'].astype('string')
            self._logger.info(f"All datatypes converted to relevant types and formats in transform():\n\n{df_d.dtypes}\n\n{df_d.head()}\n\n")
            
            #We have a lot of NaN values in our df. NaN can cause issue while inserting records in our database. Hence, replace NaN with None(Null)
            df_d=df_d.map(lambda x:None if pd.isna(x) else x)

            # Logs the final transformed DataFrame
            df_t=df_d
            self._logger.info(f"NaN replaced with None in transform():\n\n{df_t.head()}\n\n")
            
            #Returns the transformed DataFrame
            return df_t
        except Exception as e:
            #Logs a critical error message for any unexpected exceptions
            self._logger.critical(f"Unexpected error in transform(): {e}\n")
        #Returns an empty DataFrame if an error occurs
        return pd.DataFrame

    
    def table_exists(self, session):
        """
        Checks if a specific table exists in the database schema.
        
        1. Define a SQL query to check the existence of a table in the database's INFORMATION_SCHEMA.TABLES.
        2. The query counts the number of entries in the TABLES information schema where:
        3. The schema name matches `self.db_schema_name` (case-insensitive comparison by converting to uppercase).
        4. The table name matches `self.db_table_name` (case-insensitive comparison by converting to lowercase).
        
        Args:
        session: The active database session used to execute SQL queries.
        Returns:
        bool: True if the table exists in the specified schema, False otherwise.
        """

        query=f"""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA=\'{self.db_schema_name.upper()}\'
        AND TABLE_NAME=\'{self.db_table_name.lower()}\'
        """
        #Executes the SQL query using the provided session and collect the results.
        result=session.sql(query).collect()
        #Returns True if the table exists (i.e., COUNT(*) > 0), otherwise return False.
        return result[0][0] > 0

    
    def load_to_snowflake(self, df:pd.DataFrame):
        """
        Loads data from a pandas DataFrame into a Snowflake table.
        
        Args:
        df (pd.DataFrame): The DataFrame to be loaded into Snowflake.
        
        This function performs the following steps:
        1. Logs the DataFrame's column names before processing.
        2. Creates a session to connect to Snowflake using the provided credentials and configuration.
        3. Checks if the target table exists in the database.
            - If it exists, compares the table's columns with the DataFrame's columns.
            - Adds any missing columns to the table.
        4. Loads the data into the Snowflake table, overwriting any existing data.
        5. Handles and logs exceptions during execution.
        6. Closes the Snowflake session.
        
        Exceptions:
        Logs critical errors for both Snowflake SQL exceptions and general exceptions.
        """
        try:
            #Log the DataFrame's column names before writing to Snowflake
            cols_list=df.columns.tolist()
            self._logger.info(f"Dataframe columns before writing in load_to_snowflake():\n{cols_list}\n")

            #Log a message before creating a Snowflake session
            self._logger.info("Before creating snowflake session in load_to_snowflake()\n")

            #Create a Snowflake session using provided credentials and configuration
            session=Session.builder.configs({
                "user":self.db_user,
                "password":self.db_password,
                "account":self.db_account,
                "warehouse":self.db_warehouse,
                "database":self.db_database_name,
                "schema":self.db_schema_name,
                "role":self.db_role_name
            }).create()
            self._logger.info("Snowflake session successfully created in load_to_snowflake()\n")

            #Check if the target table exists in Snowflake
            if self.table_exists(session):
                #Dynamically adding columns into the table so if schema of file changes, snowflake will also be updated with the same schema without any errors
                self._logger.info(f"Table  \"{self.db_table_name}\" exists in database in load_to_snowflake()\n")

                #Query the schema of the existing table
                query=f"DESCRIBE TABLE \"{self.db_table_name}\""
                table_schema=session.sql(query).collect()

                #Extract column names from the table schema
                table_cols=[row['name'].lower() for row in table_schema]
                self._logger.info(f"Table column in database in load_to_snowflake():\n{table_cols}\n")

                #Identify missing columns in existing table compared to the DataFrame
                missing_cols=[col for col in df.columns if col.lower() not in table_cols]
                if missing_cols:
                    #Log missing columns and add them to Snowflake table
                    self._logger.warning(f"Snowflake db table has missing columns in load_to_snowflake():\n{missing_cols}\n")
                    for col in missing_cols:
                        alter_table_query=f"ALTER TABLE \"{self.db_table_name}\" ADD COLUMN \"{col}\" STRING"
                        session.sql(alter_table_query).collect()
                        self._logger.info(f"Added columns in snowflake db in load_to_snowflake():\n{df.columns.tolist()}\n")
            #Measure the time taken to write the DataFrame to Snowflake
            start_time=time.time()

            #Write the DataFrame directly to Snowflake table, overwriting any existing data in the table
            session.write_pandas(
                df,
                table_name=self.db_table_name,
                overwrite=True
            )

            #Log the elapsed time for the data load operation
            elapsed_time=time.time()-start_time
            self._logger.info(f"Table created and data loaded into snowflake in {self.db_table_name} in {elapsed_time:2f} seconds in load_to_snowflake()\n")
        except SnowparkSQLException as e:
            #Log critical errors for Snowflake SQL exceptions
            self._logger.critical(f"Snowflake sql error in load_to_snowflake(): {e}\n")
        except Exception as e:
            #Log critical errors for general exceptions
            self._logger.critical(f"Exception occurred in load_to_snowflake(): {e}\n")
        finally:
            #Close the Snowflake session and log the action
            session.close()
            self._logger.info("Session closed successfully in load_to_snowflake()\n")
            
        
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

            #Sets pandas display options for better visualization of large DataFrames
            pd.set_option('display.max_rows', 20)
            pd.set_option('display.max_columns', None)

            #Detects the encoding of the input CSV File
            encd=self.detect_encoding()
            #Extracts data from CSV File into a pandas dataframe 
            df=self.extract_csv_data(encd)
            #Checks if the DataFrame is empty, logs a warning then exits if it is empty
            if df.empty:
                self._logger.warning("Dataframe is empty. Exiting netflix from run()\n")
                return
            else:
                #Transforms the dataframe to remove duplicates, drop unnamed columns and handle misising columns
                df_t=self.transform(df)
                sys.stdout.flush()
                sys.stderr.flush()
                self.load_to_snowflake(df_t)
        except Exception as e:
            #Logs a critical error message for any unexpected exception
            self._logger.critical(f"Exception occurred in run(): {e}\n")



if __name__ == '__main__':
    netflix=Netflix_ETL()
    netflix.run()