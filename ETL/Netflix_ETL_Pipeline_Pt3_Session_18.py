import logging
import pandas as pd
import chardet
import sys
import time
import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.exceptions import SnowparkSQLException

LOGGER_NAME = "Netflix_ETL"

class Netflix_ETL:
    def __init__(self):
        self._logger = logging.getLogger(LOGGER_NAME)
        self.input_file_path = r"LOCATION_OF_FILE_IN_YOU_COMPUTER"
        self.output_file_path = r"LOCATION_OF_FILE_IN_YOU_COMPUTER\reencoded_FILE_NAME.csv"
        self.db_user = 'YOUR_DB_USERNAME'
        self.db_password = 'YOUR_SNOWFLAKE_DB_PASS'
        self.db_account = 'YOUR_SNOWFLAKE_DB_ACC'
        self.db_warehouse = 'YOUR_SNOWFLAKE_DB_WAREHOUSE'
        self.db_database_name = 'YOUR_SNOWFLAKE_DB_NAME'
        self.db_schema_name = 'YOUR_SNOWFLAKE_DB_SCHEMA_NAME' 
        self.db_role_name = 'YOUR_SNOWFLAKE_DB_ROLE'
        self.db_table_name = 'YOUR_SNOWFLAKE_DB_TABLE_NAME'
    # -------------------- Detect Encoding --------------------
    def detect_encoding(self):
        """
        Detects encoding using chardet library
        Reads a portion of content (10,000 bytes)
        """
        if not self.input_file_path:
            self._logger.warning("Input file path is empty. Returning from detect_encoding()")
            return None

        try:
            with open(self.input_file_path, "rb") as f:
                result = chardet.detect(f.read(10000))

            self._logger.info(f"Detected encoding: {result['encoding']}")
            return result["encoding"]

        except FileNotFoundError:
            self._logger.error(f"File not found: {self.input_file_path}")
        except PermissionError:
            self._logger.error(f"Permission denied: {self.input_file_path}")
        except Exception as e:
            self._logger.critical(f"Unexpected error in detect_encoding(): {e}")

        return None

    # -------------------- Write CSV --------------------
    def write_csv_data(self, encd: str):
        if not self.input_file_path:
            self._logger.warning("Input file path is empty. Returning from write_csv_data()")
            return

        if not self.output_file_path:
            self._logger.warning("Output file path is empty. Returning from write_csv_data()")
            return

        try:
            with open(self.input_file_path, "r", encoding=encd, errors="replace") as infile:
                with open(self.output_file_path, "w", encoding=encd) as outfile:
                    for line in infile:
                        outfile.write(line)

            self._logger.info("CSV file successfully re-encoded and written")

        except FileNotFoundError:
            self._logger.error(f"File not found: {self.input_file_path}")
        except PermissionError:
            self._logger.error("Permission denied while reading/writing CSV files")
        except UnicodeDecodeError:
            self._logger.error(f"Unicode decode error with encoding: {encd}")
        except Exception as e:
            self._logger.critical(f"Unexpected error in write_csv_data(): {e}")

    # -------------------- Extract --------------------
    def extract_csv_data(self, encd: str) -> pd.DataFrame:
        try:
            self.write_csv_data(encd)
            df = pd.read_csv(self.output_file_path)
            self._logger.info("CSV data successfully extracted into DataFrame")
            return df

        except FileNotFoundError:
            self._logger.error(f"File not found: {self.output_file_path}")
        except Exception as e:
            self._logger.critical(f"Unexpected error in extract_csv_data(): {e}")

        return pd.DataFrame()

    # -------------------- Transform --------------------
    def transform_csv_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Performs transformations on Netflix CSV data
        """
        try:
            # 1. Identify duplicate rows
            df_dupes = df[df.duplicated(keep=False)]
            if not df_dupes.empty:
                self._logger.warning(f"Duplicate rows found:\n{df_dupes}")
                df = df.drop_duplicates()
                self._logger.info("Duplicate rows removed")

            # 2. Drop unnamed columns
            df_d = df.loc[:, ~df.columns.str.contains("Unnamed", case=False)]

            # 3. Expected columns
            expected_cols = [
                "show_id", "type", "title", "director", "cast", "country",
                "date_added", "release_year", "rating", "duration",
                "listed_in", "description"
            ]

            # 4. Add missing columns
            missing_cols = [col for col in expected_cols if col not in df_d.columns]
            if missing_cols:
                self._logger.warning(f"Missing columns found: {missing_cols}")
                for col in missing_cols:
                    df_d[col] = None
            else:
                self._logger.info("No missing columns found")

            # 5. Handle unexpected columns
            unexpected_cols = [col for col in df_d.columns if col not in expected_cols]
            if unexpected_cols:
                self._logger.warning(f"Unexpected columns found: {unexpected_cols}")
                expected_cols.extend(unexpected_cols)
                df_d = df_d[expected_cols]

            # 6. Drop rows where all columns are null
            df_all_null = df_d[df_d.isnull().all(axis=1)]
            if not df_all_null.empty:
                self._logger.warning(f"Rows with all NULL values:\n{df_all_null}")
                df_d = df_d.dropna(how="all")
                self._logger.info("Rows with all NULL values dropped")

            # 7. Log data types
            self._logger.info(f"Data types before transformation:\n{df_d.dtypes}")

            # 8. Convert date columns to YYYY-MM-DD
            date_cols = [col for col in df_d.columns if "date" in col.lower()]
            for col in date_cols:
                df_d[col] = (
                    pd.to_datetime(df_d[col], errors="coerce")
                    .dt.strftime("%Y-%m-%d")
                )

            self._logger.info(f"Data types after date conversion:\n{df_d.dtypes}")

            # 9. Converts object columns to string type dynamically
            df_d[df_d.select_dtypes(include=['object']).columns] = df_d.select_dtypes(include = [object]).astype('string')
            # obj_cols = df_d.select_dtypes(include=['object']).columns
            # df_d.loc[:, obj_cols] = df_d[obj_cols].astype('strings')
            

            # 10. release year has datatype of int64, we convert it into string
            if 'release_year' in df_d.columns:
                df_d['release_year'] = df_d['release_year'].astype('string')
            self._logger.info(f"All datatypes converted into relevant data-types and format in transform_csv_data():\n\n {df_d.dtypes}\n\n {df_d.head()}\n")
            
            # 11. Replace NaN with None
            # df_d=df_d.map(lambda x:None if pd.isna(x) else x)
            df_d = df_d.where(pd.notnull(df_d), None)

            df_t=df_d
            self._logger.info("Transformation completed successfully")
            return df_t

        except Exception as e:
            self._logger.critical(f"Unexpected error in transform_csv_data(): {e}")
            return pd.DataFrame()
    
    # -------------------- Table Exists Check --------------------
    def table_exists(self, session):
        """
        1. Define SQL Query to check existence of table in database's INFORMATION_SCHEMA.TABLES
        2. The query counts the number of entries in the TABLES information schema where:
           The schema name matches `self.db_schema_name` (case-insensitive comparison by converting to uppercase).
           The table name matches `self.db_table_name` (case-insensitive comparison by converting to lowercase).
        """
        query = f"""
        SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
        WHERE
        TABLE_SCHEMA = \'{self.db_schema_name.upper()}\'
        AND
        TABLE_NAME = \'{self.db_table_name}\'
        """
        # Executes the SQL query using the provided session and collect the results.
        result = session.sql(query).collect()
        # Returns True if the table exists (i.e., COUNT(*) > 0), otherwise return False.
        return result[0][0] > 0
    
    # -------------------- Load --------------------
    def load_to_snowflake(self, df:pd.DataFrame):
        try:
            # log dataFrame columns name before writing into SnowFlake
            cols_list = df.columns.tolist()
            self._logger.info(f"Dataframe columns before writing in load_to_snowflake():\n {cols_list}\n")

            self._logger.info("Before creating snowflake session in load_to_snowflake()\n")

            # creating snowflake session
            session = Session.builder.configs({
                "user" : self.db_user,
                "password" : self.db_password,
                "account": self.db_account,
                "warehouse": self.db_warehouse,
                "database": self.db_database_name,
                "schema": self.db_schema_name,
                "role": self.db_role_name
            }).create()
            self._logger.info("SnowFlake session successfully created in load_to_snowflake()\n")

            if self.table_exists(session):
                self._logger.info(f"Table \"{self.db_table_name}\" exists in database in load_to_snowflake()\n ")

                # query for schema of existing table
                query = f"DESCRIBE TABLE \"{self.db_table_name}\""
                table_schema = session.sql(query).collect()

                # extract columns name from table schema
                table_cols = [row['name'].lower() for row in table_schema]
                self._logger.info(f"Table column in database in load_to_snowflake(): \n {table_cols} \n")

                # identify missing cols in existing table compared to DataFrame
                missing_cols = [col for col in df.columns if col.lower() not in  table_cols]
                if missing_cols:
                    self._logger.warning(f"Snowflake DB table has missing columns in load_to_snowflake(): \n {missing_cols} \n")
                    for col in missing_cols:
                        alter_table_query = f"ALTER TABLE \"{self.db_table_name}\" ADD COLUMN \"{col}\" STRING"
                        session.sql(alter_table_query).collect()
                        self._logger.info(f"Added columns in snowflake db in load_to_snowflake():\n {df.columns.tolists()} \n")
            
            # measure start time to write dataframe to snowFlake cloud
            start_time = time.time()

            # write dataframe directly to snowflake table, overwriting any existing data in table
            session.write_pandas(
                df,
                table_name = self.db_table_name,
                auto_create_table=True,
                overwrite = True
            )

            # time to load data into snowflake
            elapsed_time = time.time() - start_time
            self._logger.info(f"Table created and data loaded into snowflake in {self.db_table_name} in {elapsed_time:2f} seconds in load_to_snowflake() \n")

        except SnowparkSQLException as e:
            self._logger.critical(f"Snowflake sql error in load_to_snowflake(): {e}\n")
        except Exception as e:
            self._logger.critical(f"Unexpected error occured in load_to_snowflake(): {e}\n")
        finally:
            # close snowflake session
            session.close()
            self._logger.info("Session closed successfully in load_to_snowflake()\n")
                        

    # -------------------- Run --------------------
    def run(self):
        try:
            logging.basicConfig(
                level = logging.INFO,
                format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            self._logger.info("Started Netflix ETL Pipeline in run()\n")

            # set pandas display for better visualization of large dataFrames
            pd.set_option('display.max_rows', 20)
            pd.set_option('display.max_columns', None)
            
            #Detects the encoding of the input CSV File by calling detect_encoding function
            encd = self.detect_encoding()

            #Extracts data from CSV File into a pandas dataframe by calling extract_csv_data function
            df = self.extract_csv_data(encd)

            # check if dataFrame is empty
            if df.empty:
                self._logger.warning("DataFrame is empty. Exiting Netflix from run()\n")
                return
            else: 
                # Now transform the dataFrame by calling transform_csv_data dunction
                df_t = self.transform_csv_data(df)
                sys.stdout.flush()
                sys.stderr.flush()
                self.load_to_snowflake(df_t)
        except Exception as e:
                self._logger.critical(f"Unexpected Error Occurs in run(): {e}\n")

if __name__ == '__main__':
    netflix = Netflix_ETL()
    netflix.run()
