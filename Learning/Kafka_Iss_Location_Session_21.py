import os
import sys
import datetime
import json
import pandas as pd
import urllib.parse
import requests
import logging
import pytz
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer, KafkaConsumer
from snowflake.snowpark import Session
from multiprocessing import Process


LOGGER_NAME="ISS_IN_SPACE"

class iss_in_space():
    def __init__(
        self
    ):
        self._logger=logging.getLogger(LOGGER_NAME)
        self.db_user = 'aenamaryam'
        self.db_password = 'Snowflake_0299'
        self.db_account = 'rx18818.central-india.azure'
        self.db_warehouse = 'COMPUTE_WH'
        self.db_database_name = 'TRAINING_DB'
        self.db_schema_name = 'TRAINING_SC'
        self.db_role_name = 'ACCOUNTADMIN'
        self.db_table_name = 'iss_realtime_location'
        self.issloc_api="http://api.open-notify.org/iss-now.json" #ISS Space Station Online Public API
        self.topic_name='iss_realtime_location'
        #For initializing kafkaadmin client, we need to provide, IP, Port against bootstrap_servers param 
        #and provide a string unique client id against client_id param
        self.admin_client = KafkaAdminClient(
            bootstrap_servers="localhost:9092",
            client_id="iss_realtime_location_topic_creator"
        )
        #Initializing producer below
        self.producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode('utf-8') #Serializing json data
        )
        #Initializing consumer below which prints data and shows events from beginning including the ones getting consumed in realtime
        self.print_consumer_obj = KafkaConsumer(
            self.topic_name,
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest", #To consume all events of topic being produced since beginning
            enable_auto_commit=True,
            group_id='iss_space_print_consumer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')), #Deserializing json data
            consumer_timeout_ms=2000 #Timeout set to 2000ms or 2s if no messages are found
        )
        #Initializing consumer below which loads the data generated in realtime into snowflake
        self.load_consumer_obj = KafkaConsumer(
            self.topic_name,
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest", #To consume all events of topic being produced since beginning
            enable_auto_commit=True,
            group_id='iss_space_load_consumer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')), #Deserializing json data
            consumer_timeout_ms=2000 #Timeout set to 2000ms or 2s if no messages are found
        )


    def get_realtime_location(self) -> dict:
        self._logger.info("Before fetching data from API in get_realtime_location()\n")
        try:
            if not self.issloc_api:
                self._logger.warning("The API is empty in get_realtime_location()\n")
                return
            else:
                self._logger.info(f"The API is fine in get_realtime_location():\n {self.issloc_api}\n")
                #Snce, it is realtime so we may get timeout error while printing response. So, I have set timeout =120 seconds
                response=requests.get(self.issloc_api, timeout=120)
                if response.status_code==200:
                    self._logger.info("API has been successfully fetched in get_realtime_location()\n")
                    data=response.json() #json is a structured format, fetching data from our response using json()
                    message=data.get('message','fail')
                    longitude=data['iss_position'].get('longitude', 0.0000)
                    latitude=data['iss_position'].get('latitude', 0.0000)
                    timestamp=datetime.datetime.utcfromtimestamp(data.get('timestamp', '0000-00-00 00:00:00')).strftime('%Y-%m-%d %H:%M:%S')
                    iss_dict={'message':message, 'longitude':longitude, 'latitude':latitude, 'timestamp':timestamp}
                    return iss_dict
                    
                else:
                    self._logger.error(f"API could not be fetched  in get_realtime_location() due to response status code: {response.status_code}\n")
                    return
        except Exception as e:
            self._logger.critical(f"Exception occurred in get_realtime_location(): {e}\n")

    
    def topic_exists(self) -> bool:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers="localhost:9092",
                client_id="iss_realtime_location_topic_checker"
            )
            self._logger.info("Admin_client successfully initialized in topic_exists()\n")
            #Fetching a list of topics inside kafka brokers
            topics=admin_client.list_topics()
            if self.topic_name in topics:
                self._logger.warning(f"The topic named {self.topic_name} already exists in topic_exists()\n")
                return True
            else:
                self._logger.info(f"The topic named {self.topic_name} does not exist in topic_exists(). Create it.\n")
                return False
        except Exception as e:
            self._logger.critical(f"Exception occurred while creating a topic in topic_exists(): {e}\n")
            return
            

    
    def create_topic(self):
        try:
            self._logger.info("Admin_client successfully initialized in create_topic()\n")
            #Assign topic name, no. of partitions and replication factor
            #topic_name='iss_realtime_location'
            num_partitions=3
            replication_factor=1
            #Create a new topic now
            new_topic=NewTopic(name=self.topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            self._logger.info("New_topic successfully assigned in create_topic()")
            #Creation of topic using following line of code
            admin_client.create_topics(new_topics=[new_topic], validate_only=False)
            self._logger.info(f"Topic named {self.topic_name} successfully created in create_topic() \n")
            return
        except Exception as e:
            self._logger.critical(f"Exception occurred while creating a topic in create_topic(): {e}\n")
            return

    
    def produce_topic_event(self, event_dict:dict):
        try:
            self._logger.info("Kafka producer initialized in produce_topic_event()\n")
            self.producer.send(self.topic_name, event_dict)
            self.producer.flush()
            self._logger.info(f"Event Produced successfully within topic {self.topic_name} in produce_topic_event()\n")
        except Exception as e:
            self._logger.critical(f"Exception occurred in produce_topic_event(): {e}\n")


    
    def print_consumer(self):
        try:
            self._logger.info("Kafka consumer print initialized in print_consumer()\n")
            #setting a 20 sec timeout for consumer because it keeps looking for messages within apache kafka even after reading all available messages
            #So, unless consumer is specifically told to stop listening, it will keep executing in loop
            timeout=5
            start_time=datetime.datetime.now()
            for data in self.print_consumer_obj:
                print(f"{data.value} in print_consumer()\n")
                if (datetime.datetime.now()-start_time).total_seconds() > timeout:
                    break
        except Exception as e:
            self._logger.critical(f"Exception occurred in print_consumer(): {e}\n")


    def table_exists(self, session):
        query = f"""
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = \'{self.db_schema_name.upper()}\'
        AND TABLE_NAME = \'{self.db_table_name.lower()}\'
        """
        result = session.sql(query).collect()
        return result[0][0] > 0  

    
    def db_load_consumer(self):
        try:
            self._logger.info("Kafka consumer load initialized in db_load_consumer()\n") 
            session = Session.builder.configs({
                "user":self.db_user,
                "password": self.db_password,
                "account": self.db_account,
                "warehouse": self.db_warehouse,
                "database": self.db_database_name,
                "schema": self.db_schema_name,
                "role": self.db_role_name
            }).create()
            self._logger.info("Snowflake session successfully created in db_load_consumer()\n")
            for data in self.load_consumer_obj:
                if self.table_exists(session):
                    self._logger.info(f"Table \"{self.db_table_name}\" already exists in snowflake in db_load_consumer()\n")
                    insert_query=f"""
                    INSERT INTO \"{self.db_database_name.upper()}\".\"{self.db_schema_name.upper()}\".\"{self.db_table_name.lower()}\" (
                    message, longitude, latitude, timestamp)
                    VALUES(
                    '{data.value['message']}',
                    '{data.value['longitude']}',
                    '{data.value['latitude']}',
                    '{data.value['timestamp']}'
                    )
                    """
                    result=session.sql(insert_query).collect()
                    self._logger.info(f"Data successfully inserted in \"{self.db_table_name}\" in snowflake in db_load_consumer()\n")
                else:
                    self._logger.info(f"Table \"{self.db_table_name}\" needs to be created in snowflake in db_load_consumer()\n")
                    create_query=f"""
                    CREATE TABLE \"{self.db_database_name.upper()}\".\"{self.db_schema_name.upper()}\".\"{self.db_table_name.lower()}\"(
                    message varchar(10),
                    longitude float,
                    latitude float,
                    timestamp datetime
                    )
                    """
                    result=session.sql(create_query).collect()
                    self._logger.info(f"Table \"{self.db_table_name}\" successfully created in snowflake in db_load_consumer()\n")
                
                    insert_query=f"""
                    INSERT INTO \"{self.db_database_name.upper()}\".\"{self.db_schema_name.upper()}\".\"{self.db_table_name.lower()}\" (
                    message, longitude, latitude, timestamp)
                    VALUES(
                    '{data.value['message']}',
                    '{data.value['longitude']}',
                    '{data.value['latitude']}',
                    '{data.value['timestamp']}'
                    )
                    """
                    result=session.sql(insert_query).collect()
                    self._logger.info(f"Data successfully inserted in \"{self.db_table_name}\" in snowflake in db_load_consumer()\n")
        except Exception as e:
            self._logger.critical(f"Exception occurred in db_load_consumer(): {e}\n")
        finally:
            session.close()
            self._logger.info("Session closed successfully in db_load_consumer()\n")

    
    def run(self):
        try:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            self._logger.info("Starting iss space station realtime tracking now in run()\n")
            stop=False
            while stop==False:
                iss_dict=self.get_realtime_location()
                if not iss_dict:
                    self._logger.warning("ISS realtime space location and data dictionary is empty in run()\n")
                    return
                else:
                    self._logger.info(f"ISS data dictionary in run():\n{iss_dict}\n")
                    if self.topic_exists()==False:
                        self.create_topic()
                    self.produce_topic_event(iss_dict)
                    self.print_consumer()
                    self.db_load_consumer()
        except Exception as e:
            self._logger.critical(f"Exception occurred in run(): {e}\n")
        
        


if __name__=='__main__':
    iss=iss_in_space()
    iss.run()