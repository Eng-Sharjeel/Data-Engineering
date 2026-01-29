<<<<<<< HEAD
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
    def __init__(self):
        self._logger=logging.getLogger(LOGGER_NAME)
        self.issloc_api="http://api.open-notify.org/iss-now.json"
        self.topic_name="iss_realtime_location"
        
        #Intializing kafka admin client in order to create topic later
        self.admin_client=KafkaAdminClient(
            bootstrap_servers="localhost:9092",
            client_id="iss_realtime_location_topic_creator"
        )

        #Initialize producer
        self.producer=KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v:json.dumps(v).encode('utf-8')
        )

    def topic_exists(self)->bool:
        try:
            topics=self.admin_client.list_topics()
            if self.topic_name in topics:
                self._logger.warning(f"The topic named {self.topic_name} already exists in topic_exists()\n")
                return True
            else:
                self._logger.warning(f"The topic named {self.topic_name} does not exist in topic_exists(). Create it.\n")
                return False
        except Exception as e:
            self._logger.critical(f"Exception occurred while checking the list of topics in topic_exists(): {e}\n")

    
    def create_topic(self):
        try:
            num_partitions=3
            replication_factor=1
            new_topic=NewTopic(name=self.topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            self._logger.info("New topic successfully assigned in new_topic create_topic()\n")
            self.admin_client.create_topics(new_topic=[new_topic], validate_only=False)
            self._logger.info(f"Topic named {self.topic_name} sucessfully created in create_topic()\n")
            return
        except Exception as e:
            self._logger.critical(f"Exception occurred while creating a topic in create_topic(): {e}\n")
            return


    def get_realtime_location(self):
        self._logger.info("Before fetching data from API in get_realtime_location()\n")
        try:
            if not self.issloc_api:
                self.logger.warning("The API variable is empty in get_realtime_location()\n")
                return
            else:
                response=requests.get(self.issloc_api, timeout=120)
                if response.status_code==200:
                    self._logger.info("API has been successfully fetched in get_realtime_location()\n")
                    data=response.json()
                    print(data)
                    message=data.get('message', 'fail')
                    longitude=data['iss_position'].get('longitude', 0.0000)
                    latitude=data['iss_position'].get('latitude', 0.0000)
                    timestamp=datetime.datetime.utcfromtimestamp(data.get('timestamp', '0000-00-00 00:00:00')).strftime('%Y-%m-%d %H:%M:%S')
                    iss_dict={'message':message, 'longitude':longitude, 'latitude':latitude, 'timestamp':timestamp}
                    return iss_dict
                else:
                    self._logger.error(f"API could not be fetched in get_realtime_locaiton() due to response status code: {response.status_code}\n")
                    return
        except Exception as e:
            self._logger.critical(f"Exception occurred in get_realtime_location(): {e}\n")

    def produce_topic_event(self, event_dict:dict):
        try:
            self._logger.info("Kafka producer initialized in produce_topic_event()\n")
            self.producer.send(self.topic_name, event_dict)
            self.producer.flush()
            self._logger.info(f"Event produced successfully within topic {self.topic_name} in produce_topic_event()\n")
        except Exception as e:
            self._logger.critical(f"Exception occurred in produce_topic_event(): {e}\n")

    def run(self):
        try:
            logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
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
                        print(self.topic_exists())
                        self.create_topic()
                    print(self.topic_exists())
                    self.produce_topic_event(iss_dict)
        except Exception as e:
            self._logger.critical(f"Exception occurred in run(): {e}\n")

if __name__=='__main__':
    iss=iss_in_space()
    iss.run()

=======
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
    def __init__(self):
        self._logger=logging.getLogger(LOGGER_NAME)
        self.issloc_api="http://api.open-notify.org/iss-now.json"
        self.topic_name="iss_realtime_location"
        
        #Intializing kafka admin client in order to create topic later
        self.admin_client=KafkaAdminClient(
            bootstrap_servers="localhost:9092",
            client_id="iss_realtime_location_topic_creator"
        )

        #Initialize producer
        self.producer=KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v:json.dumps(v).encode('utf-8')
        )

    def topic_exists(self)->bool:
        try:
            topics=self.admin_client.list_topics()
            if self.topic_name in topics:
                self._logger.warning(f"The topic named {self.topic_name} already exists in topic_exists()\n")
                return True
            else:
                self._logger.warning(f"The topic named {self.topic_name} does not exist in topic_exists(). Create it.\n")
                return False
        except Exception as e:
            self._logger.critical(f"Exception occurred while checking the list of topics in topic_exists(): {e}\n")

    
    def create_topic(self):
        try:
            num_partitions=3
            replication_factor=1
            new_topic=NewTopic(name=self.topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
            self._logger.info("New topic successfully assigned in new_topic create_topic()\n")
            self.admin_client.create_topics(new_topic=[new_topic], validate_only=False)
            self._logger.info(f"Topic named {self.topic_name} sucessfully created in create_topic()\n")
            return
        except Exception as e:
            self._logger.critical(f"Exception occurred while creating a topic in create_topic(): {e}\n")
            return


    def get_realtime_location(self):
        self._logger.info("Before fetching data from API in get_realtime_location()\n")
        try:
            if not self.issloc_api:
                self.logger.warning("The API variable is empty in get_realtime_location()\n")
                return
            else:
                response=requests.get(self.issloc_api, timeout=120)
                if response.status_code==200:
                    self._logger.info("API has been successfully fetched in get_realtime_location()\n")
                    data=response.json()
                    print(data)
                    message=data.get('message', 'fail')
                    longitude=data['iss_position'].get('longitude', 0.0000)
                    latitude=data['iss_position'].get('latitude', 0.0000)
                    timestamp=datetime.datetime.utcfromtimestamp(data.get('timestamp', '0000-00-00 00:00:00')).strftime('%Y-%m-%d %H:%M:%S')
                    iss_dict={'message':message, 'longitude':longitude, 'latitude':latitude, 'timestamp':timestamp}
                    return iss_dict
                else:
                    self._logger.error(f"API could not be fetched in get_realtime_locaiton() due to response status code: {response.status_code}\n")
                    return
        except Exception as e:
            self._logger.critical(f"Exception occurred in get_realtime_location(): {e}\n")

    def produce_topic_event(self, event_dict:dict):
        try:
            self._logger.info("Kafka producer initialized in produce_topic_event()\n")
            self.producer.send(self.topic_name, event_dict)
            self.producer.flush()
            self._logger.info(f"Event produced successfully within topic {self.topic_name} in produce_topic_event()\n")
        except Exception as e:
            self._logger.critical(f"Exception occurred in produce_topic_event(): {e}\n")

    def run(self):
        try:
            logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
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
                        print(self.topic_exists())
                        self.create_topic()
                    print(self.topic_exists())
                    self.produce_topic_event(iss_dict)
        except Exception as e:
            self._logger.critical(f"Exception occurred in run(): {e}\n")

if __name__=='__main__':
    iss=iss_in_space()
    iss.run()

>>>>>>> 0e0acac (Add MovieLens ELT dbt project with Netflix folder, README, diagram, and setup scripts)
    