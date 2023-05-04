from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random
import pandas as pd

KAFKA_TOPIC_NAME_CONS = "orderstopicdemo"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'
if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")
    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,value_serializer=lambda x: dumps(x).encode('utf-8'))
file_path = "/home/vagrant/orders.csv"
orders_pd_df=pd.read_csv(file_path)
print(orders_pd_df.head(1))
orders_list = orders_pd_df.to_dict(orient="records")
print(orders_list[0])
for order in orders_list:
    message=order
    print("Message to be sent: ", message)
    kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
    time.sleep(1)
# print("Breaking the loop")
# break
