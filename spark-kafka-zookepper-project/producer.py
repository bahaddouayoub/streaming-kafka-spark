from kafka import KafkaProducer
import json
import csv
import time



producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
target_topic = "topic1"

with open('data.csv') as file_obj:
    heading = next(file_obj)
    reader_obj = csv.reader(file_obj)
    for row in reader_obj:
        time.sleep(6)
        data=[]
        for j in row:
            data.append(j)
          #print(j)

        print(data)
        print("Sent Successfuly !!!")
        print("\n")
        print("\n")
        producer.send(target_topic,json.dumps(data))
        producer.flush(timeout=10) # this forcibly sends any messages that are stuck.
producer.close(timeout=5)





