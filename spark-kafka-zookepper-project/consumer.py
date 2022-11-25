import json
from kafka import KafkaConsumer
import pickle
import time
import re
from pyspark.ml.linalg  import Vectors
from pyspark.ml.classification import DecisionTreeClassificationModel
import pandas as pd

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('spark-kafka').getOrCreate()



# initialize list of lists that we use to create a new pandas dataframe
data = [[0,0,0,0,0]]
  
# Create the pandas DataFrame
df = pd.DataFrame(data, columns=["Glucose","BloodPressure","Insulin","Bmi", "Age"])

# Load model
loaded_model = DecisionTreeClassificationModel.load('spark-kafka-module')


consumer = KafkaConsumer("topic1", bootstrap_servers='localhost:9092', group_id="consumertraitement")

for message in consumer:
    arguments = json.loads(message.value.decode())
    prediction_data = re.findall(r"[-+]?(?:\d*\.\d+|\d+)", arguments)
    res = [eval(i) for i in prediction_data]
    features=Vectors.dense(prediction_data)

    print("********************* TEST RESULT ********************")
    print("\n")
    prediction_result = loaded_model.predict(features)

    if prediction_result==1.0 :
       print(":( :( :( :( Your Test Result is  : Positive !!! :( :( :( :( ")
    else :
       print(":) :) :) :) Your Test Result is  : Negative :) :) :) :) ")

    print("\n")
    print("******************** STATISTICS ********************")
    print("\n")

    new_row = {'Glucose':res[0], 'BloodPressure':res[1], 'Insulin':res[2], 'Bmi':res[3],'Age':res[4]}
    df = df.append(new_row, ignore_index=True)
    
    print(df.describe())
    print("\n")
    print("\n")
    print("\n")
    print("\n")
