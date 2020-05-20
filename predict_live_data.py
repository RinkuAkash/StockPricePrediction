# importing required libraries
import json
import time
import pandas as pd

from datetime import datetime
from flask import Flask, render_template, make_response

from pyspark import SparkContext
from pyspark.sql import SQLContext
from kafka import KafkaConsumer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql.types import DoubleType

# creating spark context
sc = SparkContext(appName="StockPricePrediction")
sqlContext = SQLContext(sc)

# vector Assembler is to transform values to fit in model
vectorAssembler = VectorAssembler(inputCols=['open', 'high', 'low'], outputCol='features')
model = LinearRegressionModel.load('model') # loading machine learning model


# function to get predicted stock price from kafka
def predict_price(vectorAssembler, model):
    consumer = KafkaConsumer('stockprices'
                             , bootstrap_servers=['localhost:9092']
                             , api_version=(0, 10)
                             , consumer_timeout_ms=3000
                            )
    for message in consumer:

        record = json.loads(message.value)

        series = pd.Series(record, dtype=float) # message.value contains key value pair that support to convert into pandas series
        dataframe = series.to_frame()
        dataframe = dataframe.T
        dataframe.columns = ['open', 'high', 'low', 'close', 'volumn'] # modifying columns names for easy access

        spark_df = sqlContext.createDataFrame(dataframe) # creating spark dataframe

        vectored_dataframe = vectorAssembler.transform(spark_df)
        vectored_dataframe = vectored_dataframe.select(['features', 'close'])

        predictions = model.transform(vectored_dataframe) # predictions on real time data

        predict_value = predictions.select('prediction').collect()[0].__getitem__("prediction") # predicted value from dataframe
        close_value = predictions.select('close').collect()[0].__getitem__('close') # close value from dataframe
        date_time = message.key.decode('utf-8') # decoding datetime from key in message

        return round(predict_value, 2), round(close_value, 2), date_time

# To avoid errors after emptying message in kafka consumer server following values are returned
    predict_value = 0
    close_value = 0
    date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return predict_value, close_value, date_time


# creating flask application
app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')


# url data to get data on index page
@app.route('/data', methods=['GET', 'POST'])
def data():
    stock_price, close_price, date_time = predict_price(vectorAssembler, model)

    date_time = int(datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S').strftime('%s')) * 1000

    data = [date_time, stock_price, close_price]

    response = make_response(json.dumps(data))

    response.content_type = 'application/json'

    time.sleep(2)
    return response
