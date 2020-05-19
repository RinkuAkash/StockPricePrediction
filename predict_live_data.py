import json
import time
import pandas as pd

from datetime import datetime
from flask import Flask, Response, render_template, make_response

from pyspark import SparkContext
from pyspark.sql import SQLContext
from kafka import KafkaConsumer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegressionModel


sc = SparkContext(appName="StockPricePrediction")
sqlCtx = SQLContext(sc)
vectorAssembler = VectorAssembler(inputCols=['open', 'high', 'low'], outputCol='features')
model = LinearRegressionModel.load('model')
def predict_price(vectorAssembler, model):
    consumer = KafkaConsumer('stockprices'
                             , bootstrap_servers=['localhost:9092']
                             , api_version=(0, 10)
                             , consumer_timeout_ms=3000
                            )
    for message in consumer:
        record = json.loads(message.value)
        series = pd.Series(record, dtype='float')
        dataframe = series.to_frame()
        dataframe = dataframe.T
        dataframe.columns = ['open', 'high', 'low', 'close', 'volumn']
        sdf = sqlCtx.createDataFrame(dataframe)
        vectored_dataframe = vectorAssembler.transform(sdf)
        vectored_dataframe = vectored_dataframe.select(['features', 'close'])
        predictions = model.transform(vectored_dataframe)
        predict_value = predictions.select('prediction').collect()[0].__getitem__("prediction")
        close_value = predictions.select('close').collect()[0].__getitem__('close')
        return round(predict_value, 2), close_value
    return 0,0


app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('index.html')


@app.route('/data', methods=['GET', 'POST'])
def data():
    stock_price, close_price = predict_price(vectorAssembler, model)

    data = [time.time() * 1000, stock_price, close_price]

    response = make_response(json.dumps(data))

    response.content_type = 'application/json'

    time.sleep(5)
    return response

if __name__=='__main__':
    app.run(host='127.0.0.1', port="5002", debug=True, threaded=True)

