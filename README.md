# StockPricePrediction
The purpose of this project is to predict stock price using real time data. In this project I used past Google stock prices and
built machine learning model using pyspark ml library and predicted stock price. I used alphvantage services, which allows to
get real time stock data. Predicted data is portrayed on web page using Flask on AWS EC2 instance.

## Getting Started
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.
See deployment for notes on how to deploy the project on a live system.

### Prerequisites
* Download [kafka](https://kafka.apache.org/)
* Create api key on [alphavantage](https://www.alphavantage.co/)

### Installation
Install python package manager pip
```bash
sudo apt-get install python3-pip
```
Install required libraries from requirements.txt
```bash
pip3 install -r requirements.txt
```

### Setup Environment
Add following to bashrc file
```bash
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=python3
```

### Steps
1. File to AWS S3
    * Download historical stock data from [nasdaq](https://www.nasdaq.com/market-activity/stocks/goog)
    * Create file aws_credentials with aws credentials
    * Run script to create s3 bucket and upload file
 
2. Train Model
    * Copy aws sdk jars (i.e., hadoop-aws-2.7.7.jar, aws-java-sdk-1.7.4.jar from hadoop folder to spark jars folder
    * Run script, check performance of model and save model

3. Run Kafka Server
    In kafka directory run following commands to start zookeeper and kafka
    ```bash
    bin/zookeeper-server-start.sh config/zookeeper.properties
    ```
    ```bash
    bin/kafka-server-start.sh config/server.properties
    ```

4. Run kafka producer
    * live_data file consists of kafka producer which sends real time data from alpha vantage to kafka server

5. Run app.py
    * predict_live_data.py consists kafka consumer code that takes real time data then predicts the stock price and sends to web page
