# Code: Flink and AWS Kinesis Machine Problem (MP)

A Lambda function that retrieves data from an S3 bucket and generates data to a Kinesis Data Stream.

```python
import datetime
import json
import boto3
import time
import csv

STREAM_NAME = "input-stream"

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = 'inputbucket-park'
    key = 'AMDprices2021-2022.csv'
    response = s3.get_object(Bucket=bucket_name, Key=key)
    data = response['Body'].read().decode('utf-8')
    data = data.split('\n')  # Split data by lines
    columns = data[0].split(',')  # Extract column names from first row
    for row in data[1:]:
        row_data = row.split(',')  # Split row data by comma
        # Create dictionary of column names and row data
        data_dict = dict(zip(columns, row_data))
        generate(STREAM_NAME, boto3.client(
            'kinesis', region_name='us-east-1'), data_dict)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def get_data(data_dict):
    return {
        'date': data_dict['Date'],
        'ticker': 'AMD',
        'open_price': data_dict['Open'],
        'high': data_dict['High'],
        'low': data_dict['Low'],
        'close_price': data_dict['Close'],
        'adjclose': data_dict['Adj Close'],
        'volume': data_dict['Volume'],
        'event_time': datetime.datetime.now().isoformat(),
    }

def generate(stream_name, kinesis_client, data_dict):
    data = get_data(data_dict)
    print(data)
    kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(data),
        PartitionKey="partitionkey")
    # time.sleep(0.0)
```

PyFlink and Flink SQL can be used on Apache Zeppelin for AWS Kinesis Data Analytics.

```sql
%flink.ssql(type=update)

-- create a table to glue data catalog table to enable other applications to use the data
-- you can use the Flink Kafka Consumer to read from topics.
CREATE TABLE stock_table(
    `date` STRING,
    ticker VARCHAR(6),
    open_price FLOAT,
    high FLOAT,
    low FLOAT,
    close_price FLOAT,
    adjclose FLOAT,
    volume BIGINT,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time as event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kinesis',
    'stream' = 'input-stream',
    'aws.region' = 'us-east-1',
    'scan.stream.initpos' = 'LATEST',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);
```

```sql
%flink.ssql(type=update)
/*just to see the table
check out the flink job*/

SELECT * FROM stock_table;
```

The example below builds a UDF in PyFlink to calculate the average of two inputs and outputs the result in Flink SSQL. 

```python
%flink.pyflink

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.udf import udf

# Define the custom function
@udf(result_type=DataTypes.FLOAT(), input_types=[DataTypes.FLOAT(), DataTypes.FLOAT()])
def to_average(low: float, high: float) -> float:
    # Calculate the average of low and high
    avg = (low + high) / 2
    # Return the result
    return avg

# Register the custom function with a name
st_env.create_temporary_function("to_average", to_average)
```

```sql
%flink.ssql(type=update)

SELECT low, high, event_time, to_average(low, high) AS avg_price
FROM stock_table
```

### Exercise A: Compound Monthly Growth Rate

Build a User Defined Function (UDF) in PyFlink to calculate the compound monthly growth rate (CMGR). Write in Flink SSQL to output data using the UDF CMGR.

$$
CMGR = [(MonthlyPrice/StartPrice)^{1/numberofmonths}-1)*100\%
$$

StartPrice = 92.30 (The first closing price of 2021)
Extract year, month, and day values from date string. Check if the day is within the first three days of the month and not January of 2021. Compute the number of months between the start date and the current date. Compute the CMGR*100%. Return 0 if the day is not within the first three days of the month or it is January of 2021. Register the custom function with a name. 

In a new Flink SSQL paragraph:

1. Compute CMGR for each row using the registered "cmgr" function
2. `Select` date, close price, and computed CMGR, 
3. Filter out rows where CMGR is 0

The first few expected output should look like the following:

![Untitled](Code%20Flink%20and%20AWS%20Kinesis%20Machine%20Problem%20(MP)%2087b5c577e0ae41e79540b044edcdf30a/Untitled.png)

![Untitled](Code%20Flink%20and%20AWS%20Kinesis%20Machine%20Problem%20(MP)%2087b5c577e0ae41e79540b044edcdf30a/Untitled%201.png)

```python
%flink.pyflink
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.udf import udf

@udf(result_type=DataTypes.FLOAT(), input_types=[DataTypes.FLOAT(), DataTypes.STRING()])
def cmgr(curr_price, date_str) -> float:
    start_price = 92.30
    start_month = 1 + 2021*12
    # Extract year, month, and day values from date string
    month, day, year = map(int, date_str.split("/"))
    
    
    # Compute the number of months between the start date and the current date
    if day in [1,2,3]:
        num_months = month + year*12 - start_month
        # Compute the CMGR
        cmgr = (curr_price / start_price) ** (1 / num_months) - 1
        return cmgr*100
    else:
        return 0

# Register the custom function with a name
st_env.create_temporary_function("cmgr1", cmgr)
```

```sql
%flink.ssql(type=update)

SELECT event_time, cmgr, `date`, close_price
FROM(
    SELECT event_time, `date`, close_price, cmgr1(close_price, `date`) AS cmgr
    FROM stock_table
) t
WHERE cmgr <> 0
```

The example below queries a 10-day simple moving average (SMA) and outputs the result in Flink SSQL. 

```sql
%flink.ssql(type=update)
/* stock prices of when the current price is lower than the moving average which would be a good indicator for a bear market*/
SELECT *
FROM (
  SELECT
    event_time,
    ticker,
    close_price,
    AVG(close_price) OVER (
      PARTITION BY ticker
      ORDER BY event_time
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS avg_close_price
  FROM stock_table
) t
WHERE close_price < avg_close_price;
```

### Exercise B: Exponential Moving Average

Build a 10-day exponential moving average (EMA) that gives more weight to the most recent prices in Flink SSQL to output data below the EMA. 

$$
EMA = (P - EMAprev) * K + EMAprev
$$

We first calculate the 10-period moving average using the **`AVG`** function. Then, we calculate the EMA using the formula **`EMA = (Price(t) x K) + EMAprev`**, where **`Price(t)`** is the current price, **`K`** is the smoothing factor (**`2/(n+1)`** where **`n`** is the number of periods), and **`EMAprev`** is the previous period's EMA.

The EMA is calculated using a modified version of the formula, where the natural logarithm (LN) of the closing price is first calculated and then summed over a window of 10 days using the **`SUM`** function. This sum is then divided by 10 to get the average value of the natural logarithms. This average value is then exponentiated using the **`EXP`** function to get the EMA of the current day.

**`LN`** and **`EXP`** are mathematical functions that are used to calculate the smoothing constant. **`LN`** is the natural logarithm function, and **`EXP`** is the exponential function. In our case, we use **`LN`** and **`EXP`** to calculate **`K`** based on the number of days we want to use for our EMA.

The first few expected output should look like the following:

![Untitled](Code%20Flink%20and%20AWS%20Kinesis%20Machine%20Problem%20(MP)%2087b5c577e0ae41e79540b044edcdf30a/Untitled%202.png)

![Untitled](Code%20Flink%20and%20AWS%20Kinesis%20Machine%20Problem%20(MP)%2087b5c577e0ae41e79540b044edcdf30a/Untitled%203.png)

```sql
%flink.ssql(type=update)
/* stock prices of when the current price is lower than the exponential moving average which would be a good indicator for a bear market*/
SELECT *
FROM (
  SELECT
    event_time,
    `date`,
    ticker,
    close_price,
    AVG(close_price) OVER (
      PARTITION BY ticker
      ORDER BY event_time
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
    ) AS avg_close_price,
    EXP(
      SUM(
        LN(close_price)
      ) OVER (
        PARTITION BY ticker
        ORDER BY event_time
        ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
      ) / 10
    ) AS ema_close_price
  FROM stock_table
) t
WHERE close_price < ema_close_price;
```

### Exercise C: Anomaly Detection

Build an anomaly detection in price changes using MATCH RECOGNIZE in Flink SSQL to output data below the 8% drop percentage. The query should detect drops in stock prices within a certain time window, and return information about those drops including the percentage drop and the initial and final prices. 

```sql
DEFINE
	B AS -100*(B.close_price - A.close_price)/B.close_price < 8.0
```

The query should start by selecting all columns from your table, and then apply the MATCH_RECOGNIZE function. The function is partitioned by ticker and ordered by event_time. The MEASURES clause specifies what data to extract from each match, including the time of the initial price, the time of the drop, the percentage drop in price, and the initial and final prices. The ONE ROW PER MATCH clause ensures that only one row is returned for each match. The AFTER MATCH SKIP PAST LAST ROW clause is used to skip over rows that have already been matched, so that matches do not overlap. The PATTERN is defined as a sequence of rows where the first row (A) has the initial price, followed by any number of rows (B*) with prices within 8% of the initial price, and ending with a final row (C) with a price drop of more than 8%. The DEFINE clause is used to define the condition for the B rows, which is that the price drop is less than 8%.

The first few expected output should look like the following:

![Untitled](Code%20Flink%20and%20AWS%20Kinesis%20Machine%20Problem%20(MP)%2087b5c577e0ae41e79540b044edcdf30a/Untitled%204.png)

```sql
%flink.ssql(type=update)
/*detect patterns within the data like anomoly detection by Flink SQL = match recognize */
SELECT *
FROM stock_table
    MATCH_RECOGNIZE(
        PARTITION BY ticker
        ORDER BY event_time
        MEASURES
            C.event_time AS event_time,
            A.`date` AS initialPriceDate,
            C.`date` AS dropDate,
            -100*(C.close_price - A.close_price)/C.close_price AS dropPercentage,
            A.close_price AS initialPrice,
            C.close_price as lastPrice
        ONE ROW PER MATCH
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN (A B* C) WITHIN INTERVAL '1' MINUTES
        DEFINE
            B AS -100*(B.close_price - A.close_price)/B.close_price < 8.0
    )
```

After saving the outputs in CSV files, the Python file parses through them and sends them to the grader Lambda function.

```python
import csv
import requests
import json

url = "https://seorwrpmwh.execute-api.us-east-1.amazonaws.com/prod/mp11"

student = {
    "submitterEmail": "@illinois.edu",  # <Your coursera account email>
    "secret": ""  # <Your secret token from coursera>
}
lambda_url = "https://d5sgadicepzjz3qk2jtsxztx5m0wllmk.lambda-url.us-east-1.on.aws/"

exerciseACsvPath = "cmgr.csv"  # <Filepath for your exercise A csv file>
exerciseBCsvPath = "ema.csv"  # <Filepath for your exercise B csv file>
exerciseCCsvPath = "matchrecognize.csv"
# <Filepath for your exercise C csv file>

# Your columns
exerciseAdateColumn = 2
exerciseAcmgrColumn = 1
exerciseAclosepriceColumn = 3
exerciseBdateColumn = 1
exerciseBemaColumn = 5
exerciseBclosepriceColumn = 3
exerciseCinitialPriceDateColumn = 2
exerciseCdropDateColumn = 3
exerciseCdropPercentageColumn = 4
exerciseCinitialPriceColumn = 5
exerciseClastPriceColumn = 6

def readexerciseA(filePath, date, cmgr, close_price):
    vizData = {}

    with open(filePath, encoding="utf8", errors='ignore') as csvfile:
        reader = csv.reader((line.replace('\0', '')
                            for line in csvfile), delimiter=',')
        header = reader.__next__()

        for row in reader:
            vizData[row[date]] = [str(row[cmgr]), str(row[close_price])]

    return vizData

def readexerciseB(filePath, date, ema, close_price):
    vizData = {}

    with open(filePath, encoding="utf8", errors='ignore') as csvfile:
        reader = csv.reader((line.replace('\0', '')
                            for line in csvfile), delimiter=',')
        header = reader.__next__()

        for row in reader:
            vizData[row[date]] = [str(row[ema]), str(row[close_price])]

    return vizData

def readexerciseC(filePath, initialDate,
                  dropDate, dropPercentage, initialPrice, lastPrice):
    vizData = {}

    with open(filePath, encoding="utf8", errors='ignore') as csvfile:
        reader = csv.reader((line.replace('\0', '')
                            for line in csvfile), delimiter=',')
        header = reader.__next__()

        for row in reader:
            vizData[row[dropDate]] = [str(row[initialDate]), str(
                row[dropPercentage]), str(row[initialPrice]), str(row[lastPrice])]

    return vizData

def sendToAutograder(payload):
    r = requests.post(url, data=json.dumps(payload))
    r = requests.post(lambda_url, data=json.dumps(payload),
                      headers={"Content-Type": "application/json"})
    print(r.status_code, r.reason)
    print(r.text)

def main():
    ans1Data = readexerciseA(exerciseACsvPath, exerciseAdateColumn,
                             exerciseAcmgrColumn, exerciseAclosepriceColumn)
    ans2Data = readexerciseB(exerciseBCsvPath, exerciseBdateColumn,
                             exerciseBemaColumn, exerciseBclosepriceColumn)
    ans3Data = readexerciseC(exerciseCCsvPath, exerciseCinitialPriceDateColumn,
                             exerciseCdropDateColumn, exerciseCdropPercentageColumn, exerciseCinitialPriceColumn, exerciseClastPriceColumn)

    payload = {}
    payload['student'] = student
    payload['ans1_output'] = ans1Data
    payload['ans2_output'] = ans2Data
    payload['ans3_output'] = ans3Data

    print(json.dumps(payload))
    sendToAutograder(payload)

if __name__ == "__main__":
    main()
```

The grader Lambda function checks the results and sends the appropriate grade to the submitter’s Coursera.

```python
import json
import boto3
from boto3 import client as boto3_client
import json
import random
import uuid
import requests
import time

COURSERA_URL = 'https://www.coursera.org/api/onDemandProgrammingScriptSubmissions.v1'
CORRECT = "1"
WRONG = "0"
ASSIGNMENT_KEY = "uJ2xTtddSLeI6MX1vew7_A"
PART1_ID = "QIKNk"
PART2_ID = "nx8pW"
PART3_ID = "J1S8R"
DEFAULT_REGION = "us-east-1"

count = 0

class Log:
    def __init__(self, email):
        if "@" in email:
            email = email[0:email.index("@")]
        self.netId = email

    def print(self, msg):
        print(self.netId + " : " + msg)

def lambda_handler(event, context):
    global Logger
    global count
    Logger = Log("autograder")
    Logger.print("Request received. Checking parameters")

    data = json.loads(event['body'])

    is_valid, error_msg = is_payload_valid(data)
    if not is_valid:
        return send_failure_message(data, "Error in processing input data: " + str(data), {})

    student = data['student']
    Logger = Log(student["submitterEmail"])
    Logger.print(event['body'])

    TC1, TC2, TC3 = False, False, False

		ans1_output = {"2/1/...
		ans2_output = {"1/15/2021": [ ...
		ans3_output = {"1/15/2021": ["1/12/2021", "8.105659", "95.36", "88.21"], "2/25/2021": ["1/19/2021", "8.529482", "89.45", "82.42"], "3/4/2021": ["2/26/2021", "8.694537", "84.51", "77.75"], "5/12/2021": ["3/11/2021", "8.829051", "81.23",

		check_ans1 = data['ans1_output']
    check_ans2 = data['ans2_output']
    check_ans3 = data['ans3_output']

    parts = {}

    if check_ans1 == ans1_output:
        TC1 = True
        addToParts(parts, PART1_ID, CORRECT)
        count += 1
    else:
        addToParts(parts, PART1_ID, WRONG)

    if check_ans2 == ans2_output:
        TC2 = True
        addToParts(parts, PART2_ID, CORRECT)
        count += 1
    else:
        addToParts(parts, PART2_ID, WRONG)

    if check_ans3 == ans3_output:
        TC3 = True
        addToParts(parts, PART3_ID, CORRECT)
        count += 1
    else:
        addToParts(parts, PART3_ID, WRONG)

    if TC1 is False and TC2 is False and TC3 is False:
        return send_failure_message(data, "All 3 Test cases failed!", parts)
    elif TC1 and TC2 and TC3:
        return send_success_message(data, "All test cases passed", parts)
    else:
        sucess_tc = [
            "TC"+str(i+1) for i, result in enumerate([TC1, TC2, TC3]) if result is True]
        return send_success_message(data, "Only " + ", ".join(sucess_tc) + " passed", parts)

    return {
        'statusCode': 200,
        'body': 'Data sent to Lambda in Account #2'
    }

def is_payload_valid(body):
    params = ["student", "ans1_output", "ans2_output", "ans3_output"]

    for param in params:
        if param not in body:
            error = param + " inside body is missing. Please check your test.py"
            return False, error

    return True, ""

def post_to_coursera(data, parts):
    try:
        body = json.dumps({
            "assignmentKey": ASSIGNMENT_KEY,
            "submitterEmail": data["submitterEmail"],
            "secret": data["secret"],
            "parts": parts
        })
        Logger.print(body)
        requests.post(COURSERA_URL, data=body)
    except Exception as e:
        print(e)
        return e

def addToParts(parts, partId, answer):
    parts[partId] = {
        "output": answer
    }

def send_success_message(data, msg, parts):
    Logger.print(msg)
    post_to_coursera(data['student'], parts)
    return {
        "body": msg,
        "statusCode": 200
    }

def send_failure_message(data, msg, parts):
    post_to_coursera(data['student'], parts)
    return {
        "body": msg,
        "statusCode": 200
    }
```