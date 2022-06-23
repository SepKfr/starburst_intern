# Data Analysis using Trino
## Zeek data
Zeek is on open source network monitoring tool. Zeek sits on a “sensor,” a hardware, software, virtual, or cloud platform that quietly and unobtrusively observes network traffic. Zeek interprets what it sees and creates compact, high-fidelity transaction logs, file content, and fully customized output, suitable for manual review on disk or in a more analyst-friendly tool like a security and information event management (SIEM) system. 

The use of data analysis can be of a great help to conduct further interpretation of the data captured by zeek servers. The recorded data usually sits on clouds such as delta lakes that can be efficiently accessed by Trino. Therefore the inetgration of Trino and a data analysis tool such as python is a necessity. In this post, you are provided with a step-by-step tutorial from querying a data in S3 bucket using Trino to building a classification model to categorize the internet service provider for each connection.

## AWS account
1. You need to have an aws account with Starburst Enterprise Platform (SEP) cluster
2. Building the SEP cluster creates coordinator and worker instances
3. Any query has to be submitted to the coordinator for processing on the cluster
4. The coordinator has a host name such as *starburst.example.com*, or IP address

## AWS CLI
1. You need aws cli to move the datasets that you want to perform data analysis on to S3 bucket in your aws account
2. Download and install [aws cli](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) compatible to your operating system
3. Open your command prompt and config your aws cli
```
$ aws configure
AWS Access Key ID [****************DB6A]: Your access key id
AWS Secret Access Key [****************nf04]: Your secret access key
Default region name [us-east-2]: the region where your instances are
Default output format [ENTER]:
```
## Upload to S3 Bucket
1. Create a S3 bucket called zeekdata using aws cli 
```
$ aws s3 mb s3://zeekdata
```
2. Locate to the directory where your dataset resides
3. move your data to zeekdata bucket
```
aws s3 cp data s3://zeekdata --recursive
```
## Install Trino CLI
1. Download trino cli executable file [trino-cli-386-executable.jar](https://repo1.maven.org/maven2/io/trino/trino-cli/386/trino-cli-386-executable.jar)
2. Locate to the folder of the downloaded file
3. Change the name of the file to trino
```
$ mv trino-cli-386-executable.jar trino
```
4. Connect to your SEP coordinator by running the following command
```
./trino --server http://<your-SEP-coordinator-ip>:8080
```
## Create Schema and Table in S3 Bucket
1. Create an schema using the hive connector
```
trino> create schema hive.zeekschema with (location = 's3://zeekdata');
```
2. Create the table, hive.zeekschema.conn
```
trino> CREATE TABLE hive.zeekschema.conn ( 
    ->     "date" VARCHAR,
    ->     uid VARCHAR,
    ->     "id.orig_h" VARCHAR,
    ->     "id.orig_p" INT,
    ->     "id.resp_h" VARCHAR,
    ->     "id.resp_p" INT,
    ->     proto VARCHAR,   service VARCHAR,
    ->     duration DOUBLE,
    ->     orig_bytes BIGINT,
    ->     resp_bytes BIGINT,
    ->     conn_state VARCHAR,
    ->     local_orig BOOLEAN,
    ->     local_resp BOOLEAN,
    ->     missed_bytes BIGINT,
    ->     history VARCHAR,
    ->     orig_pkts BIGINT,
    ->     orig_ip_bytes BIGINT,
    ->     resp_pkts BIGINT,
    ->     resp_ip_bytes BIGINT,
    ->     ts TIMESTAMP
    -> ) WITH (
    ->     format = 'parquet',
    ->    external_location = 's3://zeekdata/data/conn/date=2022-04-21/'
    -> );
```
## Use Pandas to Query Table in S3 bucket
1. Install trino
```
$ pip install trino
```
2. Install SQLAlchemy
```
$ pip install trino[sqlalchemy] 
```
3. Create a python file e.g. zeekanalysis.py
```
$ vi zeekanalysis.py
```
4. Import sqlalchemy and create engine using trino
5. Pandas is a great tool for data analysis, hence we read the table as a dataframe object while selecting 3000 rows by random
```
from sqlalchemy import create_engine
from sqlalchemy.sql.expression import select, text
import pandas as pd

engine = create_engine('trino://<USER_NAME>:<PASSWORD>@<SERVER-IP>:8080/system')
connection = engine.connect()

df = pd.read_sql_query('''select * from hive.zeekschema.conn order by random() limit 3000''', con=connection)
```
## Data Preparation using Pandas and SKlearn
1. The first step is to get familiar with the content of the data (it can be observed that some columns have NULL values)
```
print(df.info())
```
```
Data columns (total 21 columns):
 #   Column         Non-Null Count  Dtype  
---  ------         --------------  -----  
 0   date           3000 non-null   object 
 1   uid            3000 non-null   object 
 2   id.orig_h      3000 non-null   object 
 3   id.orig_p      3000 non-null   int64  
 4   id.resp_h      3000 non-null   object 
 5   id.resp_p      3000 non-null   int64  
 6   proto          3000 non-null   object 
 7   service        2160 non-null   object 
 8   duration       1900 non-null   float64
 9   orig_bytes     1900 non-null   float64
 10  resp_bytes     1900 non-null   float64
 11  conn_state     3000 non-null   object 
 12  local_orig     3000 non-null   bool   
 13  local_resp     3000 non-null   bool   
 14  missed_bytes   3000 non-null   int64  
 15  history        2993 non-null   object 
 16  orig_pkts      3000 non-null   int64  
 17  orig_ip_bytes  3000 non-null   int64  
 18  resp_pkts      3000 non-null   int64  
 19  resp_ip_bytes  3000 non-null   int64  
 20  ts             3000 non-null   object 
dtypes: bool(2), float64(3), int64(7), object(9)
```

2. The second step is to remove or impute the NULL values
    - We first get the percentage of NULL values per each column
    ```
    print(df.isnull().sum() / df.shape[0])
    ```
    ```
       date             0.000000
        uid              0.000000
        id.orig_h        0.000000
        id.orig_p        0.000000
        id.resp_h        0.000000
        id.resp_p        0.000000
        proto            0.000000
        service          0.280000
        duration         0.366333
        orig_bytes       0.366333
        resp_bytes       0.366333
        conn_state       0.000000
        local_orig       0.000000
        local_resp       0.000000
        missed_bytes     0.000000
        history          0.002000
        orig_pkts        0.000000
        orig_ip_bytes    0.000000
        resp_pkts        0.000000
        resp_ip_bytes    0.000000
        ts               0.000000
        dtype: float64
    ```
    - We remove all the rows with NULL values
    ```
    df.dropna(inplace=True)
    ```
3. The next step is to normalize the numerical and encode the categorical variables
```
import sklearn.preprocessing
num_cols = df._get_numeric_data().columns # numerical columns
cat_cols = df.columns[~df.columns.isin(num_cols)] # categorical columns
real_scaler = sklearn.preprocessing.StandardScaler().fit(df[num_cols]) # normalize numerical variables
cat_scalers = {}
for col in cat_cols:
    srs = df[col].apply(str)
    cat_scalers[col] = sklearn.preprocessing.LabelEncoder().fit(srs.values) # encode categorical variables

df_trans = df.copy()
df_trans[num_cols] = real_scaler.transform(df[num_cols].values) # transform to normalized values
for col in cat_cols:
    string_df = df[col].apply(str)
    df_trans[col] = cat_scalers[col].transform(string_df) # transform to encoded values

```
4. Now, our data is ready for model training
    - Suppose the inquiry is to classify customers based on the service they use for connection
    - To this end, we build a Logistic Regression model
    ```
    # independent varibales (inputs)
    X = df_trans[df.columns[~df.columns.isin(["service"])]]
    # dependent varible (target)
    y = df_trans["service"]
    
    # split to train and test with 0.7 for train and 0.3 for test
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    
    # define and fit logistic regression model
    model = LogisticRegression(random_state=1234)
    model.fit(X_train, y_train)
    # make prediction
    y_hat = model.predict(X_test)
    prediction = cat_scalers["service"].inverse_transform(y_hat)
    true_y = cat_scalers["service"].inverse_transform(y_test)
    # classification report
    print(classification_report(true_y, prediction))
    ```
    ```
                      precision    recall  f1-score   support

            dhcp       0.00      0.00      0.00         1
             dns       0.99      1.00      1.00       308
            http       0.00      0.00      0.00         1
             ntp       0.00      0.00      0.00         1
             ssl       0.99      0.99      0.99       173

        accuracy                           0.99       484
        macro avg       0.40      0.40      0.40       484
     weighted avg       0.99      0.99      0.99       484
    ```
    - The overall accuracy of the model is 0.99 which indicates a very accurate model
# Summery
In this post, you learned how to access a dataset on a cloud based storage like delta lake, and you learned how to build a model using the data provided by Trino query engine. Last but not least, we can conclude that Trino can be easily integrated with data analysis tools such as python.
