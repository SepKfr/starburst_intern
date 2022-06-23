# Data Analysis using Trino
## Zeek data
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
## Create Schema and Table in S3 Bucket
## Use Pandas to Query Table in S3 bucket
## Data Preparation using Pandas and SKlearn


