# Pinterest Data Pipeline

1. Downloaded Pinterest Infrastructure (data resembling data received by the Pinterest API when a post request is made by a user uploading data to Pinterest). The infrastrucutre contains three tables which are as follows:
    -pinterest_data
    -geolocation_data
    -user_data
2. Configured an EC2 instance to use as an Apache Kafka client machine which I will then create topics on. Then I set up MSK connect to allow the MSK cluster to send data to a S3 bucket and any data that gets sent to the topic will be automatically saved and stored in a dedicated S3 bucket.

3. Created an API in AWS API Gateway which will send data to the MSK cluster using the MSK connect connector.

4. Read data from AWS into databricks for batch processing,. I cleaned and ran computations using spark on databricks

5. Orchestrated Databricks Worloads on AWS MWAA by uploading a DAG to a MWAA enviornment and triggering it to run at a given time. 

6. Read data into databrickks for stream processing. I sent data to the Kinesis streams, read and transformed the data using databricks and wrote the streaming data to delta tables.