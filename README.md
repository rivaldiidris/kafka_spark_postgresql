# Spark To PostgreSQL, Kafka To PostgreSQL with Kafka Connector JDBC

This project is about ingesting a sample data using [PySpark](https://spark.apache.org/docs/latest/api/python/index.html "PySpark") and also [Apache Kafka](https://docs.confluent.io/kafka-clients/python/current/overview.html "Apache Kafka") to [PostgreSQL](https://www.postgresql.org/ "PostgreSQL"). We are going to use [Docker](https://www.docker.com/ "Docker") Container through Docker Compose for running each of services.

## Getting Started

### pre-requisite

* Docker installed into your local machine
* Basic understanding of Docker
* Basic understanding of Apache Spark Framework
* Basic understanding of Apache Kafka

## NOTES

* There will be only **1 docker compose file only** means that both batch and stream ingestion included there
* If you want to run batch ingestion, you should comment the stream ingestion code block docker compose file. Vice versa for stream ingestion.

### Executing program for stream ingestion

* Comment the code block for batch ingestion from docker compose file
* Go to project directory and run 
```
docker-compose up -d
```

* Make a sure that all services is running healthy.
* Try to inspect the network for postgres container using 
```
docker inspect <container_name>
```
Find the value of **IPAddress** of your postgres container. This will be useful for **connection.url value inside postgres-sink-config.json** and also **Host name/address while creating postgresql server through pgAdmin 4**

* Go to pgAdmin 4 by accessing
```
http://localhost:8888/browser/
```

* Login using user and password pgAdmin 4 based on Docker compose file
* Create postgres server using **IPAddress** above and fill the user and also password based on Docker compose file. Make a sure that you can interact with the postgresql through pgAdmin 4
* Go to Control Center Confluent Apache Kafka by accessing
```
http://localhost:9021/clusters
```
Just make a sure that the cluster is running healthy

* Go to **postgres-sink-config.json** and try to change the **IPAddress**. This will make this configuration file to create a sink connector to postgresql.
* Make a sure that you have not create the same connector before, you can check by running this command into your terminal and this command below also for checking the status of connectors
```
curl http://localhost:8083/connectors/<connector_name>/status
```
If you have not create the same connector, then let's create the sink connector by running this command below
```
curl -X POST -H "Content-Type: application/json" --data @postgres-sink-config.json http://localhost:8083/connectors
```
Make a sure that your sink connector is running well. If your connectors running well is shown like below
```
{"name":"postgres-sink-test","connector":{"state":"RUNNING","worker_id":"kafka-connect:8083"},"tasks":[{"id":0,"state":"RUNNING","worker_id":"kafka-connect:8083"}],"type":"sink"}
```

* You dont need to create manually topics, just start to produce the data by running
```
python producer_testing.py
```

* You can monitor the performance through Control Center Confluent Apache Kafka and also check pgAdmin 4 by running the SELECT query there, because new table should created inside postgresql based on Topic name

### Executing program for batch ingestion

* Comment the code block for stream ingestion from docker compose file
* Go to project directory and run without -d
```
docker-compose up
```

* Try to find the token of jupyterlabs inside the logs of docker compose 
* Go to jupyterlabs as playground for PySpark
```
http://localhost:9999/
```
Put the token and login

* Open the remote terminal to run requirements.txt
* If the remote terminal open, run command below
```
pip install -r requirements.txt
```
That code will installing pre-requisite of libraries that we need inside our jupyterlabs playground

* You can create Notebook for trying learning of Apache Spark with PySpark

* Before we running ingestion script into postgresql, we need to prepare the postgresql environment

* Find the postgresql container **IPAdress**
```
docker inspect <container_name>
```
find the value of **IPAddress**. The value will be useful for creating postgresql server and also postgresql connection while batch ingestion into python script.

* Go to pgAdmin 4
```
http://localhost:8888/browser/
```
Login using username and password based on Docker compose file

* Create postgresql server by filling **host name/address** with **IPAddress** of postgresql container. And also, fill the user and password based on docker compose file

* Prepare the table 
```
CREATE TABLE loan (loan_id varchar(255) NULL,loan_amount int NULL,borrower_id varchar(255) NULL,status varchar(255) NULL,partner varchar(255) NULL,current_dpd int NULL,max_dpd int NULL,interest_rate double precision NULL,loan_term int NULL);
CREATE TABLE partner (partner varchar(255) NULL,name varchar(255) NULL);
```

* Change postgrsql **host** inside **spark_postgresql/utils/config.ini** with **IPAddress** of postgresql container 
* Go to remote terminal and run this code below
```
python main.py
```
This script is about ingesting the data through postgresql by performing transformation or data preparation using PySpark

## Help and Authors

Any advise for common problems or issues. Please reach out me through LinkedIn [Rahul M Soemitro](https://www.linkedin.com/in/2618137-rahmul/ "Rahul M Soemitro")

## Version History

* 0.1
    * Initial Release Tue, 16 Jan 2023