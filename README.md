[//]: # (Image References)

[image0]: ./images/covid.png "COVID-19"
[image1]: ./images/raw_dataset.png "Datasets"
[image2]: ./images/covid19_model.png "COVID-19 Model"
[image3]: ./images/data_dictionary.png "Data Dictionary"
[image4]: ./images/architecture.png "Architecture"
[image5]: ./images/airflow.png "Airflow"
[image6]: ./images/Athena_Brazil.png "Airflow"
[image7]: ./images/Athena_USA.png "Airflow"

![COVID-19][image0] 

# Capstone Project: COVID-19 Data Lake
The **COVID-19 data lake** is my Capstone project for [Udacity](https://www.udacity.com/) [Data Engineering Nanodegree Program](https://www.udacity.com/course/data-engineer-nanodegree--nd027). In this project, I demonstrate what I have learned throughout the program, in particular the development of a data lake. To do so, I created an ETL pipeline using Python, AWS EMR, Spark, Airflow, AWS ECS, AWS CloudWatch, and Docker, storing Parquet files on AWS S3.


# Purpose
Currently, there are several initiatives around the world to gather and organize data about COVID-19. However, there are common issues, such as assuring the reliability of the data and keeping the data available and up-to-date in a centralized repository.

In order to solve those problems, this project proposes to build an automated pipeline which gather data from distinct sources and organize it in a data lake to be accessed by multiple clients. 

There are many available tools for data analytics. I could suggest tools such as AWS GLUE or AWS Athena. However, you can choose another of your preference to explore this data lake.

### Data Analysis
This data lake contains *daily* data about COVID-19. It is organized as following:

**Fact tables:**

- **Fact\_covid\_country:** number of deaths and confirmed cases of COVID-19, by country.  
- **Fact\_covid\_country\_province:** number of deaths and confirmed cases of COVID-19, by country and provinces. This fact table contains information only about Brazil and USA.*

**Dimension tables:**

- **Dim\_country:** general information about each country.
- **Dim\_province:** provinces by country (only Brazil and USA*).
- **Dim\_time:** occurrence date, split in year, week, month, day, and weekday.

**This data lake allows us to trace information about:**

- Confirmed cases and deaths in the USA, by province. 
- Confirmed cases and deaths in Brazil, by province. 
- Confirmed cases and deaths in other 186 countries.

**Possible analysis over this data lake include:**  

- Comparison of confirmed cases and deaths among countries.
- Spread of the Corona Virus.
- Measurement of the mortality rate.
- Measurement of the increase in the percent of confirmed cases and deaths.

__*__ This project provides detailed information about provinces only from United States and Brazil (currently, the two countries with more confirmed cases of the disease). However, the structure of this project allows the incorporation of provinces from other countries.

# Project Structure  

In this project, the tools below were used to automate and organize the extraction, transformation, and load processes. The chosen architectures is cheaper than the conventional as it is not necessary to keep a database or Amazon services online all the time.  Here I optioned by the use of S3 Parquet to store data so it is cheaper than to keep an online database. The end-users get easily to access the data by Athena, GLUE, or directly by the AWS account. Below there is a simple explanation about each adopted service or tool in this project.

## Architecture - Tools and Services

This architecture was conceived to keep the data available online seven days a week in a format supported by many processing systems. Hence, the ETL pipeline writes the output in a public AWS S3 bucket, following the Apache Parquet specification.  

Taking in consideration the size of the input data, this is a Big Data project that is better handled in a cluster rather than in a single machine. For that reason, AWS EMR (Elastic Map Reduce) was the choice to run the ETL pipeline in a Spark cluster.  

In addition to the data avaiability issue, cost-effectiveness is a concern as well. Thus, the EMR cluster should be active only during the processing time, and it should be shutdown once the job is completed. Apache Airflow was the choice to tackle the task of spinning the cluster up and down. Airflow is also responsible to ensure the input data availability, push tasks to the EMR cluster, guarantee that each step is executed in the defined order, re-run incompleted steps, and stop any service after the job completion.  

Airflow was packed in a Docker image along with the DAG definition. This image was made available in DockerHub, thus it can be loaded in a AWS ECS (Elastic Container Service) container launched by Fargate provider. This approach eliminates the necessity for a dedicated EC2 instance and, consequently, reduces costs.

To avoid incurring in extra charges, the ECS container that runs the Apache Docker image is started by an AWS CloudWatch Event at the scheduled time, once a day. Having assured all tasks completion, Airflow stops the container in where it is running, ceasing the costs of the ECS service.

The following image summarize the relationship of tools and services in this architecture.  

![Arquitecture][image4] 


- **AWS CloudWatch:** Triggers the ECS Task at the scheduled time, every day at 07:00 AM.
- **AWS Elastic Container Service (ECS):** The defined task starts one container running a Docker image which contains Airflow and the DAG definition.
- **DockerHub:** Repository where the Docker image is stored.
- **Apache Airflow:** Manages the ETL pipeline, running each step in the determined order. It spins the EMR cluster up and down and adds steps to be executed on it.
- **AWS Elastic MapReduce (EMR):** Creates a Spark cluster to run the ETL process.
- **Apache Spark:** Manages a cluster of EC2 instances so as to run the ETL script. It provides an interface for programming entire clusters with implicit data parallelism and fault tolerance.
- **GitHub:** Repository where the source code is stored. Spark runs the ETL script directly from GitHub.
- **AWS S3:** Holds the input data file as well as the data lake generated by the ETL pipeline.
- **Apache Parquet:** It is a data storage format compatible with most of the data processing frameworks in the Hadoop environment.
- **Python:** It is a high-level language, which also supports advanced data structures such as lists, sets, tuples, dictionaries, and many more. Because of its characteristics, it is an excellent tool to work with big data.



## Airflow: covid19\_pipeline\_dag  

![Airflow][image5] 

**Airflow Steps**

- **Begin\_execution:** DummyOperator that begins the pipeline.
- **transfer\_usa\_data\_file:** It copies the source file to a bucket on  Amazon S3. The source file is updated constantly  what causes failures during the processing time. For this reason, I chose to copy this file into a S3 bucket where it is not affected by updates.
- **transfer\_brazil\_data_file:** It copies the file to a bucket on Amazon S3 to make it available for the Spark session. 
- **verify\_world\_data\_file:** It verifies if the file is available.
- **spin\_up\_emr\_cluster:** It spins up the EMR cluster which runs a Spark session.
- **add\_pipeline\_step:** It adds the ETL script to run into the EMR/Spark cluster.
- **spin\_down\_emr\_cluster:** It spins down the EMR cluster used in this process.
- **stop\_airflow\_containers:** It stops the container to cease the costs of the ECS service.
- **End\_execution:** DummyOperator that ends the pipeline.

## ETL Architecture

In order to build the ETL pipeline, I considered some fundamental questions:   

- Which data sources do I need so as to perform the data extraction?
- Which tools and services may I use in my pipeline?
- Which information should I provide in the data lake?
- Which data is important to achieve the project goals?

Thinking through these questions assisted me in determining the overall data architecture.

### Datasets  

For this project, I gathered and organized three distinct data sources: two from [AWS COVID-19 Data Lake](https://aws.amazon.com/pt/blogs/big-data/exploring-the-public-aws-covid-19-data-lake/), one from [Brasil.io](https://brasil.io/dataset/covid19/caso_full/). The data is available in two file formats (.csv and .json). For more detail, see the table below:

![Datasets][image1] 

### Pipeline Steps  

I decided to build a pipeline that uses only one Spark session. Within this session, the ETL script runs tasks to read, transform, and load data into a data lake on AWS S3. Below I describe the steps of this pipeline.

**The first step loads dimension tables**

- **dim\_country:** contains basic data of the countries. The **COVID-19-Cases.csv**, available on [AWS COVID-19 Data Lake](https://aws.amazon.com/pt/blogs/big-data/exploring-the-public-aws-covid-19-data-lake/), is used to load the  Parquet files on AWS S3. No transformation is necessary in this step.

- **dim\_province\_country:** contains information about Brazil and the USA. To load the USA data, I used the JSON file available on [AWS COVID-19 Data Lake](https://aws.amazon.com/pt/blogs/big-data/exploring-the-public-aws-covid-19-data-lake/). The files COVID-19-Brazil.csv.gz and provinces\_brazil.csv contain information about Brazil. The file provinces\_brazil.csv contains the state's abbreviation name and the long name of each state. This process loads the information to AWS S3, partioned by country and province.

- **dim\_time** It contains information about date, year, week of the year, month, day, and day of the week. The date column in the **COVID-19-Cases.csv** file is presented as a string, so it must be converted like explained before. This process loads the information to AWS S3 partitioned by year and month.


**Second step: Load Brazil data** 

- **fact\_province\_country**
This table contains information about provinces. To load information about Brazil, it uses the files COVID-19-Brazil.csv and provinces_brazil.csv. The process loads the information to AWS S3 partitioned by date, country, and province.

- **fact\_country**
This table contains information about daily cases from each country. To load information about Brazil, it uses the file COVID-19-Brazil.csv. The process loads the information to AWS S3 partitioned by date and country.


**Third step: Load the USA data** 

- **fact\_province\_country**
This table contains information about provinces. To load information about the USA, it uses the files part-XXX.json. The process loads the information to AWS S3 partitioned by date, country, and province.


- **fact\_country**
This table contains information about daily cases from each country. To load information about the USA, it uses the file part-XXX.json. The process loads the information to AWS S3 partitioned by date and country.


**Fourth step: Load data of the 186 countries** 

This last step loads information about other 186 countries.

- **fact\_country**.
This table contains information about daily cases from each country. To load information about these countries, it uses the file COVID-19-Cases.csv. The process loads the information to AWS S3 partitioned by date and country.



## Data Model


The first step to design a data model is to identify which measures we would like to load into the data lake. 
During the development of this project, I explored and assessed several data sources multiple times to guarantee that the chosen information was suitable for my proposal. I opted for [open data on AWS](https://registry.opendata.aws/) that is a centralized repository for up-to-date and curated datasets. The document is available on [AWS](https://registry.opendata.aws/aws-covid19-lake/). I also used data from a Brazilian public repository that is available on [https://brasil.io/](https://brasil.io/dataset/covid19/caso_full/).

For this data model I created three dimension tables and two fact tables as I describe below: 

**Fact tables:**

- **Fact\_covid\_country:** number of deaths and confirmed cases of COVID-19, by country.  
- **Fact\_covid\_country\_province:** number of deaths and confirmed cases of COVID-19, by country and provinces. This fact table contains information only about Brazil and USA.*

**Dimension tables:**

- **Dim\_country:** general information about each country.
- **Dim\_province:** provinces by country (only Brazil and USA*).
- **Dim\_time:** occurrence date, split in year, week, month, day, and weekday.

**This data lake allows us to trace information about:**

- Confirmed cases and deaths in the USA, by province. 
- Confirmed cases and deaths in Brazil, by province. 
- Confirmed cases and deaths in other 186 countries.

**Possible analysis over this data lake include:**  

- Comparison of confirmed cases and deaths among countries.
- Spread of the Corona Virus.
- Measurement of the mortality rate.
- Measurement of the increase in the percent of confirmed cases and deaths.

__*__ This project provides detailed information about provinces only from United States and Brazil (currently, the two countries with more confirmed cases of the disease). However, the structure of this project allows the incorporation of provinces from other countries.


Below I provide the data model and the data dictionary.


### **The COVID19 Model**

![COVID-19 Model][image2] 

### **Data dictionary**

![Data Dictionary][image3] 

## Data quality checks  
Data quality checks ensure the pipeline have run as expected. There are two data quality checks which run in the ETL pipeline (covid19_etl.py):

- **Data quality checks to verify the number of countries** in Dim\_country and the number in Fact\_country.  This check compares the total number of countries in each table.
- **Data quality checks to verify the number of dates** in Dim\_time and the number in Fact\_country.  This check compares the total number of dates in each table.

# Potential Scenarios

It is necessary to analyze the scenarios below because data and remote accesses are increasing significantly constantly:

- **The data was increased by 100x**

COVID-19 data is increasing considerably every day so the best solution is to store the data in AWS S3 bucket. S3 creates and stores copies of all S3 objects across multiple systems, this is great to guarantee scalability and durability. This way data is available when needed and protected against failures, errors, and threats.

- **The pipeline can be run every day**
 
This process was thought to run daily, at a scheduled time. **AWS CloudWatch** is the service used to trigger the ETL pipeline.

- **The database needed to be accessed by 100+ people**

In the AWS S3 it is possible to create policies to manage S3 bucket access control and to audit S3 object's permissions. S3 is a highly scalable distributed storage service managed by AWS which handles the increase of access seamlessly.


# Queries examples using AWS Athena


- **Brazil query**

![Brazil query][image6]   

- **USA query**  

![USA query][image7] 

# References
- Centers for Disease Control and Prevention [CDC](https://www.cdc.gov/)
- World Health Organization [WHO](https://www.who.int/)
- [COVID19 Data Lake AWS](https://registry.opendata.aws/aws-covid19-lake/) 
- [Brasil.IO](https://brasil.io/dataset/covid19/caso_full/)
- [Airflow](https://airflow.apache.org/)
- [DockerHub](https://hub.docker.com/)