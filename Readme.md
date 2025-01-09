# Kafka Project: Ingesting Data from Wikimedia to OpenSearch

This project demonstrates how to use Apache Kafka to stream data from Wikimedia and ingest it into OpenSearch for search and analysis purposes.

## Overview

This project streams real-time change events from Wikimedia using Kafka. The data is then processed and ingested into OpenSearch, enabling easy exploration and analysis of Wikimedia updates.

---
![kafka flow](images/kafka-Opensource.png)

## Prerequisites

Before you start, ensure you have the following installed:

1. **Java (JDK 11 or later)**  
2. **Apache Kafka** 
3. **Docker** (for OpenSearch and Kafka setup)  
4. **Git** (to clone this repository)  
5. **Gradle** (for building the project)  

---

## Setup Instructions

1. **Clone the Repository**
2. **Setup Docker by running Docker-compose up -d**
3. **Run the WikimediaChangeProducer java file**
4. **Run the OpenSearchConsumer java file**

## Data Flow
- WikimediaChangeProducer will create events for the changes occurred in the Wikimedia website. These events are send to kafka as  topics From the Producer
- OpenSearchConsumer will Receive the records from the kafka and index them into the opensearch for user to do anlaysis on the change data using Open Search DashBoard.
