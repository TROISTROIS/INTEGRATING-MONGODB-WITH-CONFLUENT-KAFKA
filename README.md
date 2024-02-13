# INTEGRATING MONGODB WITH KAFKA 
# Objective
Develop a Python-based application that integrates Kafka and MongoDB to process logistics data. The application will involve a Kafka producer and consumer, 
data serialization/deserialization with Avro, and data ingestion into MongoDB. Additionally, an API will be developed to interact with the data stored in MongoDB.

<img src="https://github.com/TROISTROIS/INTEGRATING-MONGODB-WITH-CONFLUENT-KAFKA/blob/main/diagram.png" alt="Alternative text" />
<br>

# Requirements:
●	Basic understanding of Python, Kafka, MongoDB.
<br>
●	Access to Confluent Kafka and MongoDB Atlas.
<br>

# Tasks
We are going to create a Kafka producer then use Pandas read logistics data from a CSV file and serialize the data into Avro format and publish it to a Confluent Kafka topic.
Using a Schema Registry for managing Avro schemas, the Kafka producer and consumer fetch the schema from the Schema Registry during serialization and deserialization. The consumer will then deserialize avro data and ingest it into a MongoDB collection. Implementing data validation checks in the consumer script before ingesting data into MongoDB for example:
Validations like checking for null values, data type validation, and format checks.We will then create an Flask API to interact with the MongoDB collection, to do the following:
<br>
● Simple CRUD(Create, Read, Update, Delete) operations.
<br>
●	Implement endpoints for filtering specific JSON documents and for aggregating data.


