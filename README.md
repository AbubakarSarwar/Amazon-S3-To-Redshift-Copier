# Amazon-S3-To-Redshift-Copier
A solution created to upload a CSV file into Redshift instance using the help of Python and S3.

### 1) Setting up Redshift Instance

The following steps were followed from the Amazon Redshift Getting Started Guide to set up the Redshift environment:

Step 1: Set Up Prerequisites: 

•	Sign Up for AWS
•	Install SQL Client Drivers and Tools
•	Determine Firewall Rules

Step 2: Create an IAM Role:

•	Create a new role 
•	Set permission policies
•	Backup ARN for further use in data loading 

Step 3: Launch a Cluster:

•	Create a new Amazon Redshift Cluster 
•	Configure the cluster (cluster identifier, DB port, etc.)
•	Configure the node (Node type, cluster type, etc.)

Step 4: Authorize Cluster Access (EC2 – Virtual Private Cloud):

•	Configure the security access group of the cluster
•	Set customer inbound rules

Step 5: Connect to the Cluster

•	Get your own connection string
•	Connect your SQL workbench with your cluster
•	Run a simple query for testing purposes

Step 6: Load Sample Data

•	Write Query to create sample table
•	Import a sample data set 
•	Execute queries on the database
 

Figure 1: Role and Redshift Cluster

### 2) Loading File to Redshift using Python

Overview: There are multiple ways of pushing a file to Redshift using Python. The method used in this guide consists of the following steps:

•	Load the CSV file to Python
•	Extract the data types of the file to create SQL Table creation script
•	Create connection with Redshift instance
•	Execute the script on your Redshift instance to set up the meta data of the table
•	Upload the file to S3 
•	Create query to move data from S3 to Redshift
•	Execute the statement to copy data from S3 to Redshift

Dataset: The data set used for this current approach is a sample dummy banking data that was created earlier. The data itself is in CSV format and consists of 16 columns and 1000 rows. The snapshot of the data set is given below:
 
Figure 2: Sample Banking Data

Prerequisites: The following were the prerequisites of the current approach:
•	Installing specific python packages such as psycopg2 and ast
•	Creation and Configuration of Redshift instance
•	Creation and Configuration of S3 instance with the above dummy data
•	SQL workbench installation for testing purposes
  
Figure 3: S3 and Redshift instance

Code Explanation:

The following code is to load the above shown data set to the Redshift instance. The code has been divided into chunks of snippets and broken down into steps so that it is easier for the reader to understand.

Step 1: Importing Libraries and Reading Data in Python
•	Import three libraries csv, ast, psycopg2
•	Load the data into a CSV reader object
•	Initialize the required lists

Step 2: Identify the Data Types for Attributes
•	Create a function in python 
•	Use the ast library to evaluate the data types
•	Assign the appropriate data type for attribute

Step 3: Assign Data Types with their max length 
•	Iteratively walk through the full file 
•	Call the function we created earlier for each attribute
•	Assign data types to each attribute with their respective maximum length

Step 4: Create SQL Script for Meta Data Creation 
•	Create a script for creating table 
•	Concatenate the script and validate it

Step 5: Create Metadata in RedShift
•	Create Redshift Connection using psycopg2
•	Assign appropriate database configuration values
•	Execute the statement created above for meta data creation

Step 6: Execute Query to copy data from S3 to Redshift instance 
•	Create query for copying data 
•	Assign appropriate configuration values
•	Execute the statement to insert the data to Redshift instance
 
Figure 4: Retrieving the uploaded data

	
