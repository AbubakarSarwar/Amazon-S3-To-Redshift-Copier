# Amazon-S3-To-Redshift-Copier
A solution created to upload a CSV file into Redshift instance using the help of Python and S3.

1) Setting up Redshift Instance
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
2) Loading File to Redshift using Python
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
Code Snippet:
1.	@author: AbubakarSarwar
2.	#csv = The csv library will be used to iterate over the data  
3.	#ast = The ast library will be used to determine data type  
4.	#psycopg2 = The psycopg2 is a database adapter that will be used to connect Redshift  
5.	import csv, ast, psycopg2  
6.	#Open sample banking data as read only  
7.	f = open('Banking_SampleData.csv', 'r')  
8.	reader = csv.reader(f) #read file as csv    
9.	#longest = longest length of attribute  
10.	#headers = the list of attributes  
11.	#type_list = the data types of the attributes  
12.	longest, headers, type_list = [], [], []   
Step 2: Identify the Data Types for Attributes
•	Create a function in python 
•	Use the ast library to evaluate the data types
•	Assign the appropriate data type for attribute
Code Snippet:
13.	#Function that will detect the data type of the attributes  
14.	def dataType(val, current_type):  
15.	    try:  
16.	        #Evaluate an expression node or a string containing a Python literal or container display.
17.	        t = ast.literal_eval(val)   
18.	    except ValueError:  
19.	        return 'varchar'  
20.	    except SyntaxError:  
21.	        return 'varchar'  
22.	    if type(t) in [int, int, float]:  
23.	       if (type(t) in [int, int]) and current_type not in ['float', 'varchar']:  
24.	           # Use smallest possible int type  
25.	           if (-32768 < t < 32767) and current_type not in ['int', 'bigint']:  
26.	               return 'smallint'  
27.	           # Use the next largest int type  
28.	            elif (-2147483648 < t < 2147483647) and current_type not in ['bigint']:  
29.	               return 'int'  
30.	           else:  
31.	           # Use largest int type      
32.	               return 'bigint'  
33.	           # If not int, and not varchat then make it decimal  
34.	        if type(t) is float and current_type not in ['varchar']:  
35.	           return 'decimal'  
36.	    else:  
37.	        #If not int and float, assign varchar  
38.	         return 'varchar'    

Step 3: Assign Data Types with their max length 
•	Iteratively walk through the full file 
•	Call the function we created earlier for each attribute
•	Assign data types to each attribute with their respective maximum length
Code Snippet:
39.	#Search row by row and find highest length for each attribute  
40.	for row in reader:  
41.	    #Assign attribute names to headers  
42.	    if len(headers) == 0:  
43.	        headers = row          
44.	        for col in row:  
45.	            #Initialize longest with 0  
46.	            longest.append(0)  
47.	            type_list.append('')  
48.	    else:  
49.	        for i in range(len(row)):  
50.	            # NA is the csv null value  
51.	            if type_list[i] == 'varchar' or row[i] == 'NA':  
52.	                pass  
53.	            else:  
54.	                #Call the dataType function to identify the data type   
55.	                var_type = dataType(row[i], type_list[i])  
56.	                type_list[i] = var_type  
57.	                #Assign the longest variable if the current row's length is greater than longest  
58.	                if len(row[i]) > longest[i]:  
59.	                    longest[i] = len(row[i])  
60.	                          
61.	f.close()  
62.	#Allocate an extra 20 bucket of space for each attribute to avoid any errors in future  
63.	for k in range(0,len(longest)):  
64.	    longest[k]=longest[k]+20  
65.	print(longest)  
Step 4: Create SQL Script for Meta Data Creation 
•	Create a script for creating table 
•	Concatenate the script and validate it
Code Snippet:
66.	#Create SQL Script for initialing the table  
67.	statement = 'create table Banking_Customers ('  
68.	for i in range(len(headers)):  
69.	    if type_list[i] == 'varchar':  
70.	        statement = (statement + '\n{} varchar({}),').format(headers[i].lower(), str(longest[i])) 
71.	    else:  
72.	        statement = (statement + '\n' + '{} {}' + ',').format(headers[i].lower(), type_list[i])  
73.	statement = statement[:-1] + ');'  
74.	#Print to verify the statement's syntax  
75.	print (statement)  


Step 5: Create Metadata in RedShift
•	Create Redshift Connection using psycopg2
•	Assign appropriate database configuration values
•	Execute the statement created above for meta data creation
Code Snippet:
76.	#Create connection with Redshift Instance  
77.	conn = psycopg2.connect(  
78.	    host='<Enter Cluster Host Link>',  
79.	    user='<Enter Cluster Master Username>',  
80.	    port='<Enter Cluster Port Number>',  
81.	    password='<Enter Cluster Password>',  
82.	    dbname='<Enter Database Name>'  
83.	    )  
84.	#First get a cursor from your DB connection  
85.	cur = conn.cursor()  
86.	#Execute query to create the appriopriate table  
87.	cur.execute(statement)  
88.	conn.commit()  
Step 6: Execute Query to copy data from S3 to Redshift instance 
•	Create query for copying data 
•	Assign appropriate configuration values
•	Execute the statement to insert the data to Redshift instance
89.	  
90.	#Create a query to fetch data from s3 and copy it to the above-mentioned redshift instance  
91.	sql = """copy users4 from 's3://samplebankdata/Banking_SampleData.csv' 
92.	    access_key_id '<Enter your Access Key>' 
93.	    secret_access_key '<Enter your Secret Access Key>' 
94.	    region '<Enter Region of S3 instance>' 
95.	    ignoreheader 1 
96.	    null as 'NA' 
97.	    removequotes 
98.	    delimiter ',';"""  
99.	#Execute query to copy data from s3 to redshift  
100.	cur.execute(sql)  
101.	conn.commit()
Final Output after copying file to Amazon Redshift:
 
Figure 4: Retrieving the uploaded data
3) Inserting rows into Table using Python:
Insertion using python is comparatively easy especially after what we have done above. The simple method of performing the insertion is as following:
•	Create connection with Redshift instance
•	Write/Input your Insertion query
•	Execute Query and Commit
1.	#Create connection with Redshift Instance  
2.	conn = psycopg2.connect(  
3.	    host='<Enter Cluster Host Link>',  
4.	    user='<Enter Cluster Master Username>',  
5.	    port='<Enter Cluster Port Number>',  
6.	    password='<Enter Cluster Password>',  
7.	    dbname='<Enter Database Name>'  
8.	    )  
9.	#First get a cursor from your DB connection  
10.	cur = conn.cursor()  
11.	#Create insert SQL Query   
12.	sql_insert = "INSERT INTO banking_customer (Member_ID,Credit_Score,Account_Balance,Employment_Status,Insurance,Delinquency,Credit_Card,OverDrawn_Status,Dependents,Joint_Account,Marital_Status,Loans,Age,Direct_Deposit_Income,Age_of_Membership_Account,Credit_Card_Limit) Values (1212,'Excellent','Low','Employee','None','No','Yes','No','Yes','No','Single','None',40,'High','5 years','Medium')" 
13.	#Execute and Commit SQL Query  
14.	cur.execute(sql)  
15.	conn.commit()  
Final Output after Row Insertion:
 
Figure 5: Retrieving the inserted row

	
