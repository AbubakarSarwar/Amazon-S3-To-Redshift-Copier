# -*- coding: utf-8 -*-
"""
Created on Mon Apr  9 14:43:27 2018

@author: AbubakarSarwar
"""

#csv = The csv library will be used to iterate over the data
#ast = The ast library will be used to determine data type
#psycopg2 = It is a database adapter for the Python programming language

import csv, ast, psycopg2

#Open sample banking data as read only
f = open('Banking_SampleData.csv', 'r')
reader = csv.reader(f) #read file as csv

#longest = longest length of attribute
#headers = the list of attributes
#type_list = the data types of the attributes
longest, headers, type_list = [], [], [] 

#Function that will detect the data type of the attributes
def dataType(val, current_type):
    try:
        #Evaluate an expression node or a string containing a Python literal or container display.
        t = ast.literal_eval(val) 
    except ValueError:
        return 'varchar'
    except SyntaxError:
        return 'varchar'
    if type(t) in [int, int, float]:
       if (type(t) in [int, int]) and current_type not in ['float', 'varchar']:
           # Use smallest possible int type
           if (-32768 < t < 32767) and current_type not in ['int', 'bigint']:
               return 'smallint'
           # Use the next largest int type
           elif (-2147483648 < t < 2147483647) and current_type not in ['bigint']:
               return 'int'
           else:
           # Use largest int type    
               return 'bigint'
           # If not int, and not varchat then make it decimal
       if type(t) is float and current_type not in ['varchar']:
           return 'decimal'
    else:
        #If not int and float, assign varchar
         return 'varchar'

#Search row by row and find highest length for each attribute
for row in reader:
    #Assign attribute names to headers
    if len(headers) == 0:
        headers = row        
        for col in row:
            #Initialize longest with 0
            longest.append(0)
            type_list.append('')
    else:
        for i in range(len(row)):
            # NA is the csv null value
            if type_list[i] == 'varchar' or row[i] == 'NA':
                pass
            else:
                #Call the dataType function to identify the data type 
                var_type = dataType(row[i], type_list[i])
                type_list[i] = var_type
                #Assign the longest variable if the current row's length is greater than longest
                if len(row[i]) > longest[i]:
                    longest[i] = len(row[i])
                        
f.close()
#Allocate an extra 20 bucket of space for each attribute to avoid any errors in future
for k in range(0,len(longest)):
    longest[k]=longest[k]+20
print(longest)

#Create SQL Script for initialing the table
statement = 'create table banking_customer ('
for i in range(len(headers)):
    if type_list[i] == 'varchar':
        statement = (statement + '\n{} varchar({}),').format(headers[i].lower(), str(longest[i]))
    else:
        statement = (statement + '\n' + '{} {}' + ',').format(headers[i].lower(), type_list[i])
statement = statement[:-1] + ');'
#Print to verify the statement's syntax
print (statement)

#Create connection with Redshift Instance
conn = psycopg2.connect(
    host='<Enter your cluster host here>',
    user='<Enter your master username here>',
    port='<Enter your port number here>',
    password='<Enter your password here>',
    dbname='<Enter your database name here>'
    )
#First get a cursor from your DB connection
cur = conn.cursor()
#Execute query to create the appriopriate table
cur.execute(statement)
conn.commit()


#Create a query to fetch data from s3 and copy it to the above mentioned redshift instance
sql = """copy banking_customer from '<Enter your S3 instance link here>'
    access_key_id '<Enter your access key here>'
    secret_access_key '<Enter your secret key here>'
    region '<Enter your region here>'
    ignoreheader 1
    null as 'NA'
    removequotes
    delimiter ',';"""
#Execute query to copy data from s3 to redshift
cur.execute(sql)
conn.commit()

