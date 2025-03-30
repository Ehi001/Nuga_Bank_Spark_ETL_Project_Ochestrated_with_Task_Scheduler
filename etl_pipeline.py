# Import Necessary Libraries

from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
from pyspark.sql.functions import monotonically_increasing_id
import os
import psycopg2

# set java home 
os.environ['JAVA_HOME'] = 'C:\java8'

# Initialize my spark session
spark = SparkSession.builder \
        .appName("Nuga Bank ETL") \
        .config("spark.jars", "postgresql-42.7.5.jar") \
        .getOrCreate()

# Extract the historical data into a spark dataframe

df = spark.read.csv(r'dataset\rawdata\nuga_bank_transactions.csv', header = True, inferSchema = True)

# fill Up the missing values

df_clean = df.fillna({
    'Customer_Name' : 'Unknown',
    'Customer_Address': 'Unknown',
     'Customer_City' : 'Unknown', 
     'Customer_State' : 'Unknown',
     'Customer_Country' : 'Unknown',
     'Company' : 'Unknown',
     'Job_Title' : 'Unknown',
     'Email' : 'Unknown', 
     'Phone_Number' : 'Unknown',
     'Credit_Card_Number' : 0,
     'IBAN' : 'Unknown',
     'Currency_Code' : 'Unknown',
     'Random_Number' : 0.0,
     'Category' : 'Unknown',
     'Group' : 'Unknown',
     'Is_Active' : 'Unknown',
     'Last_Updated' : 'Unknown',
     'Description' : 'Unknown',
     'Gender' : 'Unknown',
     'Marital_Status' : 'Unknown'
})

# Drop the missing values in Last_Updated column

df_clean = df_clean.na.drop(subset = ['Last_Updated'])

# Data Transformation to 2NF
# Transaction Table

transaction = df_clean.select('Transaction_Date', 'Amount', 'Transaction_Type', 'Credit_Card_Number', 'IBAN', 'Currency_Code') \
                        .withColumn('Transaction_ID', monotonically_increasing_id()) \
                        .select('Transaction_ID', 'Transaction_Date', 'Amount', 'Transaction_Type', 'Credit_Card_Number', 'IBAN', 'Currency_Code')

#Customer Table

customer = df_clean.select('Customer_Name', 'Customer_Address', 'Customer_City', 'Customer_State', 'Customer_Country', 'Company', 'Job_Title', 'Email', 'Gender', 'Phone_Number') \
                            .withColumn('Customer_ID', monotonically_increasing_id()) \
                            .select('Customer_ID', 'Customer_Name', 'Customer_Address', 'Customer_City', 'Customer_State', 'Customer_Country', 'Company', 'Job_Title', 'Email', 'Gender', 'Phone_Number')

# fact table

fact_table = df_clean.join(transaction, ['Transaction_Date', 'Amount', 'Transaction_Type', 'Credit_Card_Number', 'IBAN', 'Currency_Code'], 'inner') \
                    .join(customer, ['Customer_Name', 'Customer_Address', 'Customer_City', 'Customer_State', 'Customer_Country', 'Company', 'Job_Title', 'Email', 'Phone_Number'], 'inner') \
                    .select('Transaction_ID', 'Customer_ID', 'Currency_Code', 'Random_Number', 'Category', 'Group', 'Last_Updated', 'Description')

# Data loading

# Create a function to create tables
def create_table():
    conn = psycopg2.connect(
        host = 'localhost',
        database = 'Nuga_Bank',
        user = 'postgres',
        password = 'Grateful@1'
    )
    cursor = conn.cursor()
    
    # Drop tables if they exist
    cursor.execute("DROP TABLE IF EXISTS customer CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS transaction CASCADE;")
    cursor.execute("DROP TABLE IF EXISTS fact_table CASCADE;")
                           
    # Commit changes before recreating table
    conn.commit()                       
                           
    # Create Customer Table                       
    create_customer_table = '''    
                            CREATE TABLE customer (
                                Customer_ID BIGINT,
                                Customer_Name VARCHAR (10000),
                                Customer_Address VARCHAR (10000), 
                                Customer_City VARCHAR (10000),
                                Customer_State VARCHAR (10000),
                                Customer_Country VARCHAR (10000),
                                Company VARCHAR (10000),
                                Job_Title VARCHAR (10000),
                                Email VARCHAR (1000),
                                Gender VARCHAR (100),
                                Phone_Number VARCHAR (100)
                            );
                        '''
    cursor.execute(create_customer_table)

    
    # Create Transaction Table                        
    create_transaction_table = '''
                                CREATE TABLE "transaction" (
                                Transaction_ID BIGINT,
                                Transaction_Date DATE,
                                Amount FLOAT,
                                Transaction_Type VARCHAR (10000),
                                Credit_Card_Number BIGINT,
                                IBAN VARCHAR (10000),
                                Currency_Code VARCHAR (100)
                            );
                        '''
    cursor.execute(create_transaction_table)

    #Create Fact_Table 
    create_fact_table = '''
                            CREATE TABLE fact_table (
                            Transaction_ID BIGINT,
                            Customer_ID BIGINT,
                            Currency_Code VARCHAR (100),
                            Random_Number FLOAT,
                            Category VARCHAR (1000),
                            "Group" VARCHAR (1000),
                            Last_Updated VARCHAR (10000),
                            Description VARCHAR (10000)
                        );
                    '''
    cursor.execute(create_fact_table)

    conn.commit()
    cursor.close()
    conn.close()

url = "jdbc:postgresql://localhost:5432/Nuga_Bank"
properties = {
    "user" : "postgres",
    "password" : "Grateful@1",
    "driver": "org.postgresql.Driver"
}

customer.write.jdbc(url = url, table = "customer", mode = "append", properties = properties)
transaction.write.jdbc(url = url, table = "transaction", mode = "append", properties = properties)
fact_table.write.jdbc(url = url, table = "fact_table", mode = "append", properties = properties)

print('Database, table and data uploaded successfully')


    