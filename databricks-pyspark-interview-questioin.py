# Databricks notebook source
# Q-1 Create dataframe in pyspark databricks
data = [('Alice','Badminton,Tenis'),('Bob','Tennis,Cricket'),('Julie','Cricket,Caram')]
column=['Name','Hobbies']
df = spark.createDataFrame(data,column)
display(df)

# COMMAND ----------

# Q-2 Use function split and explode
data = [('Alice','Badminton,Tenis'),('Bob','Tennis,Cricket'),('Julie','Cricket,Caram')]
column=['Name','Hobbies']
df = spark.createDataFrame(data,column)
display(df)

# Perform Explode with split Operation
from pyspark.sql.functions import split,explode
df2 = df.select(df.Name,explode(split(df.Hobbies,',')).alias('Hobbie'))
display(df2)

# COMMAND ----------

# Q-3 Use of coalesce, with, when and otherwise function
data = [('Goa','','AP'),('','AP',None),(None,'','bglr')]
columns = ['City1','City2','City3']
df = spark.createDataFrame(data,columns)

from pyspark.sql.functions import *
df1 = df.withColumn('firstnonNull',
                    coalesce(when(df.City1=="",None).otherwise(df.City1),
                             when(df.City2=="",None).otherwise(df.City2),
                             when(df.City3=='',None).otherwise(df.City3)))
display(df1)

# COMMAND ----------

# Q-4 Use of join in pyspark
data1= [(1,"Steve"),(2,"David"),(3,"John"),(4,"Shree"),(5,"Helen")]
data2=[(1,"SQL","90"),(1,"Pyspark",100),(2,"SQL",70),(2,"Pyspark",60),(3,"SQL",30),(3,"Pyspark",20),(4,'SQL',50),(4,"Pyspark",50),(5,'SQL',45),(5,'Pyspark',45)]

schema1=['Id','Name']
schema2=['Id','Subject','Mark']

df1=spark.createDataFrame(data1,schema1)
df2=spark.createDataFrame(data2,schema2)

display(df1)
display(df2)

# Applying join bwtween two dataframe
%python
df_join=df1.join(df2,df1.Id==df2.Id).drop(df1.Id)
display(df_join)

# COMMAND ----------


# Q-5 Use of group by, agg, sum function
# Note:- dataframe refrenced from the above cell where we are creating the dataframe
dfper = df_join.groupBy('Id','name').agg(sum('Mark')/count('*')).alias('Percentage')
display(dfper)
 

# COMMAND ----------

# Q-6 Use of dense_rank() function
#Note to use function like rank() or row_num() just replace dense_rank() with rabk and row_num function,Other all logic is going tobe same.
data = [(1,"A",1000,'IT'),(2,"B",1500,'IT'),(3,"C",2500,'IT'),(4,"D",3000,'HR'),(5,"E",2000,'HR'),(6,"F",1000,'HR'),(7,"G",4000,'Sales'),(8,"H",4000,'Sales'),(9,"I",1000,'Sales'),(10,"J",2000,'Sales')]

scheme=["EmpId","EmpName","Salary","DeptName"]

df = spark.createDataFrame(data=data,schema=scheme)

display(df)

from pyspark.sql.functions import *
from pyspark.sql.window import *
# Use of dense_rank() function
df_rank = df.select('*',dense_rank().over(Window.partitionBy(df.DeptName).orderBy(df.Salary.desc()))).alias('desc_rank')
display(df_rank)

# COMMAND ----------

# Q-7 Use of filter operation
# Note:- df_rank is created in the above cell, use that for refrence 
df_rank1 = df_rank.filter(df_rank.dense_rank==1)
display(df_rank1)

# COMMAND ----------

# Q-7 Use of withColumn and to_date() function operation

data1=[(100,"Raj",None,1,"01-04-23",50000),
       (200,"Joanne",100,1,"01-04-23",4000),(200,"Joanne",100,1,"13-04-23",4500),(200,"Joanne",100,1,"14-04-23",4020)]
schema1=["EmpId","EmpName","Mgrid","deptid","salarydt","salary"]
df_salary=spark.createDataFrame(data1,schema1)
display(df_salary)
#department dataframe
data2=[(1,"IT"),
       (2,"HR")]
schema2=["deptid","deptname"]
df_dept=spark.createDataFrame(data2,schema2)
display(df_dept)

# Operations applied for the above dataframe
from pyspark.sql.functions import *
df=df_salary.withColumn('Newsaldt',to_date('salarydt','dd-MM-yy'))

# COMMAND ----------

# Q-8 Way of reading file in Filestore with multiple read option operator.
# File location and type

file_location = "/FileStore/tables/Churn_Modelling.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Q-9 To get partation of dataframe in rdd
# to check partation 
# Note:- dataframe used is refrence above
df.rdd.getNumPartitions()
display(df.rdd.getNumPartitions())

# COMMAND ----------

# Q-9 To perform repartation 
# Note:- The Dataframe used is created above and refrenced here 
df=df.repartition(10)
display(df)

# COMMAND ----------

# Q-10 Schema Comparision in pyspark :- It is used when we need to store multiple dataframe into single dataframe and that dataframe needs to be stored in in some file.
data1=[(1,"Ram","Male",100),(2,"Radhe","Female",200),(3,"John","Male",250)]
data2=[(101,"John","Male",100),(102,"Joanne","Female",250),(103,"Smith","Male",250)]
data3=[(1001,"Maxwell","IT",200),(2,"MSD","HR",350),(3,"Virat","IT",300)]
schema1=["Id","Name","Gender","Salary"]
schema2=["Id","Name","Gender","Salary"]
schema3=["Id","Name","DeptName","Salary"]
df1=spark.createDataFrame(data1,schema1)
df2=spark.createDataFrame(data2,schema2)
df3=spark.createDataFrame(data3,schema3)
display(df1)
display(df2)
display(df3)

# Operation 1 : To check schema of two dataframe
if df1.schema == df2.schema:
    print("Schema Matched")
else:
    print('Schema Not matched')

# Operation 2 : To print schema of any dataframe
print(df.schema)

# Operation 3 : To subtrach column bw two dataframe
print(list(set(df1.schema)-set(df3.schema)))
print(list(set(df3.schema)-set(df1.schema)))

# Operation 4 : Collect all the column
all_Columns = df1.columns+df3.columns 

# Operation 5 : To get unique column
unique_column=list(set(df1.columns+df3.columns)) 

# Operation 6 : To find missing column in a dataframe and use of lit operation 
from pyspark.sql.functions import *
for i in unique_column:
    if i not in df1.columns:
        df1=df1.withColumn(i,lit(None))
    if i not in df3.columns:
        df3=df3.withColumn(i,lit(None))

# COMMAND ----------

# Q-11 Use of StructFiled to enfore the schema
from pyspark.sql.functions import *
from pyspark.sql.types import *
data=[(1,('A-424','Noida','India')),(2,('M.15','Unnao','India'))]
schema = StructType([
     StructField('AddId', IntegerType(), True),
     StructField('Address', StructType([
         StructField('Add1', StringType(), True),
         StructField('City', StringType(), True),
         StructField('Country', StringType(), True)
         ]))
     ])

# COMMAND ----------

# Q-12 Difference between subtract() and exceptall()
#Subtract and Except All
#subtract(): functions return a new dataframe with rows in the first dataframe that are not in second dataframe
#exceptall():- functions also returns a new dataframe with rows in the first dataframe that are not in first dataframe but dones not remove dublicate.

data = [(1,"Mike","2018","10",30000), 
    (2,"John","2010","20",40000), 
    (2,"John","2010","20",40000), 
    (3,"Jack","2010","10",40000), 
    (4,"Charlee","2005","60",35000), 
    (5,"Guo","2010","40",38000)]
schema = ["empid","empname","doj", "deptid","salary"]

data1 = [(1,"Mike","2018","10",30000), 
           (4,"Charlee","2005","60",35000), 
           (5,"Guo","2010","40",38000)] 
schema1 = ["empid","empname","doj", "deptid","salary"]
df = spark.createDataFrame(data, schema)
df1 = spark.createDataFrame(data1,schema1) 
display(df)
display(df1)

# Operation 1 exceptall() and subtract()
display(df.exceptAll(df1))
display(df.subtract(df1))


# COMMAND ----------

# Q-13 Different way of reading a file  PREMISSIVE,DROPMAILFORMED,FAILFAST

# Premissive is by default when corrupted records comes up put that record into the column create to handle corrupt records.
df1 = spark.read.option('header',true).option('mode','PREMISSIVE').schema(schema).csv(path='mention path')
# DROPMALFORMED drop the records it self.
df3 - spark.read.option('header',true).option('mode','DROPMALFORMED').schema(schema).csv(path='mention path')
# FAILFAST throw error if data is not coorect.
df2 = spark.read.option('header',true).option('mode','FAILFAST').schema(schema).csv(path='mention path')


# COMMAND ----------

# Q-14 Date operation in pyspark
from pyspark.sql.types import *
data=[(1,'2024-01-01',"I1",10,1000),(2,"2024-01-15","I2",20,2000),(3,"2024-02-01","I3",10,1500),(4,"2024-02-15","I4",20,2500),(5,"2024-03-01","I5",30,3000),(6,"2024-03-10","I6",40,3500),(7,"2024-03-20","I7",20,2500),(8,"2024-03-30","I8",10,1000)]
schema=["SOId","SODate","ItemId","ItemQty","ItemValue"]
df1=spark.createDataFrame(data,schema)
display(df1)

# Operation 1 cast() and DataType(), basically casting into datetype
df1= df1.withColumn('SODate',df1.SODate.cast(DateType()))
df1.printSchema()

# Operation 2 Extract month and year from the date
from pyspark.sql.functions import *
df2=df1.select(month(df1.SODate).alias('Month'),year(df1.SODate).alias('Year'),df1.ItemValue)
display(df2)

# COMMAND ----------

# Q-15 Use of when and function
from pyspark.sql.types import *
data = [('IT','M'),('IT','F'),('IT','M'),('IT','M'),('HR','F'),('HR','F'),('HR','F'),('HR','F'),('HR','F'),('SALES','M'),('SALES','M'),('SALES','F'),('SALES','F'),('SALES','M'),('SALES','M'),('SALES','F'),]
schema = ['DeptName','Gender']
df1= spark.createDataFrame(data,schema)
display(data)

# Opertion 1
from pyspark.sql.functions import *
df2 = df1.select(df1.DeptName,when(df1.Gender=='M',1).alias('Male'),when(df1.Gender=='F',1).alias('Female'))
display(df2)

# COMMAND ----------

# Q-16 Use of split Function
data=[('Joanne',"040-20215632"),('Tom',"044-23651023"),('John',"086-12456782")]
schema=["name","phone"]
df=spark.createDataFrame(data,schema)
display(df)

from pyspark.sql.functions import * 

# operation 1
df=df.withColumn("std_code",split(df.phone,'-').getItem(0))
df=df.withColumn("phone_num",split(df.phone,'-').getItem(1))
display(df)

# COMMAND ----------

# Question 17 Use of union function 
simpleData1 = [(1,'Sagar','CSE','UP',88),(2,'Shivam','IT','MP',86),(3,'Munni','Mech','AP',78),]   
column =["ID","Student_Name","Department_Name","City","Marks"]
df1 = spark.createDataFrame(simpleData1,column)

simpleData2 = [(2,"Raj","CSE","HP"),(3,"Raj","IT","HP")]
columns = ["ID","StudentName","Departemnt","City"]
df2 = spark.createDataFrame(simpleData2,columns)


# Operation 1
df1.union(df2)

# Note :- As both table is having different set of colum its not possible to union both to fix the problem below code change is made and then union is applied.

from pyspark.sql.functions import lit
df2=df2.withColumn("Marks",lit("null"))
df3 = df1.union(df2)
display(df3)


# COMMAND ----------

# Q-18 Use of regex operation rlike
simpleData = ([1,"Shivam","UBFGD78945"],[2,"Sagar","7338941808"],[3,"Muni","9401330193"])
columns = ["Id","Name","MobileNo"]

df1 = spark.createDataFrame(simpleData,columns) 

# Operation 1
df2 = df1.select("*").filter(col("MobileNo").rlike('^[0-9]*$'))
display(df2)

# COMMAND ----------

# Q-19 Use of pivot operation 
data = [(1, 'gaga', 'UK', '2022-04-26'), (2, 'baba', 'UK', '2022-01-10'), (3, 'mama', 'India', '2022-01-14'), (4, 'mama', 'USA', '2022-06-20'), (1, 'gaga', 'Canada', '2022-08-02'), (2, 'baba', 'Canada', '2022-07-08'), (3, 'zaza', 'India', '2022-03-28'), (4, 'mama', 'UK', '2022-04-30'), (1, 'gaga', 'USA', '2022-01-16'), (2, 'zaza', 'India', '2022-08-13'), (3, 'gaga', 'USA', '2022-09-06'), (4, 'mama', 'India', '2022-09-07'), (1, 'mama', 'Germany', '2022-10-05'), (2, 'baba', 'Germany', '2022-09-13'), (3, 'baba', 'Canada', '2022-09-11'), (4, 'zaza', 'UK', '2022-09-24'), (1, 'baba', 'Canada', '2022-09-03'), (2, 'baba', 'India', '2022-06-11'), (3, 'mama', 'UK', '2022-01-30'), (4, 'zaza', 'USA', '2022-12-15')]

columns=['ID','Name','Country','Date_part']

df = spark.createDataFrame(data,columns)

# Operation - 1 
from pyspark.sql.functions import *
df1 = df.groupBy('ID','Date_part').pivot('Name').agg(first('Country'))
display(df1)


# COMMAND ----------

# Q-20 Use of row_num() and Windows funcition
# Note:- The above cell dataframe is refrenced here
from pyspark.sql.window import Window
from pyspark.sql.functions import * 

# Operation 1
windowspec = Window.orderBy(col('Id'))
df = df.withColumn('rownumber',row_number().over(windowspec))
display(df)

# COMMAND ----------

# Q-21 How to find occurace of duplicate row with the help of primary key
data = [(1,'Anish',30000,'enganish213@outlook.com'),(1,'Anish',30000,'enganish213@outlook.com'),(1,'Anish',30000,'enganish213@outlook.com'),(2,'Bhavesh',40000,'bhavesh213@gmail.com'),(2,'Bhavesh',40000,'bhavesh213@gmail.com'),(3,'Chandan',50000,'chandan213@yahoo.com'),(3,'Chandan',50000,'chandan213@yahoo.com'),(4,'Dinesh',60000,'dinesh213@yahoo.com'),(4,'Dinesh',60000,'dinesh213@yahoo.com'),(5,'Esha',70000,'rakesh213@yahoo.com')]
columns = ['Id','Name','Salary','Email']
df = spark.createDataFrame(data,columns)

# Operation 1
dublicate_email = (
    df.groupBy('Email').agg(count('Email').alias('EmailCount')).filter(col('EmailCount') >1)
)

# COMMAND ----------

# Q-22 Use of Windows Paration By and order function 
from pyspark.sql.functions import col, rank
from pyspark.sql.window import Window

data = [
    (1, "HR", 5000),
    (2, "HR", 6000),
    (3, "IT", 7000),
    (4, "IT", 8000),
    (5, "HR", 7000),
    (6, "IT", 6000)
]

# Create DataFrame
columns = ["employee_id", "department", "salary"]
df = spark.createDataFrame(data, columns)
df.show()

# Operation 1 Apply Order by and partationBy
windowSpec = Window.partitionBy("department").orderBy(col("salary").desc())

# Operation 2 Apply rank function to rank employees within each department based on salary
df_with_rank = df.withColumn("rank", rank().over(windowSpec))

# Show the result
df_with_rank.show()