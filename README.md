Data Lake For Hospital DATA


Extract :-
●
Source,
○ Hospital Admin (Structured data)
○ Hospital Report Admin (structured data)
○ LAb Admin (structured data)

●
Data pipeline
○ To be built in spark job
○ To read from RDBMS and store into HDFS which is used as DataLake
○ Read all report and directly uploaded into data lake

Load :-
Data Lake:-
All different types of data store in data lake

Transform :-
➖Data retrieval
1)Spark job fetch data from data lake and store in MOngo db or mysql
Mongo db :- used for unstructured data storage like json
Mysql : - Structured data store
Data visualization:-
We used data visualization tools here like grafana

