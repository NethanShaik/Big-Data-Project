Big Data Project - Netflix Viewers Analytics


By,
Nethan Shaik - 1230824
Tharun Sekar - 1188718
Ted Thomas - 1212288
Ranton Nigil Roke Keisar - 1226620

Introduction

In this project, we will demonstrate how the Apache Spark cluster works and perform data analytics on the dataset provided by Netflix in one of their challenges in Kaggle.

Dataset Link: https://www.kaggle.com/datasets/netflix-inc/netflix-prize-data
Github Project Link: https://github.com/NethanShaik/Netflix-Analytics

Requirements

In order to accomplish the above task, we need to have the following requirements:

4 machines
Hadoop
Apache-spark
Apache Pyspark

Demonstration

Firstly, you are going to need to keep one machine as the Master. This computer will perform the main Pyspark and data analytics operations, and we will need three machines to be kept as the Workers.

On the master, enter the following command: 

spark-class org.apache.spark.deploy.master.Master


You should get the following output


![image](https://github.com/user-attachments/assets/9f2811fa-06c1-46cf-a723-f007e5b0cfd8)


When you enter the MasterWebUI IP address, you should be getting the spark-cluster UI to display the cluster information. For now it will be blank

<img width="468" alt="image" src="https://github.com/user-attachments/assets/70fd3f4e-ddc7-4236-a737-756aa3157ac4">


(For demonstration purposes, I will be entering the command on my machine to show the output that you will be performing on the worker machine)


On the worker, you should enter the following command in order to connect to the master:

<img width="468" alt="image" src="https://github.com/user-attachments/assets/80dc1ae8-c5b0-402f-8af2-dff8f5d5e9bf">


spark-class org.apache.spark.deploy.worker.Worker spark://192.168.250.109:7077

Upon entering the command on the worker, the worker machine should be listed on the MasterWebUI cluster information


![image](https://github.com/user-attachments/assets/1220a300-32c2-4ef1-b31d-4c6b6b25122b)

Now, let’s go to the Pyspark program that will be executed on the master. You must initalize the spark session

![image](https://github.com/user-attachments/assets/679b9b75-a2e4-4dc5-beab-ccd649105886)

Now, you must ensure that you have the dataset stored in the HDFS in order for pyspark to read the file. 

You must create a directory and a path first to store the file

hdfs dfs -mkdir -p /data/netflix


Enter the following command to transfer the dataset from local to HDFS.

hdfs dfs -put /Users/nethanshaik/Desktop/Big_Data_Project/dataset/netflix_merged_output/netflix_merged_summary.csv /data/netflix/


After entering the above commands, you must go to the HDFS UI and check if the file is present.


![image](https://github.com/user-attachments/assets/5443fa04-d9a9-4f25-8d36-aa3502776043)

You should now be able to load the dataset from the hdfs command via pyspark.

![image](https://github.com/user-attachments/assets/a5338abc-3366-43b4-b507-43951869c6cb)

V’s of Big Data in the project

Volume: We have used a huge dataset that had to be pre-processed before the implementation of machine learning with Pyspark. Considering the fact that we have stored in HDFS, the use of spark ensures that the data is processed in a distributed and scalable manner.

Visualization: We have used matplolib and seaborn in order to understand and display the patterns and the clusters performed using K-Means. Using this, we can address the visualization of the viewer's patterns in ratings. 

Value: The insights derived from the analysis, such as yearly statistics, total ratings and the cluster tables of movies in terms of average ratings and the number of unique users, explain the value of the results. 

Variety: To mention variety, the dataset provided by the Kaggle Netflix challenge consisted of the text files and the CSV files that needed to be read and processed to combine the dataset in an orderly manner before being read by Pyspark. 
