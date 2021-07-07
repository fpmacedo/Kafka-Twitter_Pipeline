# Project: Twitter-Streaming-Kafka-PySpark

> by Filipe Macedo 06 July 2021

## 1. Project Description

The main objective of this project is to have the first contact with Apache Kafka, Spark Streaming and learn about the streaming process. To do this aproach I will use the twitter streaming API that allow developers to extract tweets in real time.

In the figure bellow you can see the arquitecture of the proposed solution.

![](img/flow.PNG)

## 2. Datasets

In this project just one dataset will be used, this dataset just have the tweet id and message:

```json
{
  "id" : "1412565941775474689",
  "message" : "Excellent Grades Guaranteed A+    
                #Assignment
                #Biology
                #Essay due
                ✔️Paper write
                #Paper pay
                Chem
                #Case study
                ✔️Do my h… https://t.co/ue7ZVtAyDZ"
}
```

