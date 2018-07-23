Timo(Temporal In-Memory Operate System With Spark) 

Abstract

Massive intensive time-series data generates from internat applications.Challenging that tranditional databases are not effective processing predictive analysis function over these data arises.It’s difficulty to process search and analysis operations with high throughout and low latency.This paper introduces Timo,a high availability distributed timing model, which based on in-memory computing.Data always is partitioned and transfered to different nodes on distributed system.Timo supports two-level indexes that global index can optimize to search on partitions and local index can reduce the latency of querying within partitions.Addition,Timo also introduces temporal index which based on checkpoint.Compared with current state-of-the-art indexes,this supports more efficient query performance with less storage.Timo analyses the process of temporal analysis and makes some optimized strategies to improve the throughtout.Since Spark is one of the widely adopted distributed in-memory computing system,we deploy Timo to Spark with seamless,and extend spark execution plans.The experiments,compared with other temporal systems based on Spark,shows that Timo’s query latency has reduced 1 to 4 times and the analytics throughtout of Top-K is improved by 2 to 3 times.
Timo is based on Spark 2.1.

Function
Timo supports common temporal operations,includes query,aggeration and Topk,as follows:
Query:Temporal_Query,Range_Query and Interval_Query
Aggerator:Max,Min and Mean
Analyse:Topk

If you have questions ,you can email elroyt@163.com.
