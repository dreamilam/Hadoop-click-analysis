# Hadoop-click-analysis
This repository contains the code for some basic click and buy analysis on e-commerce data using Hadoop and MapReduce.

## Data
The data is from the 2015 RecSys challenge and can be found at http://recsys.yoochoose.net/challenge.html . 
We have two sets of data. The first one contains the click information and the other contains the buy information.

## Tasks
  * The first task is to find the most clicked item for the month of April. Only the items with the top 10 no of clicks should be displayed and the outputs should be sorted by the no of clicks.
  * The second task is to find the total revenue generated for a particular time interval in a day. The outputs have to be sorted by revenues.
  * The third task is to find out the success rate for an item based on both the click and buy data. The items with the top 10 success rates should be displayed and sorted by the success rate.

## Implementation
  * For all the three tasks, we use two MapReduce jobs that are chained and executed one after the other. The first MapReduce job gets the counts and the second one is used to sort and truncate the output.
  * For the third task, we used a simple trick to accompolish inner joins.

