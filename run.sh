#!/bin/sh

# create input directory
hdfs dfs -mkdir /input

# put input files
hdfs dfs -put input/* /input

# run jar
hadoop jar recommender.jar Driver /input /dataDividedByUser /coOccurrenceMatrix /Normalize /Multiplication /Sum /Average 8
