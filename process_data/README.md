This folder contains the script for processing reddit comments in json format. It assigns topics to each comment, removes stop words,
splits the comments into individual words, finds frequencies in topics and overall for all words, and finds rolling averages for all word
frequencies. 

The spark command line submission file contains what should be entered on the command line to load the necessary packages for the script to run.

Spark Cluster Details:
18 x r5a.Large EC2 instances
Ubuntu 18.04
Spark 2.4.4


