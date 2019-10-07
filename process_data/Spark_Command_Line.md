#spark submission to load packages:
spark-submit --master spark://10.0.0.16:7077 --packages org.apache.hadoop:hadoop-aws:2.7.3,  --packages org.postgresql:postgresql:42.2.5 process_reddit_comments.py

