Tested all functions with a small hand-written dataset. The dataset, test_json, has comments with UTCs from 5 days on May 1-5.

The table(s) that result(s) from each step is saved as .csv in its own folder. Some units were grouped b/c the table contained 
data that could not be written to .CSV

All of the comments are in a basketball related subreddit but 3 different subreddits in total

There is 1 comment for each day of the month, so May 1 will have 1 comment, and May 5 will have 5

All comments contain the word pizza at least once, so the totals for each day should be the same as the day of the month. May 3rd, 3 pizza

First run of tests found error in subreddit_topics_csv, where some subreddit names had a space after the name. I updated the
script that makes the list to fix this. 

All other calculations confirmed by hand. 

Future modifications, a word will count multiple times in overall wordcount for each topic it is a part of might want to figure out a way of avoiding this
