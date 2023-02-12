import pandas as pd
from datetime import datetime, timezone
import random
import json
import math

# load csv as df
file_dates = pd.read_csv('tweet-file-dates.csv', index_col=[0])

# sort by date
file_dates = file_dates.sort_values(by='start_date', ascending=False)
file_dates = file_dates.reset_index(drop=True)

# convert date strings to datetime
file_dates['start_date'] = pd.to_datetime(file_dates['start_date'])

# find key dates and assign period
def after_leak(row):
    '''
    Determines whether each file includes tweets from after the leak (May 3rd).
    '''
    if row['start_date'] < datetime(2022, 5, 3,0,0,0, tzinfo=timezone.utc):
        return 0
    else:
        return 1

def after_decision(row):
    '''
    Determines whter each file includes tweets from after the Supreme Court 
    decision (June 24th).
    '''
    if row['start_date'] < datetime(2022, 6, 24,0,0,0, tzinfo=timezone.utc):
        return 0
    else:
        return 1

file_dates['after_leak'] = file_dates.apply(after_leak, axis=1)
file_dates['after_decision'] = file_dates.apply(after_decision, axis=1)
# print(file_dates.head())
# print(file_dates.tail())

# get files before leak for person 1 and 2
files_preleak = file_dates[file_dates['after_leak'] == 0]
num_preleak = len(files_preleak)
p1_all_files = files_preleak.iloc[:num_preleak//2].reset_index(drop=True)
p2_all_files = files_preleak.iloc[num_preleak//2:].reset_index(drop=True)
# print('p1 len: ', len(p1_all_files))
# print(p1_all_files.head())
# print('p2 len: ', len(p2_all_files))
# print(p2_all_files.head())

# get files after leak and before decision for person 3 and 4
files_predec = file_dates[(file_dates['after_leak'] == 1) &\
    (file_dates['after_decision'] == 0)]
num_predec = len(files_predec)
p3_all_files = files_predec.iloc[:num_predec//2].reset_index(drop=True)
p4_all_files = files_predec.iloc[num_predec//2:].reset_index(drop=True)
# print('p3 len: ', len(p3_all_files))
# print(p3_all_files.head())
# print('p4 len: ', len(p4_all_files))
# print(p4_all_files.head())

# get files after decision for person 5 and 6
files_postdec = file_dates[file_dates['after_decision'] == 1]
num_postdec = len(files_postdec)
p5_all_files = files_postdec.iloc[:num_postdec//2].reset_index(drop=True)
p6_all_files = files_postdec.iloc[num_postdec//2:].reset_index(drop=True)
# print('p5 len: ', len(p5_all_files))
# print(p5_all_files.head())
# print('p6 len: ', len(p6_all_files))
# print(p6_all_files.head())

# print files for each person
p1_start = p1_all_files['file_num'].iloc[0]
p1_stop = p1_all_files['file_num'].iloc[-1]
p2_start = p2_all_files['file_num'].iloc[0]
p2_stop = p2_all_files['file_num'].iloc[-1]
p3_start = p3_all_files['file_num'].iloc[0]
p3_stop = p3_all_files['file_num'].iloc[-1]
p4_start = p4_all_files['file_num'].iloc[0]
p4_stop = p4_all_files['file_num'].iloc[-1]
p5_start = p5_all_files['file_num'].iloc[0]
p5_stop = p5_all_files['file_num'].iloc[-1]
p6_start = p6_all_files['file_num'].iloc[0]
p6_stop = p6_all_files['file_num'].iloc[-1]
# print('The range of files for person one is: ',p1_start,'-',p1_stop)
# print('The range of files for person two is: ',p2_start,'-',p2_stop)
# print('The range of files for person three is: ',p3_start,'-',p3_stop)
# print('The range of files for person four is: ',p4_start,'-',p4_stop)
# print('The range of files for person five is: ',p5_start,'-',p5_stop)
# print('The range of files for person six is: ',p6_start,'-',p6_stop)

# get random files for each person
def sample_tweets(files, person, num_samples):
    '''
    Sample tweets for files in given range assigned to a person. Creates a csv
    file.
    '''
    sampled_tweets = pd.DataFrame(columns=['created_at', 'id', 'text'])

    sampled_files = random.sample(files, num_samples)
    # pick files
    for f in sampled_files:
        # load file
        filename = f'/students/mt105/cs234/download/twitter-dataset-2/{f}'
        with open(filename, 'r') as inputFile:
            file_json = json.load(inputFile)
        file_df = pd.DataFrame(file_json)

        # choose page
        pages = len(file_df['tweets'])
        page = random.randint(0, pages-1)

        # choose tweet
        tweets_len  = len(file_df['tweets'][page])
        tweet_num = random.randint(0, tweets_len-1)
        tweet = file_df['tweets'][page][tweet_num]

        # get data from tweet (which is a dictionary) and make it a df
        tweet_sub = dict((k, tweet[k]) for k in ('id', 'created_at', 'text'))
        tweet_data_df = pd.DataFrame(tweet_sub, index=[0])
        # tweet_data_df['file_num'] = file

        # append sampled tweet to main df
        sampled_tweets = sampled_tweets.append(tweet_data_df)

    # print(len(sampled_tweets))
    # print(sampled_tweets.head())

    # write csv file
    csv_filename = f'{person}-tweets-sampled.csv'
    sampled_tweets.to_csv(csv_filename, index = False)

# sample tweets
sample_tweets(list(p1_all_files['file_name']), 'p1', 200)
sample_tweets(list(p2_all_files['file_name']), 'p2', 200)
sample_tweets(list(p3_all_files['file_name']), 'p3', 200)
sample_tweets(list(p4_all_files['file_name']), 'p4', 200)
sample_tweets(list(p5_all_files['file_name']), 'p5', 200)
sample_tweets(list(p6_all_files['file_name']), 'p6', 200)