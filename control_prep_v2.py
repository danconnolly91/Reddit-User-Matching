import praw
import prawcore
import pandas as pd
import time
from psaw import PushshiftAPI
import numpy as np
#import statsmodels
import scipy

start = time.time()

def get_uniques_from_counts(count_array):
    """Takes in a sparse array of counts and returns the most unique frequency in each row relative to average distribution"""
    print('normalizing user distributions')
    count_array = count_array/(count_array.sum(axis=1)[:, None]).astype('float16')
    print('finding average user distribution')
    average_activity = count_array.mean(axis=0)
    #print(average_activity)
    print('finding deviations from mean')
    count_array = count_array - average_activity
    #print(count_array)
    print('returning uniques')
    #print(count_array.idxmax(axis=1))
    return count_array.idxmax(axis=1)
    

path = "C:/Users/Dan/Box/Class/Fall 2020/Large-Scale Social Phenomena/unemployment/"
#print('retrieving submissions')
#submissions = pd.read_csv(path + "submissions.csv")
#submissions['type'] = 'submission'
#submissions.drop(columns=['title', 'num_comments', 'upvote_ratio'])
#submissions = submissions[submissions['subreddit']!='Unemployment']

#print('retrieving comments')
#comments = pd.read_csv(path + "comments.csv")
#comments['type'] = 'comment'
#comments.drop(columns=['parent_id', 'is_submitter'])
#comments = comments[comments['subreddit']!='Unemployment']

#submissions.append(comments)
#submissions.to_csv('all_posts_noru_treatment.csv')

print('reading data')
submissions = pd.read_csv('all_posts_noru_treatment.csv')
#submissions = submissions[:100000]

print('getting dummies')
keep_list = ['author', 'subreddit', 'type']
count_cols = [element for element in submissions.columns.tolist() if element in keep_list]
counts = submissions[count_cols]
counts.columns = ['author', 'subreddit', 'counts']
counts = counts.groupby(['author', 'subreddit']).agg('count').reset_index()
print(len(counts['subreddit'].unique().tolist()))

#see average number of unique subreddits
uniques_sr = counts.groupby(['author']).agg('count').reset_index()
print(uniques_sr['subreddit'].agg('median'))
exit()

print('pivoting to wide')
counts = counts.pivot_table(index='author', columns='subreddit', values='counts').rename(columns={'author':'count','counts':'total'}).reset_index()
sr_list = [element for element in counts.columns.tolist() if element not in count_cols]
count_array = counts[sr_list].astype(pd.SparseDtype("int16", np.nan))

print('getting uniques')
counts['most_unique_sr'] = get_uniques_from_counts(count_array)
print(counts)

print(counts[sr_list].head())

subreddits_for_matching = counts[['author', 'most_unique_sr']]
subreddits_for_matching.to_csv(path + "subreddits_for_matching.csv")

end = time.time()
print('runtime = ' + str(end-start))

