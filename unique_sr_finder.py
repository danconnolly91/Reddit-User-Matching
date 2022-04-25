"""
This program reads in the file at TREATMENT_FILE_PATH, which is expected to contain entries that have 'author', 'subreddit' information.
It then analyzes the entries by counting the number of submissions in each subreddit by a given author, normalizing the count, computing the mean of each subreddit and then identifying the subreddit that deviates the most from the mean for each author.
The output is store in OUTPUT_FILE_NAME. 
A couple of methods for summary statistics are also included. (get_num_unique_column_in_df, get_median_num_unique_subreddit_per_user)
"""

import pandas as pd
import time
import configparser

start = time.time()

"""
Load configurations
"""
config = configparser.ConfigParser()
config.read('config.ini')
config.sections()
TREATMENT_FILE_PATH = config['outputPaths']['treatmentSubmissions']
OUTPUT_FILE_NAME = config['outputPaths']['userMatchingReddit']

# return a new dataframe that contains the author, subreddit and correspond number of posts in the submissions
def count_by_author_and_subreddit(submissions):
    return submissions.groupby(['author', 'subreddit']).size().to_frame(name= 'count').reset_index()

# return the number of unique items in the specified column of the dataframe
def get_num_unique_column_in_df(df, column_name):
    return len(df[column_name].unique())

# get the median number of unique subreddits each author submits in
def get_median_num_unique_subreddit_per_user(submissions):
    return (count_by_author_and_subreddit(submissions).groupby(['author']).agg('count'))['subreddit'].agg('median')

# return a data frame where each row contains the number of submissions in the subreddit(column) by a given author
def get_author_per_subreddit(counts_author_subreddit_pair):
    return pd.DataFrame(counts_author_subreddit_pair.pivot_table(index='author', columns='subreddit', values='count').fillna(0))

# cast counts to a sparse datatype
def get_sparse_df(df):
    return df.astype(pd.SparseDtype("int16"))

# normalize each row to have a sum of 1
def normalize_row(df):
    return df.div(df.sum(axis=1), axis=0)

def average_by_column(df):
    return df.mean(axis=0)

def get_index_max_of_row(df):
    return df.idxmax(axis=1)

"""Takes in a sparse array of count by author per subreddit and returns the most unique frequency in each row relative to average distribution"""
def get_most_unique_subreddit_from_counts(count_author_per_subreddit_array):
    # normalizing each row to have a sum of 1, so that the number reflect the user's activity across different subreddits
    activity_array = normalize_row(count_author_per_subreddit_array)
    # finding average user distribution per subreddit
    average_activity = average_by_column(activity_array)
    # finding deviations from the mean
    activity_centered_array = activity_array - average_activity
    # Return index of first occurrence of maximum over requested axis
    max_idx = get_index_max_of_row(activity_centered_array)
    max_idx = max_idx.to_frame().reset_index()
    max_idx.columns = ['author','most_unique_sr']
    return max_idx

def get_most_unique_subreddit_from_treatment(treatment_file_name):
    submissions = pd.read_csv(treatment_file_name)
    counts_author_subreddit_pair = count_by_author_and_subreddit(submissions)
    counts_author_per_subreddit = get_author_per_subreddit(counts_author_subreddit_pair)
    count_array = get_sparse_df(counts_author_per_subreddit)
    return get_most_unique_subreddit_from_counts(count_array)

# group the submissions by author and subreddit, then compute the average of created (the resulting column is still called created)
def get_avg_active_created_for_author_in_subreddit(submissions):
    return submissions.groupby(['author','subreddit'])['created'].mean().reset_index()

# left join the subreddits_for_matching df with avg_active_time df by matching author and subreddit, producing a df that contains author, most unique sr and the average time the author has posted in that subreddit
def merge_matching_subreddit_and_avg_active_time(subreddit_for_matching, avg_active_time):
    merged_df = pd.merge(subreddit_for_matching, avg_active_time, how='left', left_on=['author','most_unique_sr'], right_on = ['author','subreddit'])
    merged_df = merged_df[['author','most_unique_sr','created']]
    return merged_df

def get_most_unique_subreddit_and_time_from_treatment(treatment_file_name):
    submissions = pd.read_csv(treatment_file_name)
    counts_author_subreddit_pair = count_by_author_and_subreddit(submissions)
    counts_author_per_subreddit = get_author_per_subreddit(counts_author_subreddit_pair)
    count_array = get_sparse_df(counts_author_per_subreddit)
    sr_for_matching = get_most_unique_subreddit_from_counts(count_array)
    active_time_df = get_avg_active_created_for_author_in_subreddit(submissions)
    merged_df = merge_matching_subreddit_and_avg_active_time(sr_for_matching, active_time_df)
    return merged_df


if __name__ == '__main__':
    most_unique_sr = get_most_unique_subreddit_and_time_from_treatment(TREATMENT_FILE_PATH)
    # output to csv
    most_unique_sr.to_csv(OUTPUT_FILE_NAME)

end = time.time()
print('runtime = ' + str(end-start))