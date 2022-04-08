import pandas as pd
import time

start = time.time()

"""
Configurations
"""
TREATMENT_FILE_PATH = 'all_posts_noru_treatment_top1000.csv'
OUTPUT_FILE_NAME = 'subreddits_for_matching.csv'

# read in the data
print('reading data')
submissions = pd.read_csv(TREATMENT_FILE_PATH)
# print(submissions.columns)

# return a new dataframe that contains the author, subreddit and correspond number of posts in the submissions
def count_by_author_and_subreddit(submissions):
    return submissions.groupby(['author', 'subreddit']).size().to_frame(name= 'count').reset_index()

counts_author_subreddit_pair = count_by_author_and_subreddit(submissions)

# return the number of unique items in the specified column of the dataframe
def get_num_unique_column_in_df(df, column_name):
    return len(df[column_name].unique())

# get the median number of unique subreddits each author submits in
def get_median_num_unique_subreddit_per_user(submissions):
    return (count_by_author_and_subreddit(submissions).groupby(['author']).agg('count'))['subreddit'].agg('median')

# return a data frame where each row contains the number of submissions in the subreddit(column) by a given author
def get_author_per_subreddit(counts_author_and_subreddit):
    return counts_author_subreddit_pair.pivot_table(index='author', columns='subreddit', values='count')

# print(counts_author_per_subreddit.head(5))
counts_author_per_subreddit = get_author_per_subreddit(counts_author_subreddit_pair)

# cast counts to a sparse datatype
def get_sparse_df(df):
    return df.fillna(0).astype(pd.SparseDtype("int16"))
count_array = get_sparse_df(counts_author_per_subreddit)
# print(count_array.head(5))

# normalize each row to have a sum of 1
def normalize_row(df):
    return df.div(df.sum(axis=1), axis=0)

def average_by_column(df):
    return df.mean(axis=0)

def get_index_max_of_row(df):
    return df.idxmax(axis=1)

"""Takes in a sparse array of counts and returns the most unique frequency in each row relative to average distribution"""
def get_most_unique_subreddit_from_counts(count_array):
    # normalizing each row to have a sum of 1, so that the number reflect the user's activity across different subreddits
    activity_array = normalize_row(count_array)
    # finding average user distribution per subreddit
    average_activity = average_by_column(activity_array)
    # finding deviations from the mean
    activity_centered_array = activity_array - average_activity
    # Return index of first occurrence of maximum over requested axis
    return get_index_max_of_row(activity_centered_array)

most_unique_sr = get_most_unique_subreddit_from_counts(count_array)

def get_most_unique_subreddit_from_treatment(treatment_file_name):
    submissions = pd.read_csv(treatment_file_name)
    counts_author_subreddit_pair = count_by_author_and_subreddit(submissions)
    counts_author_per_subreddit = get_author_per_subreddit(counts_author_subreddit_pair)
    count_array = get_sparse_df(counts_author_per_subreddit)
    return get_most_unique_subreddit_from_counts(count_array)

most_unique_sr2 = get_most_unique_subreddit_from_treatment(TREATMENT_FILE_PATH)

assert most_unique_sr.equals(most_unique_sr2)

# output to csv
# most_unique_sr.to_csv(OUTPUT_FILE_NAME)

end = time.time()
print('runtime = ' + str(end-start))

