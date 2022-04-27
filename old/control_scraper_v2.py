import praw
import prawcore
import pandas as pd
import time
from psaw import PushshiftAPI

start = time.time()


def user_exists(name):
    try:
        reddit.redditor(name).id
    except prawcore.exceptions.NotFound:
        return False
    except AttributeError:
        return False
    return True


reddit = praw.Reddit(client_id='ArgFl6Em2AuwoA', client_secret='WP0DaW1B_inHi3Lf8Du8Ag5ag9Y',
                     user_agent='contentScraper')
api = PushshiftAPI(reddit)

posts = []
user_submission_data = []
user_comment_data = []
runemployment = reddit.subreddit('unemployment')

start = time.time()

match_df = pd.read_csv('matches 12.6.csv')
match_df_2 = pd.read_csv('matches 12.6 pt 2.csv')
match_df_3 = pd.read_csv('matches_12_14.csv')
match_df_4 = pd.read_csv('matches_12_15.csv')
match_df_5 = pd.read_csv('matches 12.16.csv')
match_df_6 = pd.read_csv('matches 12_17 9pm.csv')
match_df = match_df.append(match_df_2, ignore_index=True)
match_df = match_df.append(match_df_3, ignore_index=True)
match_df = match_df.append(match_df_4, ignore_index=True)
match_df = match_df.append(match_df_5, ignore_index=True)
match_df = match_df.append(match_df_6, ignore_index=True)
match_df = match_df[['treatment_user', 'control_user', 'subreddit']]
match_df.columns = ['treatment_user', 'subreddit', 'control_user']

user_list = match_df['control_user'].to_list()
user_counter = 0  # write to csv every 50 entries, just to save progress in case of crash

for user in user_list:
    try:
    # construct dataset of user
        print('scraping ' + user)
        if user_exists(user) is True:
            user_submissions = list(reddit.redditor(user).submissions.new(limit=None))
            user_comments = list(reddit.redditor(user).comments.new(limit=None))

            for comment in user_comments:
                user_comment_data.append(
                    [comment.author, comment.created_utc, comment.id, comment.body, comment.subreddit,
                     comment.score, comment.parent_id, comment.is_submitter])

            for submission in user_submissions:
                user_submission_data.append([submission.author, submission.created_utc, submission.id, submission.title,
                                             submission.selftext, submission.subreddit, submission.num_comments,
                                             submission.score, submission.upvote_ratio])
            
        if user_counter == 5:
            # write to df
            print("writing!")
            posts_df = pd.DataFrame(posts, columns=['title', 'author', 'score', 'id', 'subreddit', 'url', 'num_comments',
                                                 'body', 'created'])
            user_submission_data_df = pd.DataFrame(user_submission_data, columns=['author', 'datetime', 'id', 'title', 'text',
                                                                               'subreddit', 'num_comments', 'score',
                                                                               'upvote_ratio'])
            user_comment_data_df = pd.DataFrame(user_comment_data, columns=['author', 'datetime', 'id', 'text', 'subreddit',
                                                                         'score', 'parent_id', 'is_submitter'])
    
            # write to csv
            user_submission_data_df.to_csv('submissions_control_12_17.csv')
            user_comment_data_df.to_csv('comments_control_12_17.csv')
    
            user_counter = 0

        else:
            user_counter += 1

    except:
        print("error, sleeping for 10s before continuing")
        time.sleep(10)
        continue


end = time.time()
print("runtime: " + str(end - start))
