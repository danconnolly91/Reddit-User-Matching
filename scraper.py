import praw
import prawcore
import pandas as pd
import pprint
import time
from psaw import PushshiftAPI
import datetime as dt
import configparser

start = time.time()

config = configparser.ConfigParser()
config.read('config.ini')
config.sections()

def user_exists(name):
    try:
        reddit.redditor(name).id
    except prawcore.exceptions.NotFound:
        return False
    except AttributeError:
        return False
    return True


def append_submission_to_data(user_submission, user_submission_dataset):
    user_submission_dataset.append([user_submission.title, user_submission.author, 
    user_submission.score, user_submission.id, user_submission.subreddit, 
    user_submission.url, user_submission.num_comments, user_submission.selftext, 
    user_submission.created])


def append_comment_to_data(user_comment, user_comment_dataset):
    user_comment_dataset.append(
        [user_comment.author, user_comment.created_utc, user_comment.id, 
        user_comment.body, user_comment.subreddit, user_comment.score, 
        user_comment.parent_id, user_comment.is_submitter])


def convert_posts_to_df(posts, dataCols=['title', 'author', 'score', 'id', 'subreddit', 'url', 'num_comments', 'body', 'created']):
    post_df = pd.DataFrame(posts, columns=dataCols)
    return post_df


def convert_submissions_to_df(user_submission_data, dataCols=['author', 'datetime', 'id', 'title', 'text', 'subreddit', 'num_comments', 'score', 'upvote_ratio']):
    submission_df = pd.DataFrame(user_submission_data, columns=dataCols)
    return submission_df


def convert_comments_to_df(user_comment_data, dataCols=['author', 'datetime', 'id', 'text', 'subreddit','score', 'parent_id', 'is_submitter']):
    comment_df = pd.DataFrame(user_comment_data, columns=dataCols)
    return comment_df

reddit = praw.Reddit(client_id=config['SCRAPER SETTINGS']['clientId'], client_secret=config['SCRAPER SETTINGS']['clientSecret'],
                     user_agent=config['SCRAPER SETTINGS']['userAgent'])
api = PushshiftAPI(reddit)

posts = []
user_submission_data = []
user_comment_data = []
user_list = []  # list so that we don't scrape the same user twice
max_posts = config['SCRAPER SETTINGS']['MAX_POSTS_BEFORE_WRITE']
treatmentSubreddit = reddit.subreddit(config['SCRAPER SETTINGS']['treatmentSubreddit'])

startYear = int(config['SCRAPER SETTINGS']['startYear'])
startMonth = int(config['SCRAPER SETTINGS']['startMonth'])
startDay = int(config['SCRAPER SETTINGS']['startDay'])
start_epoch = int(dt.datetime(startYear, startMonth, startDay).timestamp())

post_list = list(api.search_submissions(after=start_epoch,
                                        subreddit=treatmentSubreddit,
                                        filter=['url', 'author', 'title', 'subreddit']))

post_counter = 0  # write to csv every 50 entries, just to save progress in case of crash
def scrape(reddit, posts, user_submission_data, user_comment_data, user_list, post_list, post_counter):
    for post in post_list:
        try:
        # construct dataset of user
            user = str(post.author)
            if user not in user_list and user_exists(user):
                user_list.append(user)
                user_submissions = list(reddit.redditor(user).submissions.new(limit=None))
                user_comments = list(reddit.redditor(user).comments.new(limit=None))

                for comment in user_comments:
                    append_comment_to_data(comment, user_comment_data)

                for submission in user_submissions:
                    append_submission_to_data(submission, user_submission_data)

            if post_counter == max_posts:
            # write to df
                print("writing!")
                posts_df = convert_posts_to_df(posts)
                user_submission_df = convert_submissions_to_df(user_submission_data)
                user_comment_df = convert_comments_to_df(user_comment_data)
            # write to csv
                posts_df.to_csv('posts.csv')
                user_submission_df.to_csv('submissions.csv')
                user_comment_df.to_csv('comments.csv')
            # reset counter
                post_counter = 0

            else:
                post_counter += 1

        except RequestException:
            print("timed out, sleeping for a second before continuing")
            sleep(1)
            pass
            continue

        except ReadTimeoutError:
            print("timed out, sleeping for a second before continuing")
            sleep(1)
            pass
            continue

scrape(reddit, posts, user_submission_data, user_comment_data, user_list, post_list, post_counter)

end = time.time()
print("runtime: " + str(end - start))
