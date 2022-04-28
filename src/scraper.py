import praw
import prawcore
import pandas as pd
import time
from psaw import PushshiftAPI
import datetime as dt
import configparser
from collections import namedtuple

start = time.time()

def user_exists(name):
    try:
        reddit.redditor(name).id
    except prawcore.exceptions.NotFound:
        return False
    except AttributeError:
        return False
    return True

def append_post_to_data(post, post_dataset):
    post_dataset.append(
        [post.title, post.author, post.score, 
        post.id, post.subreddit, post.url, 
        post.num_comments, post.selftext, post.created])
    return post_dataset


def append_submission_to_data(user_submission, user_submission_dataset):
    user_submission_dataset.append(
        [user_submission.title, user_submission.author, user_submission.score, 
        user_submission.id, user_submission.subreddit, user_submission.url, 
        user_submission.num_comments, user_submission.selftext, user_submission.created])
    return user_submission_dataset


def append_comment_to_data(user_comment, user_comment_dataset):
    user_comment_dataset.append(
        [user_comment.author, user_comment.created_utc, user_comment.id, 
        user_comment.body, user_comment.subreddit, user_comment.score, 
        user_comment.parent_id, user_comment.is_submitter])
    return user_comment_dataset


def convert_posts_to_df(posts, dataCols=['title', 'author', 'score', 'id', 'subreddit', 'url', 'num_comments', 'body', 'created']):
    try:
        post_df = pd.DataFrame(posts, columns=dataCols)
    except ValueError:
        post_df = pd.DataFrame(posts)
    return post_df


def convert_submissions_to_df(user_submission_data, dataCols=['title', 'author', 'score', 'id', 'subreddit', 'url', 'num_comments', 'body', 'created']):
    try:
        submission_df = pd.DataFrame(user_submission_data, columns=dataCols)
    except ValueError:
        submission_df = pd.DataFrame(user_submission_data)
    return submission_df


def convert_comments_to_df(user_comment_data, dataCols=['author', 'datetime', 'id', 'text', 'subreddit','score', 'parent_id', 'is_submitter']):
    try:
        comment_df = pd.DataFrame(user_comment_data, columns=dataCols)
    except ValueError:
        comment_df = pd.DataFrame(user_comment_data)
    return comment_df


def scrape(reddit, post_data, user_submission_data, user_comment_data, user_list, post_list, post_counter,treatmentPostsLocation,treatmentSubmissionsLocation,treatmentCommentsLocation):
    for post in post_list:
        append_post_to_data(post, post_data)
        try:
        # construct dataset of user
            user = str(post.author)
            print('scraping posts for user: ' + user)
            if user not in user_list and user_exists(user):
                user_list.append(user)
                user_submissions = list(reddit.redditor(user).submissions.new(limit=None))
                user_comments = list(reddit.redditor(user).comments.new(limit=None))

                for comment in user_comments:
                    append_comment_to_data(comment, user_comment_data)

                for submission in user_submissions:
                    append_submission_to_data(submission, user_submission_data)

            if post_counter >= max_posts:
            # write to df
                print("writing!")
                posts_df = convert_posts_to_df(post_data)
                user_submission_df = convert_submissions_to_df(user_submission_data)
                user_comment_df = convert_comments_to_df(user_comment_data)
            # write to csv
                posts_df.to_csv(treatmentPostsLocation)
                user_submission_df.to_csv(treatmentSubmissionsLocation)
                user_comment_df.to_csv(treatmentCommentsLocation)
            # reset counter
                post_counter = 0

            else:
                post_counter += 1

        except Exception as error:
            print(error)
            time.sleep(1)
            continue


config = configparser.ConfigParser()
config.read('config.ini')
config.sections()

reddit = praw.Reddit(client_id=config['scraperSettings']['clientId'], client_secret=config['scraperSettings']['clientSecret'],
                     user_agent=config['scraperSettings']['userAgent'])
api = PushshiftAPI(reddit)

post_dataset = []
user_submission_data = []
user_comment_data = []
user_list = []  # list so that we don't scrape the same user twice
post_counter = 0  
max_posts = int(config['scraperSettings']['MAX_POSTS_BEFORE_WRITE']) # write to csv every 50 entries, just to save progress in case of crash
treatmentSubreddit = reddit.subreddit(config['scraperSettings']['treatmentSubreddit'])

startYear = int(config['scraperSettings']['startYear'])
startMonth = int(config['scraperSettings']['startMonth'])
startDay = int(config['scraperSettings']['startDay'])
start_epoch = int(dt.datetime(startYear, startMonth, startDay).timestamp())

treatmentPostsLocation = config['outputPaths']['treatmentPosts']
treatmentSubmissionsLocation = config['outputPaths']['treatmentSubmissions']
treatmentCommentsLocation = config['outputPaths']['treatmentComments']

post_list = list(api.search_submissions(after=start_epoch,
                                        subreddit=treatmentSubreddit,
                                        filter=['url', 'author', 'title', 'subreddit']))

if __name__ == '__main__':
    scrape(reddit, post_dataset, user_submission_data, user_comment_data, user_list, post_list, post_counter,treatmentPostsLocation,treatmentSubmissionsLocation,treatmentCommentsLocation)

end = time.time()
print("runtime: " + str(end - start))