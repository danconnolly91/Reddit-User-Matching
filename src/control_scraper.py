import praw
import prawcore
import pandas as pd
import time
from psaw import PushshiftAPI
import datetime as dt
import configparser

def user_exists(name):
    try:
        reddit.redditor(name).id
    except prawcore.exceptions.NotFound:
        return False
    except AttributeError:
        return False
    return True


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


def scrape_control(reddit, user_list, submissionlist, commentlist, controlSubmissionsLocation, controlCommentsLocation):
    user_counter = 0
    for user in user_list:
        try:
            if user_exists(user):
                print("scraping " + user)
                user_counter+=1
                user_submissions = list(reddit.redditor(user).submissions.new(limit=None))
                user_comments = list(reddit.redditor(user).comments.new(limit=None))

                for comment in user_comments:
                    append_comment_to_data(comment, commentlist)

                for submission in user_submissions:
                    append_submission_to_data(submission, submissionlist)

                if user_counter >= max_users:
                    # convert to df
                    print("writing to file")
                    user_submission_df = convert_submissions_to_df(submissionlist)
                    user_comment_df = convert_comments_to_df(commentlist)
                    # write to csv
                    user_submission_df.to_csv(controlSubmissionsLocation)
                    user_comment_df.to_csv(controlCommentsLocation)
                    # reset counter
                    post_counter = 0

                else:
                    post_counter += 1

        except Exception as error:
            print(error + ', moving to next user')
            time.sleep(1)
            continue


config = configparser.ConfigParser()
config.read('config.ini')
config.sections()

reddit = praw.Reddit(client_id=config['scraperSettings']['clientId'], client_secret=config['scraperSettings']['clientSecret'],
                     user_agent=config['scraperSettings']['userAgent'])
api = PushshiftAPI(reddit)

user_submission_data = []
user_comment_data = []
max_users = int(config['scraperSettings']['MAX_CONTROL_USERS_BEFORE_WRITE'])

user_list_location = config['outputPaths']['matchedUsers'] # list so that we don't scrape the same user twice
control_user_list = pd.read_csv(user_list_location)['control_user'].tolist()
controlSubmissionsLocation = config['outputPaths']['controlSubmissions']
controlCommentsLocation = config['outputPaths']['controlComments']

if __name__ == '__main__':
    scrape_control(reddit, control_user_list, user_submission_data, user_comment_data, controlSubmissionsLocation, controlCommentsLocation)
