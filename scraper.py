import praw
import prawcore
import pandas as pd
import pprint
import time
from psaw import PushshiftAPI
import datetime as dt

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
user_list = []  # list so that we don't scrape the same user twice
runemployment = reddit.subreddit('unemployment')

start = time.time()

start_epoch = int(dt.datetime(2009, 1, 1).timestamp())

post_list = list(api.search_submissions(after=start_epoch,
                                        subreddit='unemployment',
                                        filter=['url', 'author', 'title', 'subreddit']))

post_counter = 0  # write to csv every 50 entries, just to save progress in case of crash

for post in post_list:
    try:
    # construct dataset of user
        user = str(post.author)
        if user not in user_list:
            user_list.append(user)
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

        # add post to r/unemployment metadata set
        posts.append([post.title, post.author, post.score, post.id, post.subreddit, post.url,
                      post.num_comments, post.selftext, post.created])

        if post_counter == 50:
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
            posts_df.to_csv('posts.csv')
            user_submission_data_df.to_csv('submissions.csv')
            user_comment_data_df.to_csv('comments.csv')

            post_counter = 0

        else:
            post_counter += 1

    except RequestException:
        print("timed out, sleeping for a minute before continuing")
        sleep(60)
        pass
        continue

    except ReadTimeoutError:
        print("timed out, sleeping for a minute before continuing")
        sleep(60)
        pass
        continue

end = time.time()
print("runtime: " + str(end - start))
