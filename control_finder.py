import praw
import prawcore
import pandas as pd
import time
from psaw import PushshiftAPI
import numpy as np
import math
import json
import requests
import itertools
from datetime import datetime, timedelta

start = time.time()

#helper functions
def user_exists(name):
    try:
        reddit.redditor(name).id
    except prawcore.exceptions.NotFound:
        return False
    except AttributeError:
        return False
    return True


def make_request(uri, max_retries = 5):
    
    def fire_away(uri):
        response = requests.get(uri)
        assert response.status_code == 200
        return json.loads(response.content)
    
    current_tries = 1
    
    while current_tries < max_retries:
        try:
            time.sleep(.5)
            response = fire_away(uri)
            return response
        
        except:
            time.sleep(.5)
            current_tries += 1
            
    return fire_away(uri)


def pull_posts_for_sub(subreddit_of_interest, start_at, end_at):
    
    def map_posts(posts):
        return list(map(lambda post: {
            'id': post['id'],
            'created_utc': post['created_utc'],
        }, posts))
    
    SIZE = 500
    URI_TEMPLATE = r'https://api.pushshift.io/reddit/search/submission?subreddit={}&after={}&before={}&size={}'
    
    post_collections = map_posts( \
        make_request( \
            URI_TEMPLATE.format( \
                subreddit_of_interest, start_at, end_at, SIZE))['data'])
    n = len(post_collections)
    while n == SIZE:
        last = post_collections[-1]
        new_start_at = last['created_utc'] - (10)
        
        more_posts = map_posts( \
            make_request( \
                URI_TEMPLATE.format( \
                    subreddit_of_interest, new_start_at, end_at, SIZE))['data'])
        
        n = len(more_posts)
        post_collections.extend(more_posts)
        
    return post_collections 
    
    
def give_me_intervals(start_at, number_of_days_per_interval = 3):
    
    end_at = math.ceil(datetime.utcnow().timestamp())
        
    ## 1 day = 86400,
    period = (86400 * number_of_days_per_interval)
    end = start_at + period
    yield (int(start_at), int(end))
    padding = 1
    while end <= end_at:
        start_at = end + padding
        end = (start_at - padding) + period
        yield int(start_at), int(end)


def oom(number):
    if number < 0:
       return -(math.floor(math.log(abs(number)+1, 10)))
    else:
        return math.floor(math.log(number+1, 10))
    
def check_match(submission_id, target_attributes):
    """Acceots a reddit submission id and list of target attributes. Checks if the author of the post is a matche on the attributes
    where match is defined as being within an order of magnitude on comment karma and link karma and within a year of account creation"""
    
    try:
        author = reddit.submission(id=submission_id).author
        
        if author==None:
            return None
    
        author_stats = [author.name, oom(author.comment_karma), oom(author.link_karma), author.created_utc]
        loss = [((target_attributes[1]-author_stats[1])==0), ((target_attributes[2]-author_stats[2])==0), 
                                                        ((target_attributes[3]-author_stats[3])<utc_year)]
    except:
        return None
    
    if loss == [True, True, True] :         
        if author.name == target_attributes[0]:
            return None
        else:                                       
            return author.name  
    else:
        return None

path = "C:/Users/Dan/Box/Class/Fall 2020/Large-Scale Social Phenomena/unemployment/"

treatment_df = pd.read_csv('subreddits_for_matching.csv')
treatment_df = treatment_df[6905:] #edit this to start from the index number where you errored out
treatment_dict = dict(zip(treatment_df.author, treatment_df.most_unique_sr))

print(treatment_df)

reddit = praw.Reddit(client_id='ArgFl6Em2AuwoA', client_secret='WP0DaW1B_inHi3Lf8Du8Ag5ag9Y', user_agent='contentScraper')
api = PushshiftAPI(reddit)

matches = []
crawled_reddits = {}
TIMEOUT_AFTER_REQUEST_IN_SECS = .5
utc_year = 31536000000 #difference of 1 year between 2 timestamps
write_counter = 0


for key, value in treatment_dict.items():
    #get list of target attributes from the treatment user
    try:
        target_user = reddit.redditor(key)
        target_stats = [key, oom(target_user.comment_karma), oom(target_user.link_karma), target_user.created_utc]
    except: #if the target user has been banned since, give them an empty match
        print('404, bad target user')
        matches.append([key, value, ''])
        continue
    
    start_at = math.floor(target_stats[3]) #start searching posts around the target user's account creation date
    
    try:
        sr_founding = int(reddit.subreddit(value).created_utc)   #if the subreddit was created after start date, start when the subreddit was founded instead
        if start_at < sr_founding:
            print('revising start date from user birthday ' + str(start_at) + ' to subreddit founding: ' + str(sr_founding))
            start_at = sr_founding
    except:
        pass
        
    #get list of submissions in the most unique subreddit for that user
    print('pulling posts for ' + value)
    num_posts_checked = 0
    intervals = list(give_me_intervals(start_at, 7))
    for interval in intervals[:53]: #check each week for a year. if no user if found after a year, move on
        pulled_posts = pull_posts_for_sub(value, interval[0], interval[1])
        print('pulled ' + str(len(pulled_posts)) + ' posts in ' + str(interval))
        
        for submission_id in np.unique([post['id'] for post in pulled_posts]):
                match = check_match(submission_id, target_stats)
                if match is not None:
                    print('control user found for ' + key + '! the matched user is: ' + match +'. writing match')
                    matches.append([key, value, match])
                    write_counter += 1
                
                    if write_counter == 3: # write to csv
                        print('writing to file')
                        matches_df = pd.DataFrame(matches, columns=['treatment_user', 'control_user', 'subreddit'])
                        matches_df.to_csv('matches_12_17.csv')
                        write_counter = 0
                    
                    break
        
        else:
            num_posts_checked+=len(pulled_posts) 
            continue

        break

end = time.time()
print("runtime: " + str(end - start))
