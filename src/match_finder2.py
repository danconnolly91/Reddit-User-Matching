import praw
import pandas as pd
import time
from psaw import PushshiftAPI
import numpy as np
import math
import json
import requests
from datetime import datetime
import configparser
import multiprocessing as mp

# This is the number of process for multiprocessing -- Reddit API seems to not like it with 8 threads
THREADNUM = mp.cpu_count()
SIZE = 500
URI_TEMPLATE = r'https://api.pushshift.io/reddit/search/submission?subreddit={}&after={}&before={}&size={}'

#helper functions
def make_request(uri, max_retries = 5):
    def fire_away(uri):
        response = requests.get(uri)
        assert response.status_code == 200
        return json.loads(response.content)

    current_tries = 0
    
    while current_tries < max_retries:
        try:
            response = fire_away(uri)
            return response
        
        except:
            time.sleep(.5)
            current_tries += 1
     
    return {'data': None}


def map_posts(posts):
        return list(map(lambda post: {
            'id': post['id'],
            'created_utc': post['created_utc'],
        }, posts))


def pull_posts_for_sub(subreddit_of_interest, start_at, end_at):
    
    requested_posts = make_request(URI_TEMPLATE.format(subreddit_of_interest, start_at, end_at, SIZE))['data']
    if (requested_posts == None):
        n = -1
        post_collections = list()
    else:
        post_collections = map_posts(requested_posts)  
        n = len(post_collections)

    #if we collect the max number of posts, collect more
    while n == SIZE:
        last = post_collections[-1]
        new_start_at = last['created_utc'] - (10)
        
        more_requests = make_request(URI_TEMPLATE.format(subreddit_of_interest, new_start_at, end_at, SIZE))['data']
        if more_requests == None:
            n = -1
        else:
            more_posts = map_posts(more_requests)
        
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
    

def check_match(potential_match, target_attributes):
    """Accepts a reddit submission id and list of target attributes. Checks if the author of the post is a matche on the attributes
    where match is defined as being within an order of magnitude on comment karma and link karma and within a year of account creation"""
    
    try:
        if potential_match==None:
            return None
    
        author_stats = [potential_match.name, oom(potential_match.comment_karma), oom(potential_match.link_karma), potential_match.created_utc]
        loss = [((target_attributes[1]-author_stats[1])==0), ((target_attributes[2]-author_stats[2])==0), 
                                                        ((target_attributes[3]-author_stats[3])<utc_year)]
    except:
        return None
    
    if loss == [True, True, True] :         
        if potential_match.name == target_attributes[0]:
            return None
        else:                                       
            return potential_match.name  
    else:
        return None


def check_start_date(reddit, subname, start_at):
    """if the subreddit was created after  the treamtnet user's
    account start date, start when the subreddit was founded instead"""
    try:
        sr_founding = int(reddit.subreddit(subname).created_utc)   
        if start_at < sr_founding:
            start_at = sr_founding
    except:
        pass
    return start_at


def get_target_stats(treatment_username):
    try:
        target_user = reddit.redditor(treatment_username)
        target_stats = [treatment_username, oom(target_user.comment_karma), oom(target_user.link_karma), target_user.created_utc]
        return target_stats
    #if the target user has been banned since, give them an empty match
    except Exception as error: 
        target_stats = [None, None, None, None]
        return target_stats


def check_posts_for_match(target_stats_for_match, pulled_posts):
    for submission_id in np.unique([post['id'] for post in pulled_posts]):
            author = reddit.submission(id=submission_id).author
            match = check_match(author, target_stats_for_match)

            if match is not None:
                # record_match(list_of_matches, counter, treatment_username, matching_sr, match)
                return match
    return None


def match_a_user(user_sr_pair):
    treatment_user = user_sr_pair[0]
    unique_sr = user_sr_pair[1]
    print(treatment_user,unique_sr)
    target_stats = get_target_stats(treatment_user)
    if target_stats == [None, None, None, None]:
        return None

    #start searching posts around the target user's account creation date 
    start_at = int(TREATMENT_DF.loc[TREATMENT_DF['author'] == treatment_user,'created']) 
    # start_at = math.floor(target_stats[3]) 
    # start_at = check_start_date(reddit, unique_sr, start_at)
        
    #get list of submissions in the most unique subreddit for that user
    print('pulling posts for ' + treatment_user + ' in '+ unique_sr)
    intervals = list(give_me_intervals(start_at, 7))
    for interval in intervals[:SEARCH_WINDOW_IN_WKS+1]: #check as many weeks as config file says to. if no user is found within the window, move on
        pulled_posts = pull_posts_for_sub(unique_sr, interval[0], interval[1])
        print('pulled ' + str(len(pulled_posts)) + ' posts in ' + str(interval))
        
        match_in_interval = check_posts_for_match(target_stats, pulled_posts)
        if match_in_interval is not None:
            break
    return (treatment_user,match_in_interval,unique_sr)

"""
Load configurations
"""
config = configparser.ConfigParser()
config.read('config.ini')
config.sections()

UNIQUE_SR_PATH = config['outputPaths']['userMatchingReddits']
MATCHES_FILE_PATH = config['outputPaths']['matchedUsers']
TIMEOUT_AFTER_REQUEST_IN_SECS = float(config['matchSettings']['TIMEOUT_AFTER_REQUEST_IN_SECS'])
SEARCH_WINDOW_IN_WKS = int(config['matchSettings']['SEARCH_WINDOW_IN_WKS'])
MAX_MATCHES_BEFORE_WRITE = int(config['matchSettings']['MAX_MATCHES_BEFORE_WRITE'])
utc_year = 31536000000 #difference of 1 year between 2 timestamps

TREATMENT_DF = pd.read_csv(UNIQUE_SR_PATH)
treatment_dict = dict(zip(TREATMENT_DF['author'], TREATMENT_DF['most_unique_sr']))
treatment_pair = TREATMENT_DF[['author','most_unique_sr']]

reddit = praw.Reddit(client_id=config['scraperSettings']['clientId'], client_secret=config['scraperSettings']['clientSecret'],
                     user_agent=config['scraperSettings']['userAgent'])
api = PushshiftAPI(reddit)

if __name__ == '__main__':
    start = time.time()
    # matches = match_users(treatment_dict)
    pool = mp.Pool(THREADNUM)
    result = pool.map(match_a_user, treatment_dict.items())
    result_df = pd.DataFrame(result, columns=['treatment_user','control_user','subreddit'])
    result_df = result_df.dropna().reset_index()
    result_df.to_csv(MATCHES_FILE_PATH)
    end = time.time()
    print("runtime: " + str(end - start))
