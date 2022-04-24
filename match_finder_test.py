import pandas as pd
import praw
from psaw import PushshiftAPI
from match_finder import map_posts, give_me_intervals, oom, check_match
from collections import namedtuple
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
config.sections()

UNIQUE_SR_PATH = config['outputPaths']['userMatchingReddits']
MATCHES_FILE_PATH = config['outputPaths']['matchedUsers']
TIMEOUT_AFTER_REQUEST_IN_SECS = float(config['matchSettings']['TIMEOUT_AFTER_REQUEST_IN_SECS'])
SEARCH_WINDOW_IN_WKS = int(config['matchSettings']['SEARCH_WINDOW_IN_WKS'])
MAX_MATCHES_BEFORE_WRITE = int(config['matchSettings']['MAX_MATCHES_BEFORE_WRITE'])
utc_year = 31536000000 #difference of 1 year between 2 timestamps

reddit = praw.Reddit(client_id=config['scraperSettings']['clientId'], client_secret=config['scraperSettings']['clientSecret'],
                     user_agent=config['scraperSettings']['userAgent'])
api = PushshiftAPI(reddit)

RedditUser = namedtuple('RedditUser', 'name comment_karma link_karma created_utc')
noneUser = RedditUser('noneUser', None, None, None)
newUser = RedditUser('newName', 0, 0, 1650762588156)
oldUser = RedditUser('oldName', 1000, 1000, 18000000)

# test oom
def test_oom_1():
    assert oom(1)==0

def test_oom_10():
    assert oom(10)==1

def test_oom_neg1():
    assert oom(-1)==0

def test_oom_neg10():
    assert oom(-10)==-1

#test matching function
def test_checkmatch_none():
    assert check_match(noneUser, ['treatmentName', None, None, None])==None

def test_checkmatch_new():
    assert check_match(newUser, ['treatmentName', 0, 0, 1650762588156])=='newName'

def test_checkmatch_old():
    assert check_match(oldUser, ['treatmentName', 3, 3, 18000000])=='oldName'

    
