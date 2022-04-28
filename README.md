# Creating Comparison Groups for Event Study on Reddit

The algorithms matches reddit users in steps:

    1. Scrape the subreddits of interest

    2. Scrape the activities of the users whose posts are included in step 1 (treatment group) across all subreddits

    3. Analyze the behavoir of all users to find the most unique subreddit for a given user

    4. Scrape posts in the relevant subreddit to find a match (control) for the treatment user

    5. Scrape the activities of the control group

Steps 1&2 are achieved by `scraper.py`, Step 3 in `unique_sr_finder.py`, Step 4 in `match_finder.py` and Step 5 in `control_scraper.py`

## Instructions

Make sure that you have access to the [Reddit API](https://www.reddit.com/prefs/apps). You will need the 'personal use script' and 'secret'.

Additionally, the script uses `praw, psaw, configparser, numpy and pandas` modules, so please be sure to install those.

To run the script:

1. Input your Reddit API credentials in the `scraperSettings` of config_public.ini 
    -`personal use script` as `clientID`
    -`secret` as `clientSecret`
    -`userAgent` should be something [unique and descriptive](https://github.com/reddit-archive/reddit/wiki/API))

2. Fill in the `treatmentSubreddit` and change other configurations as you see fit 

3. Rename config_public.ini to config.ini

4. Navigate to the root directory in command line

5. `chmod +x user_matching.sh` to make the script executable

6. `./user_matching.sh` to run the script 
    - `match_finder2.py` is a multiprocess version of `match_finder.py`. Use with caution
