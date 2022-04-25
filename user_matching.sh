echo "The following configurations are being used";
cat config.ini;
echo "make a directory for outputs";
mkdir Data;
echo "scraping";
python scraper.py;
echo "find the most unique subreddit for each author";
python unique_sr_finder.py;
echo "matching users";
python match_finder.py