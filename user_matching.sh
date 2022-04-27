echo "The following configurations are being used";
cat config.ini;
echo "make a directory for outputs";
mkdir -p data;
echo "scraping";
python3 src/scraper.py;
echo "find the most unique subreddit for each author";
python3 src/unique_sr_finder.py;
echo "matching users";
python3 src/match_finder.py