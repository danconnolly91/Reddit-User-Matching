echo "The following configurations are being used";
cat config.ini;
echo "make a directory for outputs";
mkdir -p data;
echo "scraping";
python3 src/scraper.py;
echo "find the most unique subreddit for each author";
python3 src/unique_sr_finder.py;
echo "matching users";
python3 src/match_finder.py;
# for a multiprocessing version, use match_finder2.py (comment the line above and uncomment the line below)
# python3 src/match_finder2.py;
echo "scraping the control group";
python3 src/control_scraper.py