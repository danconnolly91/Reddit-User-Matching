from xml.etree.ElementTree import Comment
import pandas as pd
from scraper import user_exists, append_submission_to_data, append_comment_to_data, convert_posts_to_df, convert_submissions_to_df, convert_comments_to_df
from collections import namedtuple

Comment = namedtuple('Comment', 'author created_utc id body subreddit score parent_id is_submitter')
comment_empty = Comment('','','','','','','','')
comment_zerostr = Comment('0','0','0','0','0','0','0','0')
comment_zeroint =  Comment(0,0,0,0,0,0,0,0)

Submission = namedtuple('Submission', 'title author score id subreddit url num_comments selftext created')
submission_empty = Submission('','','','','','','','', '')
submission_zerostr = Submission('0','0','0','0','0','0','0','0', '0')
submission_zeroint =  Submission(0,0,0,0,0,0,0,0,0)

post_data_empty_str = [['']]

# test append_comment_to_data
def test_add_empty_comment():
    assert append_comment_to_data(comment_empty, [])==[['', '', '', '', '', '', '', '']]

def test_add_zerostr_comment():
    assert append_comment_to_data(comment_zerostr, [])==[['0', '0', '0', '0', '0', '0', '0', '0']]

def test_add_zeroint_comment():
    assert append_comment_to_data(comment_zeroint, [])==[[0,0,0,0,0,0,0,0]]

# test append_submission_to_data
def test_add_empty_submission():
    assert append_submission_to_data(submission_empty, [])==[['', '', '', '', '', '', '', '', '']]

def test_add_zerostr_submission():
    assert append_submission_to_data(submission_zerostr, [])==[['0', '0', '0', '0', '0', '0', '0', '0', '0']]

def test_add_zeroint_submission():
    assert append_submission_to_data(submission_zeroint, [])==[[0,0,0,0,0,0,0,0,0]]

# test convert_posts_to_df
def test_convert_posts_type():
    assert isinstance(convert_posts_to_df(post_data_empty_str), pd.DataFrame)

def test_convert_posts_type():
    assert convert_posts_to_df(post_data_empty_str).shape==(1, 1)

# test convert_submissions_to_df
def test_convert_submissions_type():
    assert isinstance(convert_submissions_to_df(post_data_empty_str), pd.DataFrame)

def test_convert_submissions_type():
    assert convert_submissions_to_df(post_data_empty_str).shape==(1, 1)

# test convert_submissions_to_df
def test_convert_comments_type():
    assert isinstance(convert_comments_to_df(post_data_empty_str), pd.DataFrame)

def test_convert_comments_type():
    assert convert_comments_to_df(post_data_empty_str).shape==(1, 1)



    
