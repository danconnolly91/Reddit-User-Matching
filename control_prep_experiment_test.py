import pandas as pd
from control_prep_experiment import count_by_author_and_subreddit, get_num_unique_column_in_df, get_median_num_unique_subreddit_per_user, get_author_per_subreddit

empty_df = pd.DataFrame({'author': [], 'subreddit': []})
test_df = pd.DataFrame(
[['a', 'Alice'],
['b', 'Alice'],
['b', 'Bob'],
['b', 'Bob'],
['c', 'Alice'],
['c', 'Bob'],
['c', 'Bob'],
['c', 'Charlie'],
['c', 'Charlie'],
['c', 'Charlie']],
columns=['author', 'subreddit'])
count_by_author_and_subreddit_target = pd.DataFrame(
        [['a', 'Alice', 1],
        ['b', 'Alice', 1],
        ['b', 'Bob', 2],
        ['c', 'Alice', 1],
        ['c', 'Bob', 2],
        ['c', 'Charlie', 3]],
        columns=['author', 'subreddit', 'count']
    )
count_author_per_subreddit_target = pd.DataFrame(
    [['a', 1,0,0],
    ['b',1,2,0],
    ['c',1,2,3]],
    index= ['sub','Alice','Bob','Charlie']
)

# test count_by_author_and_subreddit
def test_count_by_author_and_subreddit_empty():
    assert count_by_author_and_subreddit(empty_df).empty

def test_count_by_author_and_subreddit_reg():
    count_result = count_by_author_and_subreddit(test_df)
    assert count_result.equals(count_by_author_and_subreddit_target)

def test_get_num_unique_column_in_df_empty():
    assert 0 == get_num_unique_column_in_df(empty_df, 'author')

def test_get_num_unique_column_in_df_author():
    assert 3 == get_num_unique_column_in_df(test_df, 'author')

def test_get_median_num_unique_subreddit_per_user():
    assert 2 == get_median_num_unique_subreddit_per_user(test_df)

def test_get_author_per_subreddit():
    res = get_author_per_subreddit(count_by_author_and_subreddit_target)
    assert res.equals(count_author_per_subreddit_target)

