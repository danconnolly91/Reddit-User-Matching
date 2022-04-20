import pandas as pd
from control_prep_experiment import count_by_author_and_subreddit, get_num_unique_column_in_df, get_median_num_unique_subreddit_per_user, get_author_per_subreddit, get_sparse_df, normalize_row, average_by_column, get_index_max_of_row, get_most_unique_subreddit_from_counts, get_most_unique_subreddit_from_treatment

empty_df = pd.DataFrame({'author': [], 'subreddit': []})
test_df = pd.DataFrame(
[['Alice', 'subreddit_1'],
['Bob', 'subreddit_1'],
['Bob', 'subreddit_2'],
['Bob', 'subreddit_2'],
['Charlie', 'subreddit_1'],
['Charlie', 'subreddit_2'],
['Charlie', 'subreddit_2'],
['Charlie', 'subreddit_3'],
['Charlie', 'subreddit_3'],
['Charlie', 'subreddit_3']],
columns=['author', 'subreddit'])
count_by_author_and_subreddit_target = pd.DataFrame(
        [['Alice', 'subreddit_1', 1],
        ['Bob', 'subreddit_1', 1],
        ['Bob', 'subreddit_2', 2],
        ['Charlie', 'subreddit_1', 1],
        ['Charlie', 'subreddit_2', 2],
        ['Charlie', 'subreddit_3', 3]],
        columns=['author', 'subreddit', 'count']
    )
count_author_per_subreddit_target = pd.DataFrame(
    [['Alice', 1,0,0],
    ['Bob',1,2,0],
    ['Charlie',1,2,3]],
    columns = ['subreddit','subreddit_1','subreddit_2','subreddit_3']
).set_index('subreddit')

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

def test_get_author_per_subreddit_mini():
    output = get_author_per_subreddit(count_by_author_and_subreddit_target)
    assert output.shape == count_author_per_subreddit_target.shape
    for i in output.index:
        for j in output.columns:
            assert output.at[i,j] == count_author_per_subreddit_target.at[i,j]
            
def test_get_author_per_subreddit_small():
    count_result = count_by_author_and_subreddit(pd.read_csv('TestTreatmentData/test_treatment_small.csv'))
    output = get_author_per_subreddit(count_result)
    target = pd.read_csv('TestTreatmentData/pivot_table_small.csv').set_index('Row Label').fillna(0)
    assert output.shape == target.shape
    for i in output.index:
        for j in output.columns:
            assert output.at[i,j] == target.at[i,j]


def test_sparse_df():
    sp = get_sparse_df(count_author_per_subreddit_target)
    assert sp.sparse.density == 2/3

def test_normalize_row():
    normalized = normalize_row(count_author_per_subreddit_target)
    # sum each row
    normalized['sum_of_row'] = normalized.sum(axis = 1)
    assert sum(normalized['sum_of_row']) == len(normalized)

def test_average_by_column_same():
    input = pd.DataFrame(
        [[1,2,3],
        [1,2,3]],
        columns= ['Alice','Bob','Charlie']
    )
    output = average_by_column(input)
    target = pd.Series({'Alice': 1.0, 'Bob': 2.0, 'Charlie': 3.0})
    assert output.equals(target)

def test_average_by_column_diff():
    input = pd.DataFrame(
        [[1,0.5,0],
        [0,1,0]],
        columns= ['Alice','Bob','Charlie']
    )
    output = average_by_column(input)
    target = pd.Series({'Alice': .5, 'Bob': .75, 'Charlie': 0})
    assert output.equals(target)

def test_get_index_max_of_row_simple():
    input = pd.DataFrame(
        [['Alice', 1, -2,0],
        ['Bob',0,1,0]],
        columns= ['author','subreddit_1','subreddit_2','subreddit_3']
    ).set_index('author')
    output = get_index_max_of_row(input)
    target = pd.Series({
        'Alice':'subreddit_1',
        'Bob':'subreddit_2'}
    )
    assert output.equals(target)

def test_get_index_max_of_row_rep():
    input = pd.DataFrame(
        [['Alice', 1, 1,0],
        ['Bob',0,0,0]],
        columns= ['author','subreddit_1','subreddit_2','subreddit_3']
    ).set_index('author')
    output = get_index_max_of_row(input)
    target = pd.Series({
        'Alice':'subreddit_1',
        'Bob':'subreddit_1'}
    )
    assert output.equals(target)

def test_get_most_unique_subreddit_from_counts():
    output = get_most_unique_subreddit_from_counts(count_author_per_subreddit_target)
    target = pd.Series({
        'Alice':'subreddit_1',
        'Bob':'subreddit_2',
        'Charlie': 'subreddit_3'}
    )
    assert output.equals(target)

def test_get_most_unique_subreddit_from_treatment_mini():
    output = get_most_unique_subreddit_from_treatment("TestTreatmentData/test_treatment_mini.csv")
    target = pd.Series({
        'Alice':'subreddit_1',
        'Bob':'subreddit_2',
        'Charlie': 'subreddit_3'}
    )
    assert output.equals(target)