import os, json
import numpy as np
import pandas as pd

def handle_tuples(parent, key):
    """
    Helper function. 
    Parent can be a tuple or a string.
    Args:
        parent ([tuple, string])
        key ([string])
    Returns:
        [tuple]: [one dimension tuple]
    """    
    if(type(parent)==tuple):
        return tuple([p for p in parent] + [key])
    else:
        return tuple([parent,key])

def handle_appends(original, added):
    """[summary]

    Args:
        original (list): list to be added to
        added (list/other): element/list to be added
    """        
    if(type(added) == list):
        original += added
    else:
        original.append(added)

def get_values(item):
    """Get all values from dictionary of dictionaries.
        Does not accept lists.
    Args:
        item (dict/value): dictionary to be transversed, or value to be returned
    Returns:
        list: all values of the dictionary of dictionaries.
    """    
    list_with_value = []
    assert type(item) != list, 'function get_values() can\'t handle lists'
    
    if(type(item)==dict):
        for key in item:
            if(type(item[key]) == dict):
                handle_appends(list_with_value, get_values(item[key]))
            else:
                handle_appends(list_with_value, item[key])
    else:
        handle_appends(list_with_value, item)

    return list_with_value

def get_columns(item, parent = None):
    """Get path of keys to reach all values from dictionary of dictionaries.
    Does not accept lists.
    Args:
        item (dict/value): dictionary to be transversed, or value
        parent (tuple): tuple with all previous keys
    Returns:
        list: all key paths to all values of the dictionary of dictionaries.
    """    
    list_cols = []
    assert type(item) != list, 'function get_columns() can\'t handle lists'

    if(type(item) == dict):
        for key in item:

            if(type(item[key]) == dict):
                handle_appends(list_cols, get_columns(item[key], key if(parent is None) else handle_tuples(parent, key)))

            else:
                handle_appends(list_cols, key if(parent is None) else handle_tuples(parent, key))

        return list_cols
    return parent

def create_column_name(item_resize):
    """
    Args:
        item_resize ([tuple/string])

    Returns:
        string
    """    
    if(type(item_resize)!=str):
        new_item = '_'.join(item_resize)
    else:
        new_item = str(item_resize)
    return new_item

def traverse_and_flatten(my_panda):
    """
    Args:
        my_panda (panda Dataframe): 
            columns: message, correlationId
            will call get_values() and get_columns() functions and return a 
            dataframe with the obtained columns/values.
            If an element of the 'message' column if a list the it will
            return a row per list element with the other columns duplicated.

    Returns:
        pandas Dataframe: Dataframe with the flattened rows.
    """
    correlationId = my_panda['correlationId'][0]
    parent_columns = list(my_panda.index)

    #values and columns of elements that are not replicatable
    flattened_columns = []
    values_row = []

    #values and columns of elements that are replicatable (lists)
    values_row_replicable = []
    flattened_columns_replicable = []

    #traversing the dataframe
    for col in parent_columns:
        if(type(my_panda['message'][col]) != list): 
            #getting all the values into values_row
            handle_appends(values_row, get_values(my_panda['message'][col]))
            #getting all the columns into flattened_columns
            handle_appends(flattened_columns, get_columns(my_panda['message'][col], col))

        else:
            for element in my_panda['message'][col]:

                values_row_replicable.append([])
                flattened_columns_replicable.append([])
                #getting all the values into values_row_replicable
                handle_appends(values_row_replicable[-1], get_values(element))
                #getting all the columns into flattened_columns_replicable
                handle_appends(flattened_columns_replicable[-1], get_columns(element, col))

                assert (len(values_row_replicable[-1]) == len(flattened_columns_replicable[-1])), 'values: {}, columns: {}'.format(len(values_row), len(flattened_columns)) 

    assert (len(values_row) == len(flattened_columns)), 'values: {}, columns: {}'.format(len(values_row), len(flattened_columns))

    #create dataframe with 1 row/column with the correlationId
    final_df = pd.DataFrame([correlationId], columns=['correlationId'])

    #setting column names in right format from tuple to string snake_case
    normalized_cols = [create_column_name(col) for col in flattened_columns]
    
    assert (len(normalized_cols) == len(set(normalized_cols))), 'repeated elements: {} VS {}'.format(set(normalized_cols), normalized_cols)

    #adding a new column/element from tranversed dataframe, from the non-replicable elements
    for value, col_snake_case in zip(values_row, normalized_cols):
        final_df[col_snake_case] = value

    #adding the replicable elements into new rows
    list_repl_df = []
    
    for r_value, r_col in zip(values_row_replicable, flattened_columns_replicable):
        repl_df = pd.DataFrame([correlationId], columns=['correlationId'])

        #setting column names in right format from tuple to string snake_case
        normalized_r_cols = [create_column_name(col_tuple_format) for col_tuple_format in r_col]

        for value, col_snake_case in zip(r_value, normalized_r_cols):
            repl_df[col_snake_case] = value

        list_repl_df.append(repl_df)
    
    #merging the replicable elements with the non replicable elements
    final_df = final_df.merge(pd.concat(list_repl_df, axis=0), on='correlationId')

    return final_df

if __name__ == "__main__":

    path_to_json = str(os.getcwd()).replace('\\', '/') + "/Blue Energy/18/"
    json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
    pandas_files = []

    for json in json_files:
        test_json = pd.read_json(path_to_json+str(json), orient='columns')
        pandas_files.append(test_json)

    # check if all have same columns
    check_cols = "Index(['correlationId', 'message'], dtype='object')"

    for p, json in zip(pandas_files[1:], json_files[1:]):
        assert (check_cols==str(p.columns)), 'Unexpected JSON format, initial columns do not match. File {}'.format(json)

    final = pd.concat([traverse_and_flatten(my_panda) for my_panda in pandas_files])
    
    final.to_csv('BlueEnergy18.csv', index = False)


