import sqlite3
import os
import sys
import pandas as pd

if sys.version_info[0] < 3: 
    from StringIO import StringIO
else:
    from io import StringIO
    
from glob import glob
from sqlite3 import Error


def create_pandas_DataFrame_from_string(source_str, sep=';'):
    """reference: https://stackoverflow.com/a/22605281
    
    Args:
        source_str (:obj:`str`): SQLite Table to land `source_pandas_df` in.
            * Example:
                col1;col2;col3
                1;4.4;99
                2;4.5;200
                3;4.7;65
                4;3.2;140
                
        sep (:obj:`str`): Seperator used to denote column boundaries in `source_str`.

    Returns:
        source_pandas_df (:obj:`pd.core.frame.DataFrame`): Pandas DataFrame created during method call.
    """

    TESTDATA = StringIO(source_str)
    source_pandas_df = pd.read_csv(TESTDATA, sep=sep)
    return source_pandas_df

def create_SQLite_connection(db):
    """Create a database connection to a SQLite database.

    Args:
        db (:obj:`str`): SQLite Table to land `source_pandas_df` in.

    Returns:
        conn (:obj:`sqlite3.Connection`): SQLite database connection.
    """
    if type(db) != str:
        raise TypeError('db parameter must be str.')
    try:
        conn = sqlite3.connect(db)
        print(sqlite3.version)
    except Error as e:
        print(e)
    return conn

def push_pandas_DataFrame_to_SQLite_table(conn, source_pandas_df, destination_sqlite_table, index=False, if_exists='replace'):
    """SQLite method used to push a pandas DataFrame to a SQLite table.

    Args:
        conn (:obj:`sqlite3.Connection`): SQLite database connection created using `create_SQLite_connection` method.
        source_pandas_df (:obj:`pd.core.frame.DataFrame`): Pandas DataFrame to push to SQLite table.
        destination_sqlite_table (:obj:`str`): SQLite Table to land `source_pandas_df` in.
    """
    if type(conn) != sqlite3.Connection:
        raise TypeError('conn parameter must be type sqlite3.Connection.')    

    if type(source_pandas_df) != pd.core.frame.DataFrame  :
        raise TypeError('source_pandas_df parameter must be pd.core.frame.DataFrame.')
        
    if type(destination_sqlite_table) != str:
        raise TypeError('destination_sqlite_table parameter must be str.')
        
    if type(index) != bool:
        raise TypeError('index parameter must be boolean.')
        
    if type(if_exists) != str:
        raise TypeError('if_exists parameter must be str.')
        
    if if_exists not in ('fail', 'replace', 'append'):
        raise ValueError("""if_exists parameter must be in ('fail', 'replace', 'append').""")
        
    source_pandas_df.to_sql(destination_sqlite_table, conn, if_exists=if_exists, index=index)
    print('successfully pushed dataframe to sqlite!')

def checksum_dataframe_with_sqlite_table_same_data_before_and_after_load(conn, source_pandas_df, destination_sqlite_table):
    """Check sums a pandas DataFrame before and after being pushed into SQLite.

    Args:
        conn (:obj:`sqlite3.Connection`): SQLite database connection created using `create_SQLite_connection` method.
        source_pandas_df (:obj:`pd.core.frame.DataFrame`): Pandas DataFrame to compare with SQLite table.
        destination_sqlite_table (:obj:`str`): SQLite table to compare with `source_pandas_df`.
        
    Uses:
        `SQLiteQuery` class
            `run_query_return_list_of_dicts` method
        `pd.DataFrame` class
            `equals` classmethod
    Returns:
        (:obj:`bool`): Boolean `source_pandas_df` equals `destination_sqlite_table`
    """
    if type(conn) != sqlite3.Connection:
        raise TypeError('conn parameter must be type sqlite3.Connection.')    

    if type(source_pandas_df) != pd.core.frame.DataFrame  :
        raise TypeError('source_pandas_df parameter must be pd.core.frame.DataFrame.')
        
    if type(destination_sqlite_table) != str:
        raise TypeError('destination_sqlite_table parameter must be str.')
        
    check_query = "SELECT * FROM {}".format(destination_sqlite_table)
    df_check = pd.DataFrame(SQLiteQuery(conn,check_query).run_query_return_list_of_dicts())
    try:
        assert source_pandas_df.equals(df_check)
        print('dataframes equal before and after load.')
        return True
    except AssertionError:
        print("dataframes not equal before and after load.")
        return False

