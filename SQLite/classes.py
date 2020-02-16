import sqlite3
import os
import pandas as pd
    
from glob import glob
from sqlite3 import Error


class SQLiteQuery(object):
    def __init__(self, conn, query):
        """SQLite class used to query.
        
        Args:
            conn (:obj:`sqlite3.Connection`): SQLite database connection created using `create_SQLite_connection` method.
            query (:obj:`str`): Query to run using `sqlite3.Connection` object.
        """
        self._conn = conn
        self._query = query
        
    def run_query(self):
        """Runs self._query using self._conn.
        
        Uses:
            self._conn (:obj:`sqlite3.Connection`): SQLite database connection created using `create_SQLite_connection` method.
            self._query (:obj:`str`): Query to run using `sqlite3.Connection` object.
        """
        cursor = self._conn.cursor()
        cursor.execute(self._query)
        print('query ran successfully!')
        cursor.close()
        
    def run_query_return_list_of_dicts(self):
        """Runs self._query using self._conn returns a list_of_dicts.
        
        Uses:
            self._conn (:obj:`sqlite3.Connection`): SQLite database connection created using `create_SQLite_connection` method.
            self._query (:obj:`str`): Query to run using `sqlite3.Connection` object.
            
        Returns:
            list_of_dicts (:obj:`list` of type `dict`): List of dictionaries returned by running self._query
        """
        list_of_dicts = list()
        cursor = self._conn.cursor()
        
        for row in cursor.execute(self._query):
            d = dict(zip(next(zip(*cursor.description)), row))
            list_of_dicts.append(d)
            
        cursor.close()
        return list_of_dicts
    
    @property
    def query(self):
        """query (:obj:`str`): Query to run using `sqlite3.Connection`."""
        return self._query

    @query.setter
    def query(self, query):
        if type(query) != str:
            raise TypeError('query parameter must be str.')
        self._query = query

    @property
    def conn(self):
        """conn (:obj:`sqlite3.Connection`): SQLite database connection created using `create_SQLite_connection` method."""
        return self._conn

    @conn.setter
    def conn(self, conn):
        if type(conn) != sqlite3.Connection:
            raise TypeError('conn parameter must be type sqlite3.Connection.')
        self._conn = conn


def main():

    dat = """id;name;begin_date;end_date
    1;fam training;2020-02-01;2020-02-10
    2;project1;2020-02-01;2020-03-01
    3;project2;2020-02-15;2020-04-01
    4;project3;2020-03-15;2020-05-01"""

    df = create_pandas_DataFrame_from_string(dat)
    
    dat2 = """id;name;priority;status_id;project_id;begin_date;end_date
    3;task3;1;0;1;2020-02-15;2020-03-01
    4;task4;1;0;1;2020-02-15;2020-03-01"""

    df2 = create_pandas_DataFrame_from_string(dat2)
    
    temp_db_path = os.path.join(os.getcwd(),'temp.db')
    
    check_query = "SELECT * FROM projects"
    check_query2 = "SELECT * FROM tasks"
    with create_SQLite_connection(temp_db_path) as conn:
        push_pandas_DataFrame_to_SQLite_table(conn,df,'projects',if_exists='replace')
        push_pandas_DataFrame_to_SQLite_table(conn,df2,'tasks',if_exists='replace')
        df_check = pd.DataFrame(SQLiteQuery(conn,check_query).run_query_return_list_of_dicts())
        df2_check = pd.DataFrame(SQLiteQuery(conn,check_query2).run_query_return_list_of_dicts())
        
    assert df.equals(df_check)
    assert df2.equals(df2_check)
    
    print('dataframes equal before and after.')
    
# if __name__ == '__main__':
    # main()

