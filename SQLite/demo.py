from classes import SQLiteQuery
from methods import create_pandas_DataFrame_from_string, create_SQLite_connection, push_pandas_DataFrame_to_SQLite_table


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

