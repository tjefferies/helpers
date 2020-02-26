import string
import os
from helpers.SQLite.methods import create_SQLite_connection
from string import Template


def create_string_from_file(file_path):
    """Reads and returns a string from a file path.
    
    Args:
        file_path (:obj:`str`): File path of the file to be read into a string.
    
    Return:
        s (:obj:`str`): String representation of the contents of the input file located at `file_path`.
    """
    with open(file_path, 'r') as file:
        s = file.read().replace('\n', '')
    return s

def populate_cypher_query_template_from_file(cypher_query_template_file_path, print_cypher_query_template=False):
    """Populates and Returns a user provided Cypher query with values read from file specified at `cypher_query_template_file_path`.
    
    Args:
        cypher_query_template_file_path (:obj:`str`): File path of the .cql query with "$"-based substitution characters present.
        print_cypher_query_template (:obj:`bool`): Print string representation of the contents of the input file located at `cypher_query_template_file_path`.

    Uses:
        `create_string_from_file` method

    Return:
        cypher_query_template (:obj:`string.Template`): string.Template with "$"-based substitution characters present.
    """
    cypher_query_template = create_string_from_file(cypher_query_template_file_path)
    if print_cypher_query_template:
        print(cypher_query_template)
    return Template(cypher_query_template)

def populate_cypher_query_template_with_values_in_dict(cypher_query_template, dictionary):
    """Populates and Returns a user provided Cypher query with values plucked from a `dictionary`.
    
    Args:
        cypher_query_template (:obj:`string.Template`): string.Template to populate with values.
        dictionary (:obj:`dict`): dict of form { "$"-based substitution: value to be passed to string.Template }.
    
    Return:
        query (:obj:`str`): Input `cypher_query_template` returned with values passed to "$"-based substitutions present.
    """
    if type(cypher_query_template) != string.Template:
        raise TypeError('cypher_query_template parameter must be string.Template.')
    if type(dictionary) != dict:
        raise TypeError('dictionary parameter must be dict.')
    query = cypher_query_template.substitute(dictionary)
    return query

def list_of_dicts_to_list_of_cypher_queries(cypher_query_template, list_of_dicts):
    """Populates and Returns a list of cypher queries from an input `list_of_dicts`.
    
    Args:
        cypher_query_template (:obj:`string._TemplateMetaclass`): string.Template
        list_of_dicts (:obj:`list` of type `dict`): List of dicts with form { "$"-based substitution: value to be passed to string.Template }
    
    Uses:
        `populate_cypher_query_template_with_values_in_dict` method
    
    Return:
        list_of_cypher_queries (:obj:`list` of type `str`): List of strings of `cypher_query_template` returned with values passed to "$"-based substitutions present.
    """
    
    if type(cypher_query_template) != string.Template:
        raise TypeError('cypher_query_template parameter must be string.Template.')
    if type(list_of_dicts) != list:
        raise TypeError('list_of_dicts parameter must be list of dicts.')
    if not all(isinstance(d,dict) for d in list_of_dicts):
        raise TypeError('list_of_dicts parameter must be list of dicts.')
    
    list_of_cypher_queries = list()
    
    for d in list_of_dicts:
        query = populate_cypher_query_template_with_values_in_dict(cypher_query_template, d)
        list_of_cypher_queries.append(query)
        
    return list_of_cypher_queries

def fill_in_cypher_query_template_from_sqlite_query_results(sqlite_db_path, sqlite_query, cypher_query_template, cypher_statement_delimiter="\n"):
    """ Populates and returns a cypher_query_template populated from an input `sqlite_query`.
        1) Establishes a sqlite3.Connection using sqlite database located at `sqlite_db_path`
        2) Executes `sqlite_query` using a sqlite3.Connection => returns list_of_dicts
        3) Populates Cypher query template `cypher_query_template` using list_of_dicts => returns list_of_cypher_queries
        4) Returns list_of_cypher_queries delimited by `cypher_statement_delimiter`
    
    Args:
        sqlite_db_path (:obj:`str`): Path of a sqlite database to run `sqlite_query` against.
        sqlite_query (:obj:`str`): SQL query to run against `sqlite_db_path`
        cypher_query_template (:obj:`string.Template`): string.Template with "$"-based substitution characters present.
        cypher_statement_delimiter (:obj:`str`): Delimiter to use to seperate Cypher statements
        
    Uses:
        `create_SQLite_connection` method
        `SQLiteQuery` class
            `run_query_return_list_of_dicts` method
        `list_of_dicts_to_list_of_cypher_queries` method
        
    Return:
        cypher_query_string (:obj:`str`): String returned with values passed to "$"-based substitutions present.
    """
    with create_SQLite_connection(sqlite_db_path) as conn:
        list_of_dicts = SQLiteQuery(conn,sqlite_query).run_query_return_list_of_dicts()
    list_of_cypher_queries = list_of_dicts_to_list_of_cypher_queries(cypher_query_template, list_of_dicts)
    cypher_query_string = cypher_statement_delimiter.join(list_of_cypher_queries)
    return cypher_query_string

