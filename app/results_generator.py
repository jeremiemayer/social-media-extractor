from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pyodbc
from api_extractor_utils import load_credentials, log
from api_extractor_config import *
from sqlalchemy.schema import CreateSchema

import time
import csv
import urllib

import threading

NUMBER_OF_THREADS = 1

class insertThread(threading.Thread):
    def __init__(self, conn, dict_list, tableclass):
        threading.Thread.__init__(self)
        #self.threadID = threadID
        self.conn = conn
        self.dict_list = dict_list
        self.tableclass = tableclass

    def run(self):
        chunk_size = 1000

        for chunk_start in range(0, len(self.dict_list), chunk_size):
            chunk = self.dict_list[chunk_start:chunk_start+chunk_size]
            self.conn.execute(self.tableclass.__table__.insert().values(chunk))


#-----------------------------------------------------------------------
# configs
#-----------------------------------------------------------------------
MSSQL_COLUMN_MAX = 4000 #4000 characters is max sql field length

def get_engine(host,dbname,user,password,port=1433,driver='mssql+pyodbc'): #MSSQL driver and port by default
    "Helper function for creating db connection"
    # SQL Alchemy URI format'driver://user:password@hostname:port/database_name'
    #engine = create_engine('{}://{}:{}@{}:{}/{}'.format(driver,user,password,host,port,dbname), pool_size=25, max_overflow=0)
    engine = create_engine(
    'mssql+pyodbc:///?odbc_connect=%s?charset=utf8' % (
        urllib.parse.quote_plus(
            'DRIVER={FreeTDS};SERVER=192.168.0.1;'
            'DATABASE=db;UID=user;PWD=password;port=1433;'
            'TDS_Version=4.2;')),encoding="utf8")
    #conn = engine.connect()
    #Session = sessionmaker(bind=engine)
    #session = Session() # I wonder if we can collapse these using a Lambda
    return engine

def get_session(engine):
    #generate a db session
    Session = sessionmaker(bind=engine) #returns a session generator
    session = Session()
    return session

def get_conn(engine):
    #generate a db connection
    conn = engine.connect()
    return conn

def dict_list_cleaner(dict_list):
    "mutate all dict values to strings"
    for dict in dict_list:
        for k in dict:
            dict[k] = str(dict[k]).encode('unicode-escape') #.encode('utf-8') # 
    return dict_list

def dict_attributes(dict_list):
    "return dictionaries attributes required to generate table schema"
    fields_dict = {}
    fields = list(set().union(*(dict.keys() for dict in dict_list))) #create a unique set of all dict keys
    fields.sort()
    for field in fields:
        max_val = []
        #find the length of every value for a given key
        for dict in dict_list:
            if field in dict:
                max_val.append(len(dict[field]))
        #set column size based on the largest value for a given key
        column_size = max(max_val)
        if column_size > MSSQL_COLUMN_MAX :
            fields_dict[field] = 0 #force default to MAX when assigned as a column
        elif column_size > 0: 
            fields_dict[field] = column_size
        else:
            fields_dict[field] = 1 # give field length a minimum of 1 to prevent default to MAX
    return fields_dict

def create_schema(tablename,dict_list): #TODO figure out how to sort columns - ordered dicts etc. seem to have no effect
    "Create sqlalchemy table from dictionary specified columns"
    fields_dict = dict_attributes(dict_list) #Create a dictionary of the required table attributes - clever but not quite wizardry
    attr_dict = dict(((field,Column(String(fields_dict[field])) ) for field in fields_dict)) #turn each key into a String(varchar) column using the max lengths from fields_dict
    attr_dict['Autogen_ID'] = Column(Integer, primary_key=True) # add a PK - SQLAlchemy demands this
    attr_dict['__tablename__'] = tablename #tablename assignment is done inside the dictionary
    attr_dict['__table_args__'] = {'schema' : SCHEMA_NAME}
    return attr_dict

def create_table(engine,attr_dict,drop=False): #TODO order the fields
    "Create table from dictionary attributes"

    def get_class_by_tablename(tablename):
        """Return class reference mapped to table.

        :param tablename: String with name of table.
        :return: Class reference or None.
        """
        for c in Base._decl_class_registry.values():
            if hasattr(c, '__tablename__') and c.__tablename__ == tablename:
                return c
        return None

    #Dark, dark SQLAlchemy metaclass magic - taken from http://sparrigan.github.io/sql/sqla/2016/01/03/dynamic-tables.html
    Base = declarative_base() #Required for 'declarative' use of the SQLAlchemy ORM
    
    GenericTableClass = type('GenericTableClass', (Base,), attr_dict) #Use of Type() to dynamically generate columns. More detail here: http://sahandsaba.com/python-classes-metaclasses.html#metaclasses
    
    if drop:
        Base.metadata.drop_all(engine) #Drop all tables in the scope of this metadata
 
    Base.metadata.create_all(engine) #Create all tables in the scope of this metadata
    return GenericTableClass


def insert_records(tableclass,engine,dict_list):
    #generate a db session
    Session = sessionmaker(bind=engine) #returns a session generator
    session = Session()

    # Export records: 1 dict = 1 row - allowing for unstructured documents to be uploaded
    i = 0

    t0 = time.time()

    numb_records = len(dict_list)
    each_thread = int(numb_records / NUMBER_OF_THREADS)

    threads = []
    for i in range(NUMBER_OF_THREADS):
            if DEBUG:
                print("thread number: ", i)
            conn = engine.connect()
            if i == (NUMBER_OF_THREADS - 1):
                new_thread = insertThread(conn,
                    dict_list[i*each_thread:],
                    tableclass)
            else:
                new_thread = insertThread(conn,
                    dict_list[i*each_thread : (i+1)*each_thread],
                    tableclass)
            threads.append(new_thread)

    for th in threads:
        th.start()

    for th in threads:
        th.join()

    t1 = time.time()
    return '{} rows inserted (in {:.3f} seconds)'.format(len(dict_list), t1 - t0)

def export_to_db(tablename,engine,dict_list,drop=False,schema=None):
    # create Lotus Config table
    clean_dict_list = dict_list_cleaner(dict_list)

    # TODO: Make this rubust
    # try:
    # engine.execute(CreateSchema(SCHEMA_NAME)) # schema here is a collection of tables
    # except:
    #     log("(Probably) schema already exitst.")
    

    # but here it's the specification for columns of a table
    if schema:
        attr_dict = schema
    else:
        attr_dict = create_schema(tablename,clean_dict_list)

    tableclass = create_table(engine,attr_dict,drop=drop)
    

    results = insert_records(tableclass,engine,clean_dict_list)
    return results

def connect_to_db():
    """Connect to the MSSQL DB"""
    log('Connecting to SQL server...')

    credentials = load_credentials('db')

    MSSQL_HOST = credentials['MSSQL_HOST']
    MSSQL_DB = credentials['MSSQL_DB']
    MSSQL_USER = credentials['MSSQL_USER']
    MSSQL_PASS = credentials['MSSQL_PASS']
    MSSQL_PORT = int(credentials['MSSQL_PORT'])
    MSSQL_DRIVER = credentials['MSSQL_DRIVER']

    mssql_engine = get_engine(MSSQL_HOST,MSSQL_DB,MSSQL_USER,MSSQL_PASS,port=MSSQL_PORT,driver=MSSQL_DRIVER)

    return mssql_engine


def update_db(records, tablename, mssql_engine, schema=None, drop=False):
    """Export the records to a table"""
    log("Exporting data to '%s'..." % tablename)
    export_results = export_to_db(tablename,
        mssql_engine,
        records,
        drop=CREATE_TABLES or drop,
        schema=schema)
    log(export_results)

def run_sql_script(engine, script_str):
    conn = engine.connect()
    return conn.execute(script_str)