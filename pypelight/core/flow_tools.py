import os
import pandas as pd
import numpy as np
import math
import shutil
import datetime
from timeit import default_timer as timer
from sqlalchemy import types
from core.path_setup import create_path
from core.duplicate_tester import clean_df_db_dups
from pyodbc import OperationalError

'''ETL toolset that allows construction of easy work flows'''


def extract(script, connection, **kwargs):
    ''' 
    Extracts information via scripting the database using sql and stores results within a DataFrame. Adapted for SQL Server.
    Parameters:
    --------------
        script: Sentence to script database. Must be a SELECT.
        cnxn: Connection object to query the database. Schema must be already specified.
    Optional Parameters:
    --------------
        dbtype: Under construction. Functionality must be extended beyond SQL Server
        date: In case you desire the date to appear withing printing statement
    '''
    dbtype = None
    date = None

    for name, value in kwargs.items():
        if name == 'dbtype':
            dbtype = value
        elif name == 'date':
            name = value

    start = timer()
    df0 = connection.select(script)
    end = timer() - start

    if date != None:
        print(
            f'{date} {df0.shape[0]} registers read in {str(datetime.timedelta(seconds=(end-start)))}'
        )
    else:
        print(
            f'{df0.shape[0]} registers read in {str(datetime.timedelta(seconds=(end-start)))}'
        )
    return df0


def transform(df):
    '''
    Applies basic adjustments into DataFrame for uploading into database.
    It is recommended that this function is always executed for good practices purposes.
    Parameters:
    --------------
        df: DataFrame object to apply the transformation
    '''
    try:
        # here there should be some specific transformations
        pass
    except:
        # leave blank
        pass
    finally:
        # fixes column names
        df.columns = df.columns.str.strip().str.replace(
            ' ', '_').str.upper().str.replace('(', '')

        # removes blank spaces
        objs = df.select_dtypes(['object'])
        df[objs.columns] = objs.apply(lambda x: x.str.strip())

    return df


def load(df, connection, tablename, schema=None, check=None, dtype=None):
    ''' 
    Uploads DataFrame into SQL Server Database.
    It is recommended to use this functionality when DataFrames are not too big, due to the sqlalchemy connection being ODBC which is much slower than OLEDB.
    If you are uploading objects bigger than 1 million rows you should consider creating a SSIS package.
    Parameters:
    --------------
        df: DataFrame object
        connection: Connection object for database
        tablename: Name of the table. By default it uses the scripts name.
        check: Uses find duplicate functionality. Returns and uploads only non duplicate values.
    Functionalities:
    ---------------
        This function creates a coldict variable that automatically looks within the DataFrame and optimizes each column type for database.
    '''
    if dtype:
        coldict = sqlcol(df, frac_size=1)
    elif dtype is not None:
        coldict = dtype
    else:
        coldict = None

    try:
        start = timer()

        if check is None:
            df.to_sql(tablename.upper(), connection.engine, schema=schema,
                      index=False, if_exists='append', dtype=coldict)
        else:
            try:
                df = clean_df_db_dups(
                    df, tablename.upper(), connection.engine, check)
            except:
                pass
            finally:
                df.to_sql(tablename.upper(), connection.engine, schema=schema,
                          index=False, if_exists='append', dtype=coldict)

        end = timer()
        elapsed_time = end - start
        print(f'{tablename} updated in {elapsed_time} seconds')
        print(f'{len(df.index)} rows inserted\n')
    except KeyboardInterrupt:
        print('process interrupted by user\n')


def tablename(filename=None):
    '''
    Names the database table in relation to the file name.
    If filename is left blank then the function will return the scripts name as the filename
    Parameters:
    --------------
        filename: Name of the database
    '''
    if filename is not None:
        filename = filename
    else:
        filename = os.path.basename(__file__)

    return filename


def import_scripts(path):
    '''
    Imports scripting file (default to .sql for now) and creates dictionary to be used as an extraction form
    '''
    sqls = {}
    for name in os.listdir(path):
        if name.endswith('sql'):
            with open(os.path.join(path, name), 'r') as f:
                script = f.read()
            sqls.update({name: script})
    return sqls


def validate_folder_files(folder_path, folder_date, validation_files):
    '''
    The purpose of this function is to look within an specified dated folder that all files are uploaded. If a file is missing from the folder that is specified in the validations list then the ETL process shouldn't proceed, if that is what you want.
    Returns a boolean statement
    Parameters:
    --------------
        folder_path: root folder path in order to start looking
        folder_date: specific date folder to look for the files
        validation_files: single file or list of files to look for.
    '''
    # if validation_files is a string it will add onto list
    if type(validation_files) == str:
        validation_files = [validation_files]

    for root, dirs, files in os.walk(folder_path):
        if os.path.basename(root) == folder_date:
            matching = [s for s in files if any(
                validation_file in s for validation_file in validation_files
            )]
            if len(matching) != len(validation_files):
                non_matches = [validation_file for validation_file in validation_files if not any(
                    validation_file in match for match in matching
                )]
                print(f'{non_matches} are missing')
                return False
            else:
                print('all files are ready')
                return True


def create_mirror_path(origin, destination, copy_date=None, delete_first=False):
    '''
    This function has been adapted to a filesystem that uses year and month period folders to segment the data.
        Structure: Filename/Project - Year - Month
        Example: HSMProject - 1990 - 1990-01
    Note:
        Uses path_creator function as complement.
        copy_date must be in date format or string format. If an int is used then it will give error.
    Parameters:
    ---------------
        origin: folder that you wish to copy. If a date parameter is not added then it will not adjust automatically.
        destination: folder where you wish to copy the files. If a date parameter is not added then it will not adjust automatically.
        copy_date: date format of folders. It is recommended to use yyyy-mm-dd.
        delete_first: default False. Change to true if you wish to delete the folder structure previously.
    '''

    # configures origin path
    if origin is not None:
        origin_folder = os.path.join(origin, copy_date[:4], copy_date)
        destination_folder = os.path.join(
            destination, copy_date[:4], copy_date)
    else:
        origin_folder = origin
        destionation_folder = destination
    # creates route in folder in case it doesn't exist
    create_path(origin_folder, destionation_folder)
    # copy


def frag_load(df, connection, dtype, tablename, div=15, check=None, create_table=False, tries=10):
    df_fragments = math.ceil(df.shape[0] / div)
    i = 0
    print(f'Data will be loaded in {df_fragments} fragments')
    if create_table:
        with connection as conn:
            conn.create(dtype, tablename, None)

    for attempt in range(tries):
        try:
            with connection as conn:
                print('Starting process')
                for a in range(attempt, div):
                    fragdf = df[i: i + df_fragments]
                    df.load(fragdf, conn.engine, tablename,
                            dtype=dtype, schema='dbo', check=check)
                    i += df_fragments + 1
                    print('{0:.0%}'.format(i / df.shape[0]), 'progress')
        except OperationalError as e:
            if attempt < tries - 1:
                continue
            else:
                raise
        break
