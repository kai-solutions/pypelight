import pandas as pd
import numpy as np
import math
import os
from sqlalchemy import create_engine
from sqlalchemy import types
from core.flow_tools import import_scripts


class Connection(object):
    ''' Connection basic object with Context Manager capability
        Extends functionality for selecting and inserting using pandas and sqlite3
    '''

    def __init__(self, db_host, db_port, db_name, db_user, db_pass):
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_pass = db_pass

    def __enter__(self):
        self.engine = create_engine(self.database_uri, fast_executemany=True)
        self.connector = self.engine.connect()
        self.transaction = self.connector.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is None:
            self.transaction.commit()
        else:
            self.transaction.rollback()
        # Use in conjunction with 'with' statement to always close connection automatically.
        self.connector.close()

    def select(self, sql_string, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None):
        ''' Uses pandas read_sql method
            Extends to using an embedded connection object
        '''
        dfparam = pd.read_sql(sql_string, self.connector)
        return dfparam

    def insert(self, dfparam, tablename, index=False, if_exists='append', chunksize=100):
        ''' Exports pandas dataframe to a database
            Extensions:
                embedded connection object
                index as False
                if table exists it appends rows into table in 100 chunksize
        '''
        print(
            f'dataframe to insert contains {len(dfparam.index)} rows and {len(dfparam.columns)} columns'
        )
        dfparam.to_sql(name=tablename, conn=self.connector,
                       index=index, if_exists=if_exists, chunksize=chunksize)

    def __repr__(self):
        pass


class SQLServer(Connection):
    ''' Connection object for Microsoft SQL Server.
        Tested thoroughly on 2016.
        Extends functionality for create and drops '''

    def __init__(self, db_host, db_name, db_port=1433, trusted_connection='yes', db_user=None, db_pass=None):
        # Uses SQLAlchemy for connection management.
        self.db_type = 'mssql+pyodbc'
        self.db_driver = 'ODBC+Driver+17+for+SQL+Server'
        self.trusted_connection = trusted_connection
        self._database_uri = f'{self.db_type}://{db_host},{db_port}/{db_name}?trusted_connection={self.trusted_connection}&driver={self.db_driver}'
        self.connector = None

    def execute(self, sql):
        self.connector.execute(sql)

    def create(self, dtypedict, tablename, partition_column=None):
        ''' By using the dtypedict parameter you may create a string variable with a create sentence.
            It will execute the create query to generate the data table. You must name the table by using the tablename parameter. '''

        # Generates query sentence by using the tablename and dftypedict params.
        query = f'create table {tablename} ('
        for column, dtype in dtypedict.items():
            query = f'{query} \n {column} {dtype} ,'

        # If a partition is needed then the table may be created using the partitioning.
        if partition_column is None:
            query = query[:-1] + ');'
        else:
            # Needs to be worked upon. Right now it is a broken functionality.
            pass
            query = (
                f'{query[:-1]}  ) ON {partition} ({partition_column}) WITH ( DATA_COMPRESSION = PAGE);'
            )

        try:
            # Executes drop before creating table.
            self.drop(tablename)
            self.connector.execute(query)
        except:
            print(f'Error when creating {tablename}')
        else:
            print(f'{tablename} created successfully')

        def drop(self, tablename):
            '''Drops table function. In case you are using an older version of SQL Server it tries using both sentences. '''

            drop_sql = '''drop table if exists {}'''.format(tablename)
            drop_sql_old = ''' if exists (select * from sys.tables T
                                            join sys.schemas S
                                                on T.schema_id = S.schema_id
                                                where S.name = 'dbo' and T.name = {})
                                        drop table {}'''.format(tablename, tablename)
            try:
                self.connector.execute(drop_sql)
            except:
                self.connector.execute(drop_sql_old)
            finally:
                try:
                    # tries to find if the table has registers. if it fails then table was dropped successfully.
                    self.connector.select(
                        '''select top 1.* from {}''').format(tablename)
                except:
                    print(f'{tablename} dropped successfully')
                else:
                    print(f'drop table {tablename} failed')

        def __repr__(self):
            return self.database_uri

        def create_index(tablename, params, type='clustered'):
            sqls_dict = import_scripts(os.path.join(os.path.abspath(
                os.path.join(os.getcwd(), '..'))), 'sql_tools')
            self.tablename = f'{tablename}'
            with self.connector as conn:
                if type(params) == list:
                    params = [f'(\'{item}\')' for item in params]
                    print('creating nonclustered index on', tablename)
                    conn.execute(sqls['create_nonclustered_index.sql'].format(
                        tablename, ','.join(params)))


def sqlcol(dfparam, fraction_size=1, varchar_multiplier=1):
    ''' Determines the sentence to create a table with using T-SQL syntax
        Verifies the name of the columns, column type and max longitud or precision.
        Parameters:
        ---------------
            dfparam = dataframe
            fraction_size = random sample size to analyze column type and longitud / precision
            varchar_multiplier = multiplier to augment proportionally the max longitud on each varchar column.
        Required modules:
        ---------------
            sqlalchemy.types
            numpy
            math
    '''

    dtypedict = {}

    for i in range(0, dfparam.shape[1]-1):
        col = dfparam.iloc[:, i].dropna()

        if col.empty:
            dtypedict.update({
                col.name: types.VARCHAR(length=100)
            })
        elif str(col.dtype) == 'object':
            dtypedict.update({
                col.name: types.VARCHAR(length=math.ceil(
                    col.astype(str).map(len).max() * varchar_multiplier
                ))
            })
        elif 'datetime' in str(col.dtype):
            dtypedict.update({
                col.name: types.DateTime()
            })
        elif 'float' in str(col.dtype):
            dtypedict.update({
                col.name: types.Float(precision=np.modf(
                    col[0] * 100).astype(str).map(len).max(), asdecimal=True)
            })
        elif 'int' in str(col.dtype):
            dtypedict.update({
                col.name: types.INT()
            })

    return dtypedict
