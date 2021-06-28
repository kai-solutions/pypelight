import pandas as pd

def clean_df_db_dups(df, tablename, engine, dup_cols = [], filter_continuous_col = None, filter_categorical_col = None):

    '''
    Removes rows from a dataframe since they are already in the database
    Parameters:
    -----------
        df =        dataframe to check and remove duplicates
        engine =    SQLalchemy database engine
        tablename = name of the table in the database to check for duplicates
        dup_cols =  list or tuple of column names to check for duplicates
    Optional:
        filter_continuous_col = the name of the column with continous data for using an BETWEEN min/max.
                                it can be either a float, int, or datetime
                                useful for restricting the size of the database when doing the check
        filter_categorical_col =name of the categorical column to use with a WHERE clause
                                creates a check IN () of the unique values within the column

    Returns:
        a list of values inside the dataframe that are not present in the database
    '''

    args = 'SELECT %s FROM %s' %(', '.join(['"{0}"'.format(col) for col in dup_cols]), tablename)
    args_contin_filter, args_cat_filter = None, None

    if filter_continuous_col is not None:
        if df[filter_continuous_col].dtype == 'datetime64[ns]':
            args_contin_filter = """ '%s' BETWEEN Convert(datetime, '%s') AND Convert(datetime, '%s') """ %(filter_continuous_col,
            df[filter_continuous_col].min(), df[filter_continuous_col].max())
        elif df[filter_continuous_col].dtype == 'int64':
            args_contin_filter = """ '%s' BETWEEN '%s' AND '%s' """ %(filter_continuous_col,
            df[filter_continuous_col].min(), df[filter_continuous_col].max())

    if filter_categorical_col is not None:
        args_cat_filter = " '%s' in(%s)" %(filter_categorical_col,
        ', '.join([" '{0}'".format(value) for value in df[filter_categorical_col].unique()]))

    if args_contin_filter and args_cat_filter:
        args += ' Where ' + args_contin_filter + ' AND ' + args_cat_filter
    elif args_contin_filter:
        args += ' Where ' + args_contin_filter
    elif args_cat_filter:
        args += ' Where ' + args_cat_filter

    df0 = df.drop_duplicates(dup_cols, keep= 'last')
    df0 = pd.merge(df0, pd.read_sql(args, engine), how = 'left', on = dup_cols, indicator = True)
    df0 = df0.loc[df0['_merge'] == 'left_only']
    df0.drop(['_merge'], axis = 1, inplace = True)

    return df0