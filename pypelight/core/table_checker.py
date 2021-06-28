def table_similarity_tester(df0, df1, columns = [], rtrn = None):
    '''
    Verifies if two tables are the same by analyzing its columns.

    Parameters:
    -----------
        df0 = main table
        df1 = table to compare
        columsn = columns to match
    '''
    df = df0.merge(df1, on = columns, how = 'outer', indicator = True)
    print(df['_merge'].value_counts())
    if rtrn is None:
        return df
    if rtrn is not None and rtrn != 'diff':
        return df.loc[df['_merge'] == rtrn]
    else:
        return df.loc[df['_merge'] != 'both']