--- USE THIS STATEMENT IN CONJUNCTION WITH PYTHON TECHNIQUES FOR CREATING INDEXES ON THE FLY
declare @stmt as varchar(MAX)
declare @index_name as varchar(100) = '{}'
declare @clustered_column as varchar(50) = '{}'

set @stmt = 'drop index if exists [idx_' + @index_name + '_' + @clustered_column +'] on ' + @index_name + 
'create clustered index [idx_' + @index_name + '_' + @clustered_column +'] on ' + @index_name + 
'(' + @clustered_column + 'asc)'

print(@stmt)
exec(@stmt)