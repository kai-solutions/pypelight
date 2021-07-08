--- USE THIS STATEMENT IN CONJUNCTION WITH PYTHON TECHNIQUES FOR CREATING INDEXES ON THE FLY
declare @stmt as varchar(MAX)
declare @index_name as varchar(100) = '{}'
declare @index_column as varchar(100)
declare @nonclustered_columns as table (id_column varchar(100))

insert into @nonclustered_columns values {}

select @index_column = COALESCE(@index_column + ', ', '') + CAST(id_column as varchar(50))
from @nonclustered_columns

set @stmt = 'drop index if exists [idx_' + @index_name +'] on ' + @index_name + 
'create nonclustered index [idx_' + @index_name +'] on ' + @index_name + 
'(' + @index_column + ')'

print(@stmt)
exec(@stmt)