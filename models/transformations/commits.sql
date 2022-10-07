select value:author:login::string author_username,
       value:commit:author:name::string author_name,
       value:commit:message::string message,
       value:commit:url::string url,        
       value json_data
from (
    select value 
     from table(flatten(input=> (select * from {{ref('stg_commits')}}) , mode=>'ARRAY'))
     )
