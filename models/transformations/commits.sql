select value:author:login::string author_username,
       value:commit:author:name::string author_name,
       value:commit:message::string message,
       value:commit:url::string url,       
       value:commit:author:date::timestamp commit_author_date, 
       value json_data
from {{ref('stg_paginated_commits')}}, 
    lateral flatten(input=> github_commits, mode=>'ARRAY')
