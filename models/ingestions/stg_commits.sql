select STARSNOW_REQUEST('https://api.github.com/repos/' ||
    '{{var('gh_repository')}}/commits', NULL):data as github_commits