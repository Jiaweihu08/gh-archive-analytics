from dataclasses import dataclass


@dataclass(frozen=True)
class Query:
    title: str
    query_string: str


# ----------- Basic info -----------
event_type_counts = Query(
    title='Event type counts',
    query_string="""
SELECT
    type AS event_type,
    COUNT(*) AS number_of_events
FROM
    events
GROUP BY
    type
ORDER BY
    number_of_events DESC
LIMIT 15
""")

action_counts = Query(
    title='Event action counts',
    query_string="""
SELECT
    payload.action as action,
    count(*) AS action_counts
FROM
    events
WHERE
    payload.action IS NOT NULL
GROUP BY
    payload.action
ORDER BY
    action_counts DESC
LIMIT 15
""")

# ----------- Language Popularity -----------
language_popularity_by_PRs = Query(
    title='Language popularity - PR counts',
    query_string="""
SELECT
    payload.pull_request.base.repo.language AS language,
    COUNT(*) AS number_of_PR_events
FROM events
WHERE
    type == 'PullRequestEvent' AND
    payload.pull_request.base.repo.language IS NOT NULL
GROUP BY
    payload.pull_request.base.repo.language
ORDER BY
    number_of_PR_events DESC
LIMIT 15
""")


# ----------- Repository Popularity -----------
repo_popularity_by_num_events = Query(
    title='Repo popularity - num events',
    query_string="""
SELECT
    repo.name AS repository,
    count(*) as total_number_of_events
FROM
    events
GROUP BY
    repo.name
ORDER BY
    total_number_of_events DESC
LIMIT 15
""")

repo_popularity_by_num_PRs = Query(
    title='Repo popularity - num PRs',
    query_string="""
SELECT
    repo.name AS repository,
    count(*) as number_of_PRs
FROM
    events
WHERE
    type == 'PullRequestEvent'
GROUP BY
    repo.name
ORDER BY
    number_of_PRs DESC
LIMIT 15
""")

repo_popularity_by_num_new_forks = Query(
    title='Repo popularity - fork counts',
    query_string="""
SELECT
    repo.name AS repository,
    count(*) as number_of_forks
FROM
    events
WHERE
    type == 'ForkEvent'
GROUP BY
    repo.name
ORDER BY
    number_of_forks DESC
""")

repo_popularity_by_num_total_forks = Query(
    title='Repo popularity - total fork counts',
    query_string="""
SELECT
    repo.name AS repository,
    MAX(payload.pull_request.base.repo.forks_count) AS total_forks
FROM
    events
WHERE
    type IN (
        'PullRequestEvent',
        'PullRequestReviewEvent',
        'PullRequestReviewCommentEvent'
        )
GROUP BY
    repo.name
ORDER BY
    total_forks DESC
LIMIT 15
""")

repo_popularity_by_num_pushes = Query(
    title='Repo popularity - push counts',
    query_string="""
SELECT
    repo.name AS repository,
    COUNT(*) AS number_of_pushes,
    SUM(payload.size) as number_of_commits
FROM
    events
WHERE
    type == 'PushEvent'
GROUP BY
    repo.name
HAVING
    number_of_pushes <= number_of_commits
ORDER BY
    number_of_pushes DESC
LIMIT 15
""")

repo_popularity_by_num_new_issues = Query(
    title='Repo popularity - num new issues',
    query_string="""
SELECT
    repo.name AS repository,
    COUNT(*) AS newly_opened_issues
FROM
    events
WHERE
    type == 'IssuesEvent' AND
    payload.action == 'opened'
GROUP BY
    repo.name
ORDER BY
    newly_opened_issues DESC
LIMIT 15
""")

repo_popularity_by_num_total_open_issues = Query(
    title='Repo popularity - num all open issues',
    query_string="""
SELECT
    repo.name AS repository,
    MAX(payload.pull_request.base.repo.open_issues_count) AS total_open_issues
FROM
    events
WHERE
    type IN (
        'PullRequestEvent',
        'PullRequestReviewEvent',
        'PullRequestReviewCommentEvent'
        )
GROUP BY
    repository
ORDER BY
    total_open_issues DESC
LIMIT 15
""")


all_queries = [
    language_popularity_by_PRs, event_type_counts, action_counts, repo_popularity_by_num_new_issues,
    repo_popularity_by_num_new_forks, repo_popularity_by_num_PRs, repo_popularity_by_num_pushes,
    repo_popularity_by_num_total_open_issues, repo_popularity_by_num_total_forks, repo_popularity_by_num_events
]
