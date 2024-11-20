import argparse
import logging
from datetime import datetime

import atlassian.jira as api
import coloredlogs
import pytz
import json

LOGGER = logging.getLogger(__name__)


def format_datetime_to_user_tz(date: datetime, timezone) -> str:
    local_datetime = date.astimezone(timezone)
    return local_datetime.strftime("%Y-%m-%d %H:%M")


def get_query(seed_query: str | None = None, cutoff_date: str | None = None) -> str:
    query = seed_query or ''

    if cutoff_date:
        query += f' Updated >= "{cutoff_date}" ORDER BY Updated ASC'
    else:
        query += " ORDER BY Updated ASC"

    return query


def iterate_jql_results(
        jira_api: api.Jira,
        query: str,
):
    start = 0
    limit = 100

    while True:
        LOGGER.info(f"fetching issues from {start} to {start + limit}")

        response = jira_api.jql(
            query,
            start=start,
            limit=limit,
            # expand="changelog,comments",
        )

        for issue in response["issues"]:
            yield issue

        if len(response["issues"]) < limit:
            LOGGER.info(f"reached the last page, stopping")
            break

        start += len(response["issues"])


def jql(
        jira_api: api.Jira,
        query: str,
):
    query = get_query(query, cutoff_date=None)

    jira_url = jira_api.url.rstrip('/')

    for issue in iterate_jql_results(jira_api, query):
        data = {
            'key': issue['key'],
            'url': f"{jira_url}/browse/{issue['key']}",
            'created': issue['fields']['created'],
            'summary': issue['fields']['summary'],
        }
        print(json.dumps(data))


def remap_user(
        jira_api: api.Jira,
        source_user_id,
        target_user_id,
        seed_query: str | None,
        apply_changes=False,
):
    LOGGER.info(f"remapping user {source_user_id} to {target_user_id} (dry-run: {not apply_changes})")

    myself = jira_api.myself()

    LOGGER.info(f"logged in as account ID {myself['accountId']}, email is {myself['emailAddress']}")

    if source_user_id != myself['accountId']:
        LOGGER.warning(f"source user ID {source_user_id} is not the same as the logged in user ID {myself['accountId']}")

    target_user = jira_api.user(account_id=target_user_id)

    LOGGER.info(f"target user email is {target_user['emailAddress']}")

    # my_timezone_name = myself["timeZone"]
    #
    # LOGGER.info(f"timezone set in user profile: {my_timezone_name}")
    #
    # my_timezone = pytz.timezone(my_timezone_name)

    query = get_query(seed_query)

    issues_updated = 0

    def update_issue(issue, updates):
        nonlocal issues_updated

        if not apply_changes:
            LOGGER.info(f"[DRY RUN] would update issue {issue['key']} with {updates}")
            return

        LOGGER.info(f"updating issue {issue['key']} with {updates}")

        try:
            jira_api.update_issue(issue["key"], updates)
            issues_updated += 1
        except Exception as e:
            LOGGER.error(f"failed to update issue {issue['key']}: {e}")

    def process_issue(issue):
        LOGGER.info(f"processing issue {issue['key']}")
        fields = issue["fields"]
        creator = fields["creator"]
        assignee = fields.get("assignee")
        reporter = fields.get("reporter")

        updates = {}

        if creator['accountId'] == source_user_id:
            updates['creator'] = {
                "accountId": target_user_id
            }

        if assignee and assignee['accountId'] == source_user_id:
            updates["assignee"] = {
                "accountId": target_user_id
            }

        if reporter and reporter['accountId'] == source_user_id:
            updates["reporter"] = {
                "accountId": target_user_id
            }

        if updates:
            update_issue(issue, {
                'fields': updates,
            })

    for issue in iterate_jql_results(jira_api, query):
        process_issue(issue)

    LOGGER.info(f"updated {issues_updated} issues")


def copy_watchers(
        jira_api: api.Jira,
        source_user_id: str,
        target_user_id: str,
        apply_changes: bool,
):
    source_user = jira_api.user(account_id=source_user_id)
    target_user = jira_api.user(account_id=target_user_id)

    LOGGER.info(f"copying watchers from {source_user['emailAddress']} to {target_user['emailAddress']} (dry-run: {not apply_changes})")

    source_user_watching_query = 'watcher = "%s"' % source_user_id
    target_user_watching_query = 'watcher = "%s"' % target_user_id

    issues_updated = 0

    def process_issue(issue):
        nonlocal issues_updated

        LOGGER.debug(f"processing issue {issue['key']}")

        if not apply_changes:
            LOGGER.info(f"[DRY RUN] would add watcher {target_user_id} to issue {issue['key']}")
            return

        LOGGER.info(f"adding watcher {target_user_id} to issue {issue['key']}")
        try:
            jira_api.issue_add_watcher(issue['key'], target_user_id)
            issues_updated += 1
        except Exception as e:
            LOGGER.error(f"failed to add watcher to issue {issue['key']}: {e}")

    LOGGER.info(f"fetching issues watched by {target_user['emailAddress']}")

    target_user_watching_keys = set()
    for issue in iterate_jql_results(jira_api, target_user_watching_query):
        target_user_watching_keys.add(issue['key'])

    LOGGER.info(f"target user is already watching {len(target_user_watching_keys)} issues")

    for issue in iterate_jql_results(jira_api, source_user_watching_query):
        if issue['key'] in target_user_watching_keys:
            LOGGER.debug(f"issue {issue['key']} is already watched by {target_user['emailAddress']}")
            continue

        process_issue(issue)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--log-level', type=str, default='INFO', help='Log level')
    parser.add_argument('--jira-url', type=str, help='Jira URL')
    parser.add_argument('--jira-user', type=str, help='Jira user')
    parser.add_argument('--jira-token', type=str, help='Jira token')

    subparsers = parser.add_subparsers(dest='command', required=True)

    remap_parser = subparsers.add_parser('remap-user')
    remap_parser.add_argument('--apply-changes', action='store_true')
    remap_parser.add_argument('--source-user-id', type=str, required=True)
    remap_parser.add_argument('--target-user-id', type=str, required=True)
    remap_parser.add_argument('--seed-query', type=str, required=False)

    jql_parser = subparsers.add_parser('jql')
    jql_parser.add_argument('--query', type=str, required=True)

    copy_watchers_parser = subparsers.add_parser('copy-watchers')
    copy_watchers_parser.add_argument('--apply-changes', action='store_true')
    copy_watchers_parser.add_argument('--source-user-id', type=str, required=True)
    copy_watchers_parser.add_argument('--target-user-id', type=str, required=True)

    args = parser.parse_args()

    coloredlogs.install(level=args.log_level.upper(), logger=LOGGER)

    jira = api.Jira(
        url=args.jira_url,
        username=args.jira_user,
        password=args.jira_token,
    )

    if args.command == 'remap-user':
        remap_user(
            jira,
            args.source_user_id,
            args.target_user_id,
            args.seed_query,
            args.apply_changes,
        )
    elif args.command == 'jql':
        jql(
            jira,
            args.query,
        )
    elif args.command == 'copy-watchers':
        copy_watchers(
            jira,
            args.source_user_id,
            args.target_user_id,
            args.apply_changes,
        )
    else:
        parser.print_help()
        return


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        LOGGER.error(f"unhandled exception: {e}", exc_info=True)
        exit(1)
