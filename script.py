import os
import time
import requests
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from tabulate import tabulate

# ---- CONFIG ----
SLACK_TOKEN = os.environ["SLACK_TOKEN"]
SOURCE_CHANNEL_ID = os.environ.get("SOURCE_CHANNEL_ID", "C05TWLUUT7S")
BIRTHDAY_FIELD_ID = os.environ.get("BIRTHDAY_FIELD_ID", "Xf05STPV0Z3R")
PROFILE_DELAY = float(os.environ.get("PROFILE_FETCH_DELAY_SECONDS", "0.2"))

# Notion
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
NOTION_VERSION = os.environ.get("NOTION_VERSION", "2025-09-03")

# Notion property names in your database / data source
NOTION_NAME_PROPERTY = os.environ.get("NOTION_NAME_PROPERTY", "Name")
NOTION_SLACK_ID_PROPERTY = os.environ.get("NOTION_SLACK_ID_PROPERTY", "Slack User ID")
NOTION_BIRTHDAY_PROPERTY = os.environ.get("NOTION_BIRTHDAY_PROPERTY", "Birthday")

client = WebClient(token=SLACK_TOKEN)


def call(fn, **kwargs):
    """Retry on Slack rate limits."""
    while True:
        try:
            return fn(**kwargs)
        except SlackApiError as e:
            if getattr(e.response, "status_code", None) == 429:
                time.sleep(int(e.response.headers.get("Retry-After", "1")))
                continue
            raise


def all_members(channel_id):
    """Get all members from a channel."""
    members = []
    cursor = None

    while True:
        r = call(client.conversations_members, channel=channel_id, limit=200, cursor=cursor)
        members.extend(r.get("members", []))

        cursor = (r.get("response_metadata") or {}).get("next_cursor")
        if not cursor:
            break

    return members


def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_VERSION,
    }


def notion_query_by_slack_id(slack_user_id):
    """
    Find an existing Notion page by Slack User ID.
    Returns page object or None.
    """
    url = f"https://api.notion.com/v1/data_sources/{NOTION_DATA_SOURCE_ID}/query"
    payload = {
        "filter": {
            "property": NOTION_SLACK_ID_PROPERTY,
            "rich_text": {
                "equals": slack_user_id
            }
        },
        "page_size": 1
    }

    resp = requests.post(url, headers=notion_headers(), json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    results = data.get("results", [])
    return results[0] if results else None


def notion_create_page(name, slack_user_id, birthday):
    url = "https://api.notion.com/v1/pages"
    payload = {
        "parent": {
            "type": "data_source_id",
            "data_source_id": NOTION_DATA_SOURCE_ID
        },
        "properties": {
            NOTION_NAME_PROPERTY: {
                "title": [
                    {
                        "text": {
                            "content": name or slack_user_id
                        }
                    }
                ]
            },
            NOTION_SLACK_ID_PROPERTY: {
                "rich_text": [
                    {
                        "text": {
                            "content": slack_user_id
                        }
                    }
                ]
            },
            NOTION_BIRTHDAY_PROPERTY: {
                "rich_text": [
                    {
                        "text": {
                            "content": birthday or ""
                        }
                    }
                ]
            }
        }
    }

    resp = requests.post(url, headers=notion_headers(), json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def notion_update_page(page_id, name, birthday):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {
        "properties": {
            NOTION_NAME_PROPERTY: {
                "title": [
                    {
                        "text": {
                            "content": name or ""
                        }
                    }
                ]
            },
            NOTION_BIRTHDAY_PROPERTY: {
                "rich_text": [
                    {
                        "text": {
                            "content": birthday or ""
                        }
                    }
                ]
            }
        }
    }

    resp = requests.patch(url, headers=notion_headers(), json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def sync_user_to_notion(name, slack_user_id, birthday):
    """
    Upsert user into Notion:
    - search by Slack User ID
    - update if exists
    - create if not
    """
    existing = notion_query_by_slack_id(slack_user_id)

    if existing:
        notion_update_page(existing["id"], name, birthday)
        return "updated"
    else:
        notion_create_page(name, slack_user_id, birthday)
        return "created"


def main():
    rows = []
    created = 0
    updated = 0
    failed_notion = 0

    for uid in all_members(SOURCE_CHANNEL_ID):
        try:
            resp = call(client.users_profile_get, user=uid)
            prof = resp["profile"]

            name = prof.get("real_name") or prof.get("display_name") or ""
            fields = prof.get("fields") or {}
            birthday = (fields.get(BIRTHDAY_FIELD_ID) or {}).get("value", "")

            rows.append([name, uid, birthday])

            try:
                result = sync_user_to_notion(name, uid, birthday)
                if result == "created":
                    created += 1
                elif result == "updated":
                    updated += 1
            except requests.HTTPError as e:
                failed_notion += 1
                body = ""
                try:
                    body = e.response.text
                except Exception:
                    pass
                print(f"Notion sync failed for {uid}: {e} {body}")

            time.sleep(PROFILE_DELAY)

        except SlackApiError as e:
            print(f"Error fetching {uid}: {e.response['error']}")

    print("\nSlack Channel Members\n")
    print(tabulate(rows, headers=["Name", "Slack ID", "Birthday Field Value"], tablefmt="github"))
    print(f"\nTotal users processed: {len(rows)}")
    print(f"Notion created: {created}")
    print(f"Notion updated: {updated}")
    print(f"Notion failed: {failed_notion}")


if __name__ == "__main__":
    main()
