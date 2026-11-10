import csv
import difflib
import os
import re
import sys
import time
from pathlib import Path

import requests
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from tabulate import tabulate

# ---- CONFIG ----
SLACK_TOKEN = os.environ["SLACK_TOKEN"]
NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DATABASE_ID = os.environ["NOTION_DATABASE_ID"]
NOTION_VERSION = "2022-06-28"
PROFILE_DELAY = 0.2

# Notion properties that are always used
NOTION_NAME_PROPERTY = "Name"  # type: Title
NOTION_SLACK_ID_PROPERTY = "Slack User ID"  # type: Text
NOTION_ACTIVE_PROPERTY = "Active"  # type: Checkbox

# Notion property name -> Slack custom profile field ID
# IDs come from https://slack.com/api/team.profile.get?visibility=all
# The Notion type of each property (Text, Phone, URL, Email, Date, Select) is read from the database.
SLACK_FIELDS = {
    "Birthday": "Xf05STPV0Z3R",
    "Department": "Xf05TAPCM8PN",
    "Batch": "Xf05T1LNGTB8",
    "Nationality": "Xf0AV382B7GT",
    "Phone": "Xf05STPV0UVD",
    "Status": "Xf05TXA040N4",
    "Title": "Xf05T1LNG7TQ",
    "University": "Xf0AV7HDFTL6",
    "Degree": "Xf0AVD77MTEY",
    "LinkedIn": "Xf0AV9K6BS9Y",
    "Instagram": "Xf0AV382PDV1",
    "Private Email": "Xf0AV7HDSA2J",
    "Interests": "Xf0AUU5MKSMD",
}

# Notion property whose value is turned into country names, and the list of countries
COUNTRY_PROPERTY = "Nationality"
COUNTRIES_FILE = Path(__file__).parent / "countries.csv"

client = WebClient(token=SLACK_TOKEN)


def country_lookup():
    """Map every lowercase country name, adjective and alias to the country name."""
    lookup = {}
    with open(COUNTRIES_FILE, encoding="utf-8") as f:
        for row in csv.DictReader(f, delimiter=";"):
            for alias in [row["country"], *row["other_names"].split("|")]:
                if alias:
                    lookup[alias.strip().lower()] = row["country"]
    return lookup


COUNTRIES = country_lookup()


def to_country(part):
    """Turn one entry like 'german', 'The Netherlands' or 'Germnay' into a country name."""
    key = re.sub(r"^the\s+", "", part.strip().lower())
    if key in COUNTRIES:
        return COUNTRIES[key]
    match = difflib.get_close_matches(key, COUNTRIES.keys(), n=1, cutoff=0.85)
    if match:
        return COUNTRIES[match[0]]
    print(f"Unknown country, kept as is: {part.strip()}")
    return part.strip()


def normalize_countries(value):
    """Normalize a value with one or more countries, e.g. 'French, Dutch' -> 'France, Netherlands'."""
    parts = [p for p in re.split(r"[,/&;]", value) if p.strip()]
    countries = dict.fromkeys(to_country(p) for p in parts)
    return ", ".join(countries)


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


def all_slack_users():
    """Get every user of the workspace, including deactivated ones."""
    users = []
    cursor = None

    while True:
        r = call(client.users_list, limit=200, cursor=cursor)
        users.extend(r.get("members", []))

        cursor = (r.get("response_metadata") or {}).get("next_cursor")
        if not cursor:
            break

    return [u for u in users if not u.get("is_bot") and u["id"] != "USLACKBOT"]


def notion_headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_VERSION,
    }


def notion_pages_by_slack_id():
    """Load all pages of the database once and map Slack User ID -> page ID."""
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    pages = {}
    cursor = None

    while True:
        payload = {"page_size": 100}
        if cursor:
            payload["start_cursor"] = cursor

        resp = requests.post(url, headers=notion_headers(), json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        for page in data.get("results", []):
            texts = page["properties"].get(NOTION_SLACK_ID_PROPERTY, {}).get("rich_text", [])
            slack_id = "".join(t.get("plain_text", "") for t in texts).strip()
            if slack_id:
                pages[slack_id] = page["id"]

        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")

    return pages


def notion_schema():
    """Map each property name of the database to its Notion type."""
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}"
    resp = requests.get(url, headers=notion_headers(), timeout=30)
    resp.raise_for_status()
    return {name: prop["type"] for name, prop in resp.json()["properties"].items()}


def text(value):
    return {"rich_text": [{"text": {"content": value}}] if value else []}


def notion_value(prop_type, value):
    """Format a Slack field value for a Notion property of the given type."""
    if prop_type in ("phone_number", "url", "email"):
        return {prop_type: value or None}
    if prop_type == "date":
        return {"date": {"start": value} if value else None}
    if prop_type == "select":
        return {"select": {"name": value} if value else None}
    if prop_type == "rich_text":
        return text(value)
    raise ValueError(f"Notion property type '{prop_type}' is not supported")


def notion_properties(schema, name, slack_user_id, profile_values):
    properties = {
        NOTION_NAME_PROPERTY: {"title": [{"text": {"content": name or slack_user_id}}]},
        NOTION_SLACK_ID_PROPERTY: text(slack_user_id),
    }
    if NOTION_ACTIVE_PROPERTY in schema:
        properties[NOTION_ACTIVE_PROPERTY] = {"checkbox": True}
    for prop, value in profile_values.items():
        if prop in schema:
            properties[prop] = notion_value(schema[prop], value)
    return properties


def notion_create_page(properties):
    url = "https://api.notion.com/v1/pages"
    payload = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": properties}
    resp = requests.post(url, headers=notion_headers(), json=payload, timeout=30)
    resp.raise_for_status()


def notion_update_page(page_id, properties):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    resp = requests.patch(url, headers=notion_headers(), json={"properties": properties}, timeout=30)
    resp.raise_for_status()


def notion_set_inactive(page_id):
    notion_update_page(page_id, {NOTION_ACTIVE_PROPERTY: {"checkbox": False}})


def slack_profile_values(uid):
    """Read the custom profile fields of one user."""
    prof = call(client.users_profile_get, user=uid)["profile"]
    fields = prof.get("fields") or {}
    values = {prop: (fields.get(field_id) or {}).get("value", "") for prop, field_id in SLACK_FIELDS.items()}
    values[COUNTRY_PROPERTY] = normalize_countries(values[COUNTRY_PROPERTY])
    return values


def main():
    rows = []
    created = 0
    updated = 0
    deactivated = 0
    failed = 0

    schema = notion_schema()
    missing = [p for p in [NOTION_ACTIVE_PROPERTY, *SLACK_FIELDS] if p not in schema]
    if missing:
        print(f"Not in the Notion database, skipped: {', '.join(missing)}")

    existing_pages = notion_pages_by_slack_id()

    for user in all_slack_users():
        uid = user["id"]
        name = user.get("real_name") or user.get("profile", {}).get("display_name") or ""

        try:
            if user.get("deleted"):
                if uid in existing_pages and NOTION_ACTIVE_PROPERTY in schema:
                    notion_set_inactive(existing_pages[uid])
                    deactivated += 1
                continue

            values = slack_profile_values(uid)
            properties = notion_properties(schema, name, uid, values)

            if uid in existing_pages:
                notion_update_page(existing_pages[uid], properties)
                updated += 1
            else:
                notion_create_page(properties)
                created += 1

            rows.append([name, uid, values.get("Birthday", "")])
            time.sleep(PROFILE_DELAY)

        except SlackApiError as e:
            failed += 1
            print(f"Slack error for {uid}: {e.response['error']}")
        except requests.HTTPError as e:
            failed += 1
            print(f"Notion error for {uid}: {e} {e.response.text}")

    print("\nActive Slack members\n")
    print(tabulate(rows, headers=["Name", "Slack ID", "Birthday"], tablefmt="github"))
    print(f"\nTotal active users processed: {len(rows)}")
    print(f"Notion created: {created}")
    print(f"Notion updated: {updated}")
    print(f"Notion set inactive: {deactivated}")
    print(f"Failed: {failed}")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
