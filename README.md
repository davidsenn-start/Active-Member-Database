# Active Member Database

Keeps the START Lisbon member database in Notion up to date with Slack, once a day, automatically.

- **Slack → Notion:** personal profile fields (birthday, phone, university, …) that members fill in themselves.
- **Notion → Slack:** the profile title, built from the department, role and batch that the FinOps head sets in Notion.

Nothing has to be run by hand. The FinOps head only maintains the Notion database (see [Every semester](#every-semester-finops-head)).

---

## What happens on every run

The workflow runs every day at 06:00 UTC on GitHub Actions. GitHub often starts scheduled runs a few hours late; that is normal.

For every Slack account the script does the following, in this order:

| Case | What happens in Notion |
|---|---|
| Joined Slack **before Zeynep Colak** (start of the FS25 batch) | Skipped. An existing page is moved to the Notion trash. |
| Account **deactivated** in Slack | Existing page gets `Active` unticked. No new page is created. |
| Slack name has **no first and last name** (e.g. "Luisa", "admin") | Skipped. An existing page is moved to the Notion trash. It comes back automatically once the person adds their last name in Slack. |
| Bots and Slackbot | Ignored. |
| Everyone else | Page is created or updated with the Slack fields below. If the page has a **Profile Title**, it is written to the person's Slack profile title. |

Pages in the Notion trash can be restored for 30 days.

### Fields copied from Slack to Notion

These come from the custom fields in each member's Slack profile. They are overwritten on every run, so change them in Slack, not in Notion.

| Notion property | Slack profile field |
|---|---|
| Name | Full name |
| Slack User ID | (account ID, used to match Slack and Notion) |
| Birthday | Birthday |
| Nationality | Nationality, converted to the country name (see below) |
| Phone | Phone |
| University | University |
| Degree | Degree |
| LinkedIn | LinkedIn |
| Instagram | Instagram |
| Private Email | Private Email Address |
| Interests | Interests |

The script reads the type of each Notion property (Text, Phone, URL, Email, Date, Select) and writes the value in that format. A property that does not exist in Notion is skipped and listed in the log.

**Nationality** is always stored as the English country name: "German" → "Germany", "Dutch" → "Netherlands", "USA" → "United States", "italy" → "Italy", typos like "Germnay" → "Germany", and "French, Dutch" → "France, Netherlands". The list of countries and their alternative names is in [`countries.csv`](countries.csv). A value that cannot be matched is kept as it is and logged as `Unknown country`; add its spelling to `countries.csv` to fix it.

### Fields set by the FinOps head in Notion

The script never writes these. Only the FinOps head (or another admin) changes them in Notion.

| Notion property | Type | Values |
|---|---|---|
| Department | Select | Marketing, Events, Finance and Operations, People & Community, Partnerships |
| Role | Select | Member, Head, President, Vice-President |
| Batch | Select | `SS` (spring semester) or `FS` (fall semester) + year, e.g. `SS26`, `FS26` |
| Status | Select | Active, Passive |
| Profile Title | Formula | computed, see below |
| Active | Checkbox | ticked by the script; unticked when the Slack account is deactivated |

**Batch** is always the semester of the person's **last** position. Passive members keep the batch of their last active semester.

**Profile Title** is a Notion formula:

```
ifs(
  prop("Role") == "President", "President " + prop("Batch"),
  prop("Role") == "Vice-President", "Vice-President " + prop("Batch"),
  prop("Role") == "Head", "Head of " + prop("Department") + " " + prop("Batch"),
  empty(prop("Department")), "",
  "Member of " + prop("Department") + " " + prop("Batch")
)
```

Examples: `Member of Marketing SS26`, `Head of Events FS26`, `President SS26`.

The script copies the Profile Title into each member's **Slack profile title** whenever the two differ. If a member changes their title in Slack, it is reset the next day. Members with an empty Profile Title (no department set) keep their Slack title.

---

## Every semester (FinOps head)

At the start of each semester, in the Notion database:

1. **Filter** the view on `Status = Active`.
2. **Members who continue:** select all of them (click the first row, Shift-click the last) and set `Batch` to the new semester, e.g. `FS26`. Change `Department` or `Role` only for people who switch.
3. **Members who leave:** set `Status` to `Passive`. **Do not change their Batch.** It keeps showing their last position.
4. **New Heads, President, Vice-President:** set `Role` accordingly. Everyone else is `Member`.
5. **New members:** they appear in Notion automatically after the next daily run once they have joined Slack with first and last name. Set `Department`, `Role`, `Batch` and `Status` for each of them.
6. **New batch option:** the first time a new semester is used, add it as an option to `Batch` (e.g. `SS27`).

The Slack titles follow automatically the day after the changes.

Tip: sort the view by `Slack User ID`, descending, to see the newest members on top.

---

## Things to check from time to time

| What | When | What to do |
|---|---|---|
| **Admin token for Slack titles** | When the Slack admin who created `SLACK_ADMIN_TOKEN` leaves START Lisbon or loses admin rights | A current Workspace Admin or Owner reinstalls the Slack app (see [Setup](#setup)) and replaces the `SLACK_ADMIN_TOKEN` secret. Until then, titles are not written to Slack; the log says so. |
| **Workflow switched off by GitHub** | GitHub turns off scheduled workflows in **public** repositories after 60 days without a commit | Open **Actions → Sync Slack members to Notion → Enable workflow**. Making the repository **private** avoids this. |
| **Daily run failed** | GitHub emails the account that last changed the schedule in the workflow file (currently `davidsenn-start`) | Open **Actions**, click the red run, open the step **Run sync** and read the last lines (see [Reading the log](#reading-the-log)). |
| **New profile field in Slack** | When an admin adds a new custom field in Slack and wants it in Notion | Ask whoever maintains this repository to add it to `SLACK_FIELDS` in `script.py`, and create a Notion property with the same name. |
| **Department renamed or added** | When the structure changes | Rename or add the option in the `Department` property in Notion. Renaming an option changes it on every page and in every Profile Title at once. |

---

## Reading the log

**Actions → latest run → Run sync**, at the very bottom:

```
Not in the Notion database, skipped: ...        <- Notion properties that are missing
SLACK_ADMIN_TOKEN not set, ...                  <- titles are not written to Slack
Total active users processed: 88
Notion created: 2
Notion updated: 86
Notion set inactive: 0
Notion removed (no first and last name in Slack): 0
Notion removed (joined Slack before Zeynep Colak): 0
Slack titles updated from Notion: 5
Failed: 0
```

`Failed` above 0 makes the run red. The lines above the summary name the person and the error:

- `Notion error ... X is not a property that exists`: the property `X` was renamed or deleted in Notion.
- `Notion error ... X is expected to be ...`: the property type of `X` was changed to one the value does not fit.
- `Slack error ... missing_scope`: the Slack app lacks a permission; see [Setup](#setup).
- `Slack error ... invalid_auth` or `token_revoked`: a Slack token in the secrets is no longer valid.
- `Notion error ... 401` or `object_not_found`: the Notion token is invalid, or the integration is no longer connected to the database.

To run the sync immediately instead of waiting for the next day: **Actions → Sync Slack members to Notion → Run workflow**.

---

## Setup

Only needed once, or when a token has to be replaced.

### GitHub secrets

**Settings → Secrets and variables → Actions**:

| Secret | Value | Where to get it |
|---|---|---|
| `SLACK_TOKEN` | `xoxb-…` | Slack app → OAuth & Permissions → **Bot User OAuth Token** |
| `SLACK_ADMIN_TOKEN` | `xoxp-…` | Slack app → OAuth & Permissions → **User OAuth Token**, installed by a Workspace Admin or Owner |
| `NOTION_TOKEN` | `ntn_…` | notion.so/profile/integrations → the integration → **Internal Integration Secret** |
| `NOTION_DATABASE_ID` | 32 characters | The database URL: `notion.so/<workspace>/<THIS PART>?v=…` |

### Slack app

At https://api.slack.com/apps → the app → **OAuth & Permissions**:

- **Bot Token Scopes:** `users:read`, `users.profile:read`
- **User Token Scopes:** `users.profile:write`

After changing scopes, click **Reinstall to Workspace**. The person who reinstalls must be a Workspace Admin or Owner, otherwise the User OAuth Token cannot change other people's titles.

### Notion

- The integration must be connected to the database: open the database → `•••` → **Connections** → add the integration.
- The properties listed above must exist with exactly these names.

---

## Files

| File | Content |
|---|---|
| `script.py` | The sync. Settings (Notion property names, Slack field IDs, the oldest synced member) are at the top. |
| `countries.csv` | Country names with their adjectives and alternative spellings, used for Nationality. |
| `.github/workflows/sync-slack-birthdays.yml` | Runs `script.py` daily at 06:00 UTC and on demand. |
| `requirements.txt` | Python packages. |
