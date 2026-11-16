# Poll Watcher (PW) pipeline

> **Note on this copy:** This is a cleaned-up copy of work I did for the Georgia coordinated campaign, shared with permission. Internal details were replaced with placeholders: Google Sheet, Form and Drive links and IDs (`YOUR_SHEET_ID`, and so on), email addresses (`example.org`), the VAN API account (`YOUR_VAN_API_USER`), the BigQuery project (`your-gcp-project`), and teammates' names. No volunteer data, credentials or letters are included, so the scripts won't run as is. Some files named below (such as the Word letter templates) are not included.

## Background (read this first)

This pipeline was built in a short period of time, under tight deadlines, and it was shaped by several last-minute requests from the Voter Protection (VoPro) team as needs changed during the campaign. It replaced an earlier automation attempt from the 2026 Primary that hadn't worked as hoped. I joined two months after the Primary to rebuild the process so it would be more efficient for volunteers and for Voter Protection to manage.

The goal was to modernize a workflow VoPro had refined over many years, so the design balances new automation with how the team already worked. For example, the email wording is pulled at send time from content blocks that VoPro maintains in Google Drive and Sheets, so their staff could keep editing messages without touching code. Because of the time pressure, the code favors working by the deadline over elegance. If something here looks odd, it probably has a deadline story behind it.

In the time available, I'm proud of what it accomplished.

## What this is

Scripts that ran the Voter Protection poll-watcher program for the 2026 Georgia coordinated campaign: from a volunteer filling out the interest form, through assignment, credentialing and county notification, to the reminders sent before their shift. It was built and run for early voting and Election Day 2026.

Everything runs as Python scripts (originally as portal / Cloud Build jobs, with parameters passed in as environment variables) against BigQuery (`your-gcp-project`, `commons.pw_*` tables), the VAN / VoteBuilder API, Google Sheets, and Gmail through a service account.

No credentials live in this repo. The service account JSON and VAN API tokens were supplied at run time as secrets.

## The two halves

### A. Intake and training (volunteer signs up)

| Script | What it does |
|---|---|
| `new_foc.py` | Reads form responses from a Google Sheet and does a VAN find-or-create for each person. Also books them into a training event. |
| `pw_activistCodes.py` | Applies activist codes (language, out-of-state, VoPro volunteer) to the people `new_foc.py` created. |
| `pw_surveyQuestions.py` | Applies survey question responses (county and shift preference) in VAN. |
| `apply_events.py` | Applies event signups in VAN. Cancels stale signups when someone resubmits. |
| `confirmation_email.py` | Sends the training confirmation email. |
| `pw_training_reminder_email.py` | Sends training reminders, with a calendar invite (`.ics`) attached. |
| `post_training_email.py` | Sends the follow-up after a training, with the on-demand training and walkthrough links. |
| `reschedule_foc.py`, `reschedule_ac.py`, `reschedule_sq.py`, `reschedule_email.py` | The same flow for people who fill out the reschedule form. They read a separate sheet and write to separate audit tables. |
| `ics_helpers.py` | Shared helper that builds calendar invites. Used by the email scripts. |
| `build_pw_scheduled_trainings_daily.sql`, `post_training_status_daily.sql` | Daily snapshot tables of who is scheduled for, and who completed, a training. |

### B. Assignments, credentials and reminders (the numbered stages)

Run in this order:

1. **`pw_ingest_assignments.py` (Stage 1)** reads the LBJ CSV exports in `csv_assignments_input/` and merges them into `commons.pw_credential_status` (one row per volunteer per county). It also rebuilds `pw_credential_shifts`. It flags rows that are new or changed, flags multi-county volunteers for human review, and refuses to run if the new file is under 80% the size of the current list (`allow_shrink=true` overrides).
2. **`pw_assignment_emails.py` (Stage 2)** sends one combined email per volunteer with all their shifts. Multi-county volunteers go to a review inbox first.
3. **Stage 3** is manual. VoPro confirms assignments by hand.
4. **`credential_letters.py` and `pw_county_email.py` (Stage 4)**
   - `credential_letters.py` generates one credential letter PDF per volunteer per county from a Word template (`EV Credential Template.docx` for early voting, `Eday Credential Template.docx` for Election Day). It also builds each county's roster CSV and a combined PDF.
   - `pw_county_email.py` creates one Gmail draft per county with the roster and PDF attached. Drafts only, never sent.
5. **`pw_send_credential_emails.py` (Stage 5)** sends each confirmed volunteer their credential email. Their letter PDF is looked up in a Drive folder as `<County>/<Last>_<First>_<VANID>.pdf`. For Fulton and DeKalb it adds their nearest badge pickup site (see below). Sends are queued from a BigQuery view and logged to a results table.
6. **`pw_credential_reminder.py` (Stage 6)** sends the day-before-shift reminder, one email per shift. It runs from a "Reminder List" sheet and only includes shifts that start within the configured window.

### Badge pickup (Fulton and DeKalb)

- `build_badge_pickup_tables.sql` builds the pickup sites table (address, hours, text blurbs, coordinates).
- `step2_nearest_pickup.sql` (early voting) and `step2_nearest_pickup_eday.sql` (Election Day) geocode each volunteer from the voter file and assign the nearest site in their own county. Volunteers the voter file misses can be added to a manual coordinates table.
- Run them in the BigQuery console, in order, whenever the sites or volunteer list change.

## Running it

- Install dependencies with `pip install -r requirements.txt`. `credential_letters.py` also needs `python-docx`, and the PDF step needs LibreOffice available on the machine.
- Every script reads its settings from environment variables (`project`, `spreadsheet_id`, `queue_view`, `results_table`, and so on). Read the top of each script for the names it expects.
- `test_pw_ingest_changes.py` has unit tests for the change detection in the ingest script: `python -m unittest test_pw_ingest_changes -v`.

## Lessons from running it (read before you send anything)

- **Send in batches and check between them.** The credential sender has a `send_limit` (real mode only). The results table is written only at the end of a run, so if a run dies midway, check the queue view before rerunning.
- **Test mode needs `test_limit`.** With `test_recipient` set and no `test_limit`, the whole queue goes to the tester's inbox.
- **Dry run first, every time.** Both senders support `DRY_RUN`.
- **Never run two send jobs at once.** The queue and results tables aren't locked.
- **The portal pulls from a Cloud Source mirror of GitHub, and it can lag.** If a job runs old code, push a trivial commit to trigger a resync, and check the commit hash in the build log.
- **Bad emails fail quietly.** Check the reminder list for typos like `.con` and `.comcom` before sending. A bounce shows up in the sender's inbox, not in the script.
- **Multi-shift volunteers get one reminder per shift row.** There is no in-run dedup.
- **The PDF attachment on the reminder script is stubbed.** Reminders go out without one.
- **The VAN IDs in the LBJ exports did not match the voter file's IDs.** The geocoding script matches by ID first, then by name and ZIP.
- **Rotate the service account key** if it was ever pasted anywhere outside the secret store.

## Data caution

The CSVs, letter PDFs and `credentials_output/` folder this pipeline produces contain volunteer names, emails, phone numbers and addresses. They are git-ignored or kept local on purpose. Don't commit them.
