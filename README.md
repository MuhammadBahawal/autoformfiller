# AdsPower Excel Form Filler

This starter project lets you run multiple AdsPower browser profiles in parallel, and each browser picks the next pending Excel row to fill a web form.

## What This Script Does

- Reads data from an Excel `.xlsx` file
- Skips rows already marked `DONE`
- Runs multiple AdsPower browser profiles in parallel
- Assigns the next available row to each browser
- Writes the result back into Excel after the form is filled

## Requirements

1. AdsPower must be installed and running on the machine.
2. AdsPower Local API must be enabled.
3. The Excel file should be closed while the script is running, otherwise saving may fail.
4. Python 3.11 or a close version should be installed.

## Setup

```powershell
cd D:\autoformfiller
python -m pip install -r requirements.txt
Copy-Item config.example.json config.json
```

## What To Edit In `config.json`

- `adspower.profile_ids`: add your AdsPower browser/profile IDs here.
- `adspower.use_active_profiles`: set this to `true` if you want the script to attach to AdsPower browsers that are already open.
- `excel.path`: set this to the full path of your own Excel file.
- `form.use_existing_page`: keep this `true` if you open the form manually inside AdsPower before running the script.
- `form.tab_index`: controls which browser tab contains the form. `-1` means the last/opened tab.
- `form.url`: only use this when you want the script to open the form URL by itself for each row.
- `form.target_url_contains`: keep this aligned with the real first-step form page, for example `stimulusassistforall.com/index-v8-form.php`. If this is wrong, one browser may work while other open browsers stay on the wrong tab.
- `fields`: verify that the Excel column names and form selectors match your real form.
- `form.submit_after_fill`: keep this `false` while testing if you do not want to submit immediately.
- `form.submit_selector`: once testing is correct, set the real submit button selector and enable `submit_after_fill`.
- `form.history_recover_max_steps`: if a browser was left on a survey or offers page, the script can try a few `Back` steps to return that browser to the form automatically.
- `form.zip_step`: leave this enabled when some forms first show only the ZIP field. The script will fill ZIP, wait for the button to become ready, retry `Next` / `Continue` if needed, then fill the remaining fields on the next page. If the button text is unusual, add its selector in `form.zip_step.next_selectors`.
- `form.handle_surveys_after_submit`: keeps the post-submit survey handler on. It is designed for button-style survey choices, radio groups, checkbox lists, `Continue` / `Next` pages, and final CTA pages such as `Get`, `Claim`, `Finish`, or `Complete`.
- `form.survey_retry_count` / `form.survey_max_pages`: control how many survey reload retries are allowed and how many survey pages each browser will process before failing.

Important:

Anyone who downloads this project must update the Excel path inside `config.json`. For example, if you see:

```json
"path": "D:/formfilling/data.xlsx"
```

replace it with the full path to your own Excel file on your machine.

## Run

```powershell
cd D:\autoformfiller
python form_filler.py --config config.json
```

The current config uses `max_rows_per_profile = 1`, so on each run:

- Browser 1 takes the first pending row, Browser 2 takes the second pending row, and so on
- Only as many rows are processed as there are available AdsPower browsers
- Each browser fills only 1 row
- The configured `Continue` or submit action is triggered
- As soon as the main form submit reaches the next post-submit/survey page, the Excel row is marked `DONE`.
- After the main form submit, the script can keep answering the follow-up survey until it reaches the next step or a completion/final CTA page.
- Built-in survey answer rules now work like this:
  - `what is your current employment status` -> choose `Employed`
  - `do you own an active bank account` -> choose `Yes`
  - otherwise the script prefers `None of the Above`, `None Above`, `No Above`, `Never`, `No`, `None`, or `Not Applicable`
  - if none of those answers are visible, it selects one random visible option only
- Radio options, checkbox options, and button-style answers are supported, and checkbox/radio groups are kept to a single selected answer.
- The processed row is marked `DONE` in Excel
- Remaining rows stay pending for the next run

## Auto Tracking Columns In Excel

The script automatically creates and uses these tracking columns:

- `__status`
- `__message`
- `__processed_at`
- `__profile_id`

Rows marked `DONE` are skipped automatically in future runs.

## Default Field Mapping Included

The example config already includes starter mappings for:

- `First Name`
- `Last Name`
- `Street Address`
- `Zip Code`
- `Email Address`
- `Phone Number`
- `Birth Month`
- `Birth Day`
- `Birth Year`

A starter checkbox selector is also included. On some real pages the checkbox may be hidden or clickable only through its label, so you may need to inspect the page and update that selector.

## Two Supported Modes

### 1. You Open The Form Manually In AdsPower

- `adspower.use_active_profiles = true`
- `adspower.profile_ids = []`
- `form.use_existing_page = true`
- `form.url = ""`
- Open the target form page manually inside each AdsPower profile/browser first.
- The script will attach to the configured tab and fill the data.

### 2. The Script Opens The Form By Itself

- `form.use_existing_page = false`
- `form.url = "https://your-real-form-url"`
- The script will open the form URL for each row and then fill it.

## Important Note

This project is a configurable starter. You still need to confirm the real form URL, HTML selectors, and any page-specific behavior before using it in production.
