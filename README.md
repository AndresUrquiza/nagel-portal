# Nagel Law — Expense Portal

## What this repo does
- Hosts the expense portal on GitHub Pages (free, permanent URL)
- Runs the nightly invoice automation at 7:00 PM CST via GitHub Actions

## Files
- `index.html` — the expense portal (copy Expense_Portal_v4.html here and rename)
- `nagel_automation.py` — the nightly script
- `.github/workflows/nightly.yml` — the scheduler

## Secrets required (Settings → Secrets → Actions)
| Secret | What to put |
|---|---|
| `GOOGLE_CREDENTIALS_JSON` | Full contents of your service account JSON key |
| `ANTHROPIC_API_KEY` | Your Anthropic API key from console.anthropic.com |
| `DRIVE_ROOT_FOLDER_ID` | ID from the URL of your "Nagel Law — Intake" Drive folder |
| `EXCEL_FILE_ID` | ID from the URL of Firm_Expense_Tracker.xlsx on Drive |
| `SMTP_USER` | Your Gmail address |
| `SMTP_PASSWORD` | Gmail App Password (see setup guide) |

## How to find a Google Drive file/folder ID
Open the file or folder in Drive. The URL looks like:
`https://drive.google.com/drive/folders/1ABC123XYZ...`
The long string after `/folders/` or `/file/d/` is the ID.

## How to run manually
Go to Actions → Nagel Law — Nightly Expense Automation → Run workflow
