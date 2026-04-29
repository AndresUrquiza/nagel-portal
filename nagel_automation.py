#!/usr/bin/env python3
"""
nagel_automation.py — Nagel Law Expense Automation
===================================================
Runs nightly at 7:00 PM via GitHub Actions.

What it does:
  1. Scans each entity folder in Google Drive for new documents
  2. Sends each file to Claude AI which extracts: vendor, amount,
     date, category, entity, invoice number, and confidence score
  3. Confidence >= 90% → appended to Firm_Expense_Tracker.xlsx
  4. Confidence <  90% → saved to a "Needs Review" sheet for Andres
  5. Processed files moved to /done subfolder
  6. New entity folders detected automatically — no code changes needed
  7. Summary email sent to andres@nagellaw.com

Setup:
  pip install google-api-python-client google-auth openpyxl anthropic pillow

Environment variables (set in GitHub Actions secrets):
  GOOGLE_CREDENTIALS_JSON  — contents of your service account JSON key file
  ANTHROPIC_API_KEY        — your Anthropic API key
  SUMMARY_EMAIL            — andres@nagellaw.com
  SMTP_USER                — Gmail address used to send the summary
  SMTP_PASSWORD            — Gmail App Password (not your regular password)
  DRIVE_ROOT_FOLDER_ID     — ID of the "Nagel Law — Intake" Drive folder
  EXCEL_FILE_ID            — ID of Firm_Expense_Tracker.xlsx on Drive
"""

import os
import io
import json
import base64
import tempfile
import smtplib
import logging
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Google
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# Excel
import openpyxl

# Anthropic
import anthropic

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("nagel")

# ─── Config ─────────────────────────────────────────────────────────────────
ENTITIES = ["AFLE", "GT Nevis", "GT Bank", "Nagel & Associates"]
UNRECOGNIZED_FOLDER = "_Unrecognized"
DONE_FOLDER = "done"
CONFIDENCE_THRESHOLD = 0.90
SUMMARY_EMAIL = os.environ.get("SUMMARY_EMAIL", "andres@nagellaw.com")
DRIVE_ROOT_ID = os.environ.get("DRIVE_ROOT_FOLDER_ID", "")
EXCEL_FILE_ID = os.environ.get("EXCEL_FILE_ID", "")

SUPPORTED_MIME = {
    "application/pdf",
    "image/jpeg",
    "image/png",
    "image/heic",
    "image/webp",
    "image/tiff",
}

CATEGORIES = [
    "Payroll",
    "Rent & Utilities",
    "Insurance",
    "Professional Services",
    "Marketing",
    "Travel & Meals",
    "Software & Subscriptions",
    "Office Supplies",
    "Bank & Merchant Fees",
    "Equipment",
    "Taxes & Licenses",
    "Other",
]

# ─── Google Drive client ─────────────────────────────────────────────────────
def get_drive_service():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS_JSON env var not set.")
    info = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)


# ─── Drive helpers ───────────────────────────────────────────────────────────
def list_subfolders(drive, parent_id):
    """Return {name: id} for all subfolders of parent_id. Supports Shared Drives."""
    result = drive.files().list(
        q=f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
        fields="files(id, name)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        corpora="allDrives"
    ).execute()
    return {f["name"]: f["id"] for f in result.get("files", [])}


def list_files(drive, folder_id):
    """Return list of file dicts in a folder. Supports Shared Drives."""
    result = drive.files().list(
        q=f"'{folder_id}' in parents and mimeType!='application/vnd.google-apps.folder' and trashed=false",
        fields="files(id, name, mimeType, size)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        corpora="allDrives"
    ).execute()
    return result.get("files", [])


def ensure_subfolder(drive, parent_id, name):
    """Get or create a subfolder by name. Supports Shared Drives."""
    existing = list_subfolders(drive, parent_id)
    if name in existing:
        return existing[name]
    meta = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id]
    }
    folder = drive.files().create(
        body=meta,
        fields="id",
        supportsAllDrives=True
    ).execute()
    log.info(f"Created subfolder '{name}' in Drive.")
    return folder["id"]


def download_file(drive, file_id):
    """Download a Drive file into a BytesIO buffer. Supports Shared Drives."""
    buf = io.BytesIO()
    request = drive.files().get_media(
        fileId=file_id,
        supportsAllDrives=True
    )
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buf.seek(0)
    return buf


def find_file_in_folder(drive, folder_id, filename):
    """Search for a file by name inside a folder. Supports Shared Drives."""
    result = drive.files().list(
        q=f"'{folder_id}' in parents and name='{filename}' and trashed=false",
        fields="files(id, name)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
        corpora="allDrives"
    ).execute()
    files = result.get("files", [])
    return files[0]["id"] if files else None


def move_file(drive, file_id, new_parent_id, old_parent_id):
    """Move a file to a different folder. Supports Shared Drives."""
    drive.files().update(
        fileId=file_id,
        addParents=new_parent_id,
        removeParents=old_parent_id,
        fields="id, parents",
        supportsAllDrives=True
    ).execute()


def upload_excel(drive, local_path):
    """
    Upload updated Excel back to Drive.
    Searches for the file by name inside the root intake folder.
    If found — updates it in place.
    If not found — creates a new one inside the root folder.
    """
    excel_name = "Firm_Expense_Tracker.xlsx"
    media = MediaFileUpload(
        local_path,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        resumable=False
    )

    # Search for the file by name inside the intake folder
    found_id = find_file_in_folder(drive, DRIVE_ROOT_ID, excel_name)

    if found_id:
        log.info(f"Found Excel in Drive (id={found_id}), updating...")
        drive.files().update(
            fileId=found_id,
            media_body=media,
            supportsAllDrives=True
        ).execute()
        log.info("Excel file updated on Drive successfully.")
    else:
        log.info("Excel not found in intake folder — creating new file...")
        file_meta = {
            "name": excel_name,
            "parents": [DRIVE_ROOT_ID]
        }
        drive.files().create(
            body=file_meta,
            media_body=media,
            fields="id",
            supportsAllDrives=True
        ).execute()
        log.info("New Excel file created in intake folder.")


# ─── AI extraction ───────────────────────────────────────────────────────────
def extract_invoice_data(file_bytes: bytes, filename: str, entity: str, mime_type: str) -> dict:
    """
    Send the document to Claude and get structured invoice data back.
    Returns a dict with keys: vendor, amount, date, category, invoice_number,
    description, confidence, notes
    """
    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))

    # Convert to base64
    b64 = base64.standard_b64encode(file_bytes).decode("utf-8")

    # Normalise mime for Claude (HEIC not supported — convert hint)
    claude_mime = mime_type
    if mime_type not in {"image/jpeg", "image/png", "image/gif", "image/webp",
                         "application/pdf"}:
        claude_mime = "image/jpeg"   # fallback — PIL conversion happens below

    categories_list = "\n".join(f"- {c}" for c in CATEGORIES)

    prompt = f"""You are an expert bookkeeper reviewing a business expense document for Nagel Law.

The document was uploaded to the "{entity}" entity folder.

Extract the following information from the document:

1. vendor — the company or person being paid
2. amount — the total amount in USD (number only, no $ sign)
3. date — the invoice or transaction date in YYYY-MM-DD format
4. category — pick the single best match from this list:
{categories_list}
5. invoice_number — the invoice or reference number if present, else "N/A"
6. description — a short 3-8 word description of what was purchased
7. confidence — a number from 0.0 to 1.0 representing how certain you are
   about ALL of the above fields combined. Be strict:
   - 0.95+ only if every field is clearly visible and unambiguous
   - 0.80-0.94 if one field is estimated or partially visible
   - below 0.80 if the document is unclear, damaged, or missing key info
8. due_date — the payment due date in YYYY-MM-DD format, or null if not found
9. is_paid — true if the document shows a payment was made (Balance Due = $0.00,
   or shows a payment line with a negative amount, or is stamped/marked PAID).
   false if balance is still outstanding.
10. payment_date — if is_paid is true, the date the payment was made in YYYY-MM-DD,
    or null if not visible.
11. notes — if confidence is below 0.90, briefly explain what is unclear

Respond ONLY with a valid JSON object, no markdown, no explanation:
{{
  "vendor": "...",
  "amount": 0.00,
  "date": "YYYY-MM-DD",
  "category": "...",
  "invoice_number": "...",
  "description": "...",
  "due_date": "YYYY-MM-DD or null",
  "is_paid": false,
  "payment_date": "YYYY-MM-DD or null",
  "confidence": 0.00,
  "notes": "..."
}}"""

    try:
        if claude_mime == "application/pdf":
            content = [
                {
                    "type": "document",
                    "source": {
                        "type": "base64",
                        "media_type": "application/pdf",
                        "data": b64
                    }
                },
                {"type": "text", "text": prompt}
            ]
        else:
            content = [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": claude_mime,
                        "data": b64
                    }
                },
                {"type": "text", "text": prompt}
            ]

        response = client.messages.create(
            model="claude-opus-4-6",
            max_tokens=600,
            messages=[{"role": "user", "content": content}]
        )

        raw = response.content[0].text.strip()
        # Strip markdown fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        data = json.loads(raw.strip())

        # Validate required fields
        for key in ["vendor", "amount", "date", "category", "confidence"]:
            if key not in data:
                raise ValueError(f"Missing field: {key}")

        data["amount"] = float(data.get("amount", 0))
        data["confidence"] = float(data.get("confidence", 0))
        return data

    except Exception as e:
        log.warning(f"AI extraction failed for {filename}: {e}")
        return {
            "vendor": "Unknown",
            "amount": 0.0,
            "date": datetime.today().strftime("%Y-%m-%d"),
            "category": "Other",
            "invoice_number": "N/A",
            "description": "Extraction failed",
            "confidence": 0.0,
            "notes": str(e)
        }


# ─── Excel helpers ───────────────────────────────────────────────────────────
TRANSACTIONS_HEADERS = [
    "Date", "Year", "Month", "Entity", "Vendor", "Category",
    "Description", "Amount (USD)", "Status", "Source",
    "Invoice #", "File Name", "Due Date", "Payment Date"
]

REVIEW_HEADERS = [
    "Date Processed", "Entity", "File Name",
    "Vendor (AI)", "Amount (AI)", "Date (AI)",
    "Category (AI)", "Invoice # (AI)", "Description (AI)",
    "Confidence", "AI Notes", "Status"
]


def get_or_create_sheet(wb, name, headers):
    if name in wb.sheetnames:
        return wb[name]
    ws = wb.create_sheet(name)
    ws.append(headers)
    return ws


def first_empty_row(ws, start_row=3):
    """
    Find the first truly empty row starting from start_row.
    Skips title rows and header rows at the top.
    Default start_row=3 matches the Excel structure:
      Row 1 = Title banner
      Row 2 = Column headers
      Row 3+ = Data
    """
    for row_num in range(start_row, ws.max_row + 2):
        row_vals = [ws.cell(row=row_num, column=c).value for c in range(1, 12)]
        if all(v is None or str(v).strip() == "" for v in row_vals):
            return row_num
    return ws.max_row + 1


def is_duplicate(ws, filename, data=None, start_row=4):
    """
    Check for duplicates using TWO methods:

    Method 1 — Filename match:
      If the exact filename already exists in column 10, it's a duplicate.

    Method 2 — Content match (vendor + amount + date):
      Even if the filename is different, if vendor + amount + date all
      match an existing row it's the same invoice uploaded twice.
      Columns: 1=Date, 3=Vendor, 6=Amount

    Returns (True, reason) if duplicate, (False, "") if new.
    """
    for row_num in range(start_row, ws.max_row + 1):
        # Method 1: filename — column 12
        file_cell = ws.cell(row=row_num, column=12).value
        if file_cell and str(file_cell).strip() == str(filename).strip():
            return True, f"Filename already exists in row {row_num}", row_num

        # Method 2: vendor + amount + invoice_number (all three must match)
        #
        # This is the correct logic:
        # - Monthly rent: same vendor, same amount, DIFFERENT invoice# → valid, not duplicate
        # - Same invoice twice: same vendor, same amount, SAME invoice# → duplicate, skip
        # - Renamed file: same vendor, same amount, SAME invoice# → duplicate, skip
        #
        # Invoice# is the unique identifier per invoice.
        # Vendor is fuzzy-matched (partial) to handle slight name variations.
        # Amount must match to the cent.
        if data:
            ex_vendor = str(ws.cell(row=row_num, column=5).value or "").strip().lower()
            ex_amount = ws.cell(row=row_num, column=8).value
            ex_inv    = str(ws.cell(row=row_num, column=11).value or "").strip()

            new_vendor = str(data.get("vendor", "")).strip().lower()
            new_amount = data.get("amount", None)
            new_inv    = str(data.get("invoice_number", "")).strip()

            # Skip if invoice number is missing or generic
            if not ex_inv or not new_inv or ex_inv in ("N/A", "") or new_inv in ("N/A", ""):
                continue

            if ex_vendor and ex_amount is not None and new_amount is not None:
                vendor_match = (
                    ex_vendor == new_vendor or
                    (len(ex_vendor) > 4 and len(new_vendor) > 4 and
                     (ex_vendor in new_vendor or new_vendor in ex_vendor))
                )
                amount_match = abs(float(ex_amount) - float(new_amount)) < 0.01
                inv_match    = ex_inv == new_inv

                if vendor_match and amount_match and inv_match:
                    return True, (
                        f"Duplicate in row {row_num} — "
                        f"vendor: '{ex_vendor}' / amount: ${ex_amount} / invoice#: {ex_inv}"
                    ), row_num

    return False, "", None


def append_transaction(ws, entity, data, filename):
    """
    Append a confirmed row to the first empty row in Transactions sheet.
    Column layout (matches Firm_Expense_Tracker.xlsx):
      A(1)=Date  B(2)=Year  C(3)=Month  D(4)=Entity  E(5)=Vendor
      F(6)=Category  G(7)=Description  H(8)=Amount(USD)
      I(9)=Status  J(10)=Source  K(11)=Invoice#  L(12)=Filename
    """
    row = first_empty_row(ws)
    date_str = data.get("date", "")
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        year  = dt.year
        month = dt.strftime("%B")   # e.g. "January"
    except Exception:
        year  = ""
        month = ""

    due_date = data.get("due_date", None)
    if due_date == "null" or due_date == "":
        due_date = None

    ws.cell(row=row, column=1).value  = date_str
    ws.cell(row=row, column=2).value  = year
    ws.cell(row=row, column=3).value  = month
    ws.cell(row=row, column=4).value  = entity
    ws.cell(row=row, column=5).value  = data.get("vendor", "")
    ws.cell(row=row, column=6).value  = data.get("category", "")
    ws.cell(row=row, column=7).value  = data.get("description", "")
    ws.cell(row=row, column=8).value  = data.get("amount", 0)
    ws.cell(row=row, column=9).value  = "Pending"
    ws.cell(row=row, column=10).value = "Drive"
    ws.cell(row=row, column=11).value = data.get("invoice_number", "N/A")
    ws.cell(row=row, column=12).value = filename
    ws.cell(row=row, column=13).value = due_date
    log.info(f"  Written to row {row} — {entity} | {data.get('vendor')} | ${data.get('amount')} | due: {due_date}")


def append_review(ws, entity, data, filename):
    """Append a low-confidence row to the first empty row in Needs Review sheet."""
    row = first_empty_row(ws, start_row=3)
    ws.cell(row=row, column=1).value  = datetime.today().strftime("%Y-%m-%d")
    ws.cell(row=row, column=2).value  = entity
    ws.cell(row=row, column=3).value  = filename
    ws.cell(row=row, column=4).value  = data.get("vendor", "")
    ws.cell(row=row, column=5).value  = data.get("amount", "")
    ws.cell(row=row, column=6).value  = data.get("date", "")
    ws.cell(row=row, column=7).value  = data.get("category", "")
    ws.cell(row=row, column=8).value  = data.get("invoice_number", "")
    ws.cell(row=row, column=9).value  = data.get("description", "")
    ws.cell(row=row, column=10).value = f"{data.get('confidence', 0)*100:.0f}%"
    ws.cell(row=row, column=11).value = data.get("notes", "")
    ws.cell(row=row, column=12).value = "Needs Review"
    log.info(f"  Written to review row {row}")


def ensure_entity_in_excel(ws_entities, entity_name):
    """Add entity row to Entities sheet if not already there."""
    existing = [ws_entities.cell(row=r, column=1).value
                for r in range(2, ws_entities.max_row + 1)]
    if entity_name not in existing:
        # Generate a simple code from the name
        code = entity_name.upper().replace(" ", "-")[:8]
        ws_entities.append([entity_name, code])
        log.info(f"New entity added to Excel: {entity_name}")


# ─── Main pipeline ───────────────────────────────────────────────────────────
def run():
    log.info("=" * 60)
    log.info("Nagel Law — Expense Automation starting")
    log.info("=" * 60)

    drive = get_drive_service()
    ai_client_check = os.environ.get("ANTHROPIC_API_KEY", "")
    if not ai_client_check:
        raise ValueError("ANTHROPIC_API_KEY env var not set.")

    # ── Verify root Shared Drive access ──
    try:
        root_info = drive.files().get(
            fileId=DRIVE_ROOT_ID,
            fields="id, name",
            supportsAllDrives=True
        ).execute()
        log.info(f"Connected to Shared Drive: '{root_info.get('name', 'unknown')}'")
    except Exception as e:
        log.warning(f"Could not get root info: {e}")

    # ── Download current Excel from Drive ──
    log.info("Downloading Excel from Drive...")
    excel_name = "Firm_Expense_Tracker.xlsx"
    excel_id = find_file_in_folder(drive, DRIVE_ROOT_ID, excel_name)
    if not excel_id:
        raise FileNotFoundError(
            f"Could not find '{excel_name}' inside the Intake folder. "
            f"Make sure the file is inside the shared 'Nagel Law — Intake' folder "
            f"and the service account has Editor access to that folder."
        )
    log.info(f"Found Excel file (id={excel_id}), downloading...")
    excel_bytes = download_file(drive, excel_id)
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        tmp.write(excel_bytes.read())
        excel_path = tmp.name

    wb = openpyxl.load_workbook(excel_path)

    # Ensure required sheets exist
    ws_tx     = get_or_create_sheet(wb, "Transactions", TRANSACTIONS_HEADERS)
    ws_review = get_or_create_sheet(wb, "Needs Review", REVIEW_HEADERS)
    ws_ents   = get_or_create_sheet(wb, "Entities",
                                    ["Entity Name", "Entity Code"])

    # ── Discover entity folders (auto-detects new ones) ──
    log.info("Scanning Drive folder structure...")
    subfolders = list_subfolders(drive, DRIVE_ROOT_ID)

    # Track results for summary email
    results = {
        "processed": [],    # (entity, filename, amount, confidence)
        "flagged": [],      # (entity, filename, confidence, notes)
        "skipped": [],      # (entity, filename, reason)
        "new_entities": []  # entity names auto-detected
    }

    # ── Process each entity folder ──
    log.info(f"All subfolders found: {list(subfolders.keys())}")
    for folder_name, folder_id in subfolders.items():

        # Skip system folders and known non-entity files/folders
        SKIP_NAMES = {"00_INBOX", "00_UNCATEGORIZED", "Filing Taxonomy", DONE_FOLDER}
        if folder_name.startswith("_") or folder_name.startswith("00_") or folder_name in SKIP_NAMES:
            log.info(f"Skipping system folder: {folder_name}")
            continue

        log.info(f"Processing folder: '{folder_name}'")
        
        # List files found
        files_found = list_files(drive, folder_id)
        log.info(f"  Files in '{folder_name}': {[f['name'] for f in files_found]}")

        # Auto-detect new entity
        if folder_name not in ENTITIES:
            log.info(f"New entity detected: {folder_name}")
            results["new_entities"].append(folder_name)
            ensure_entity_in_excel(ws_ents, folder_name)

        # Ensure /done subfolder exists
        done_id = ensure_subfolder(drive, folder_id, DONE_FOLDER)

        # List files in this entity folder
        files = files_found
        if not files:
            log.info(f"  No files in {folder_name}")
            continue

        for f in files:
            filename = f["name"]
            file_id  = f["id"]
            mime     = f.get("mimeType", "")

            log.info(f"  Reading: {filename} ({mime})")

            # Skip unsupported types
            if mime not in SUPPORTED_MIME:
                log.warning(f"  Skipping unsupported type: {mime}")
                results["skipped"].append((folder_name, filename, f"Unsupported type: {mime}"))
                continue

            # Download file
            try:
                file_bytes = download_file(drive, file_id).read()
            except Exception as e:
                log.error(f"  Download failed: {e}")
                results["skipped"].append((folder_name, filename, f"Download error: {e}"))
                continue

            # Filename-only duplicate check before AI call (saves API cost)
            dup, reason, dup_row = is_duplicate(ws_tx, filename)
            if dup:
                log.info(f"  → DUPLICATE (filename) — {reason}. Skipping.")
                results["skipped"].append((folder_name, filename, f"Duplicate: {reason}"))
                continue

            # AI extraction
            data = extract_invoice_data(file_bytes, filename, folder_name, mime)

            # Content duplicate check after AI extraction (vendor+amount+invoice#)
            dup, reason, dup_row = is_duplicate(ws_tx, filename, data=data)
            if dup:
                # Check if this is a paid receipt — if so, update status
                is_paid = data.get("is_paid", False)
                payment_date = data.get("payment_date", None)
                if is_paid and dup_row:
                    log.info(f"  → PAID RECEIPT detected — updating row {dup_row} to Paid.")
                    update_status_to_paid(ws_tx, dup_row, payment_date)
                    results["processed"].append((
                        folder_name, filename,
                        0, data.get("confidence", 0)
                    ))
                else:
                    log.info(f"  → DUPLICATE — {reason}. Skipping.")
                    results["skipped"].append((folder_name, filename, f"Duplicate: {reason}"))
                continue
            confidence = data.get("confidence", 0)

            log.info(f"  Confidence: {confidence*100:.0f}% | "
                     f"Vendor: {data.get('vendor')} | "
                     f"Amount: ${data.get('amount')}")

            # Route based on confidence
            if confidence >= CONFIDENCE_THRESHOLD:
                append_transaction(ws_tx, folder_name, data, filename)
                results["processed"].append((
                    folder_name, filename,
                    data.get("amount", 0), confidence
                ))
                log.info(f"  → Written to Transactions sheet")
            else:
                append_review(ws_review, folder_name, data, filename)
                results["flagged"].append((
                    folder_name, filename, confidence,
                    data.get("notes", "Low confidence")
                ))
                log.info(f"  → Flagged for review (confidence too low)")

            # Move file to /done
            move_file(drive, file_id, done_id, folder_id)
            log.info(f"  → Moved to /done")

    # ── Save and upload Excel ──
    wb.save(excel_path)
    upload_excel(drive, excel_path)

    # ── Send summary email ──
    send_summary_email(results)

    log.info("=" * 60)
    log.info("Run complete.")
    log.info("=" * 60)


# ─── Summary email ────────────────────────────────────────────────────────────
def send_summary_email(results):
    processed = results["processed"]
    flagged   = results["flagged"]
    skipped   = results["skipped"]
    new_ents  = results["new_entities"]

    total_amount = sum(r[2] for r in processed)
    today = datetime.today().strftime("%B %d, %Y")

    # Plain text
    lines = [
        f"Nagel Law — Nightly Expense Report",
        f"Run date: {today}",
        f"{'='*48}",
        f"",
        f"PROCESSED AUTOMATICALLY ({len(processed)} documents — ${total_amount:,.2f} total)",
    ]
    for entity, fname, amount, conf in processed:
        lines.append(f"  ✓  [{entity}]  {fname}  —  ${amount:,.2f}  ({conf*100:.0f}% confidence)")

    if flagged:
        lines += [
            f"",
            f"NEEDS YOUR REVIEW ({len(flagged)} documents — confidence below 90%)",
        ]
        for entity, fname, conf, notes in flagged:
            lines.append(f"  ⚠  [{entity}]  {fname}  —  {conf*100:.0f}%  |  {notes}")
        lines.append(f"  → Open the portal → 'Needs Review' tab to confirm or correct these.")

    if new_ents:
        lines += [
            f"",
            f"NEW ENTITIES DETECTED ({len(new_ents)})",
        ]
        for e in new_ents:
            lines.append(f"  +  {e}  — added to Excel and portal automatically")

    if skipped:
        lines += [
            f"",
            f"SKIPPED ({len(skipped)} files — unsupported format or error)",
        ]
        for entity, fname, reason in skipped:
            lines.append(f"  ✗  [{entity}]  {fname}  —  {reason}")

    lines += [
        f"",
        f"{'='*48}",
        f"Nagel Law Expense Portal — automated nightly run",
    ]

    body_text = "\n".join(lines)

    # HTML version
    def row_color(i): return "#f9f9f9" if i % 2 == 0 else "#ffffff"

    proc_rows = "".join(
        f'<tr style="background:{row_color(i)}">'
        f'<td style="padding:6px 10px">{entity}</td>'
        f'<td style="padding:6px 10px">{fname}</td>'
        f'<td style="padding:6px 10px;text-align:right">${amount:,.2f}</td>'
        f'<td style="padding:6px 10px;text-align:center;color:#15803d">{conf*100:.0f}%</td>'
        f'</tr>'
        for i, (entity, fname, amount, conf) in enumerate(processed)
    ) if processed else '<tr><td colspan="4" style="padding:10px;color:#aaa;text-align:center">No documents processed tonight</td></tr>'

    flag_rows = "".join(
        f'<tr style="background:{row_color(i)}">'
        f'<td style="padding:6px 10px">{entity}</td>'
        f'<td style="padding:6px 10px">{fname}</td>'
        f'<td style="padding:6px 10px;text-align:center;color:#b45309">{conf*100:.0f}%</td>'
        f'<td style="padding:6px 10px;color:#666">{notes}</td>'
        f'</tr>'
        for i, (entity, fname, conf, notes) in enumerate(flagged)
    ) if flagged else ""

    new_ent_section = ""
    if new_ents:
        items = "".join(f'<li style="margin:4px 0">{e} — added automatically</li>' for e in new_ents)
        new_ent_section = f"""
        <h3 style="color:#1a2f5a;margin:24px 0 8px">New Entities Detected</h3>
        <ul style="margin:0;padding-left:20px;color:#374151">{items}</ul>"""

    flagged_section = ""
    if flagged:
        flagged_section = f"""
        <h3 style="color:#b45309;margin:24px 0 8px">Needs Your Review ({len(flagged)})</h3>
        <p style="color:#666;font-size:13px;margin:0 0 10px">
          Confidence below 90% — open the portal's <strong>Needs Review</strong> tab to confirm or correct.
        </p>
        <table style="width:100%;border-collapse:collapse;font-size:13px">
          <thead>
            <tr style="background:#1a2f5a;color:#fff">
              <th style="padding:8px 10px;text-align:left">Entity</th>
              <th style="padding:8px 10px;text-align:left">File</th>
              <th style="padding:8px 10px;text-align:center">Confidence</th>
              <th style="padding:8px 10px;text-align:left">Notes</th>
            </tr>
          </thead>
          <tbody>{flag_rows}</tbody>
        </table>"""

    body_html = f"""
    <div style="font-family:'Helvetica Neue',Arial,sans-serif;max-width:640px;margin:0 auto;color:#1a1a1a">
      <div style="background:#1a2f5a;padding:24px 28px;border-radius:10px 10px 0 0">
        <div style="font-size:20px;font-weight:700;color:#fff">Nagel Law</div>
        <div style="font-size:13px;color:#c9a84c;letter-spacing:1px;text-transform:uppercase;margin-top:2px">
          Nightly Expense Report
        </div>
      </div>
      <div style="background:#f8f9fc;padding:18px 28px;border-left:1px solid #e4e7f0;border-right:1px solid #e4e7f0">
        <span style="font-size:13px;color:#888">{today}</span>
      </div>
      <div style="background:#fff;padding:24px 28px;border:1px solid #e4e7f0;border-top:none">

        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-bottom:24px">
          <div style="background:#f6f7fb;border-radius:8px;padding:14px 16px;border:1px solid #e4e7f0">
            <div style="font-size:10px;font-weight:700;color:#9ca3af;text-transform:uppercase;letter-spacing:.6px;margin-bottom:6px">Processed</div>
            <div style="font-size:24px;font-weight:700;color:#1a2f5a;font-family:monospace">{len(processed)}</div>
            <div style="font-size:11px;color:#9ca3af;margin-top:3px">documents tonight</div>
          </div>
          <div style="background:#f6f7fb;border-radius:8px;padding:14px 16px;border:1px solid #e4e7f0">
            <div style="font-size:10px;font-weight:700;color:#9ca3af;text-transform:uppercase;letter-spacing:.6px;margin-bottom:6px">Total Amount</div>
            <div style="font-size:24px;font-weight:700;color:#1a2f5a;font-family:monospace">${total_amount:,.0f}</div>
            <div style="font-size:11px;color:#9ca3af;margin-top:3px">auto-added to Excel</div>
          </div>
          <div style="background:{'#fef3c7' if flagged else '#f6f7fb'};border-radius:8px;padding:14px 16px;border:1px solid {'#fcd34d' if flagged else '#e4e7f0'}">
            <div style="font-size:10px;font-weight:700;color:#9ca3af;text-transform:uppercase;letter-spacing:.6px;margin-bottom:6px">Needs Review</div>
            <div style="font-size:24px;font-weight:700;color:{'#b45309' if flagged else '#1a2f5a'};font-family:monospace">{len(flagged)}</div>
            <div style="font-size:11px;color:#9ca3af;margin-top:3px">below 90% confidence</div>
          </div>
        </div>

        <h3 style="color:#1a2f5a;margin:0 0 10px">Processed Tonight ({len(processed)})</h3>
        <table style="width:100%;border-collapse:collapse;font-size:13px">
          <thead>
            <tr style="background:#1a2f5a;color:#fff">
              <th style="padding:8px 10px;text-align:left">Entity</th>
              <th style="padding:8px 10px;text-align:left">File</th>
              <th style="padding:8px 10px;text-align:right">Amount</th>
              <th style="padding:8px 10px;text-align:center">Confidence</th>
            </tr>
          </thead>
          <tbody>{proc_rows}</tbody>
        </table>

        {flagged_section}
        {new_ent_section}

      </div>
      <div style="background:#f6f7fb;padding:14px 28px;border-radius:0 0 10px 10px;border:1px solid #e4e7f0;border-top:none">
        <span style="font-size:11px;color:#bbb">Nagel Law Expense Portal — automated nightly run · andres@nagellaw.com</span>
      </div>
    </div>"""

    # Send
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASSWORD", "")

    if not smtp_user or not smtp_pass:
        log.warning("SMTP credentials not set — skipping email.")
        log.info("Summary:\n" + body_text)
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Nagel Law — Expense Report {today} ({len(processed)} docs · ${total_amount:,.0f})"
    msg["From"]    = smtp_user
    msg["To"]      = SUMMARY_EMAIL
    msg.attach(MIMEText(body_text, "plain"))
    msg.attach(MIMEText(body_html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, SUMMARY_EMAIL, msg.as_string())
        log.info(f"Summary email sent to {SUMMARY_EMAIL}")
    except Exception as e:
        log.error(f"Email send failed: {e}")


if __name__ == "__main__":
    run()
