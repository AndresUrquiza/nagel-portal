#!/usr/bin/env python3
"""
nagel_diagnostic.py — Nagel Law Drive Connection Test
======================================================
Run this ONCE from GitHub Actions to see exactly what
the service account can see in your Drive folders.

It does NOT process any files or modify anything.
It just reports what it finds.
"""

import os
import json
import logging

from google.oauth2 import service_account
from googleapiclient.discovery import build

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("nagel-diag")

DRIVE_ROOT_ID = os.environ.get("DRIVE_ROOT_FOLDER_ID", "")

def get_drive_service():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS_JSON not set.")
    info = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)


def check_folder(drive, folder_id, folder_name, depth=0):
    indent = "  " * depth
    log.info(f"{indent}📁 FOLDER: '{folder_name}' (id={folder_id})")

    # List everything inside — folders AND files
    try:
        result = drive.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="files(id, name, mimeType, size, modifiedTime, owners)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageSize=100
        ).execute()
    except Exception as e:
        log.error(f"{indent}  ❌ Cannot list this folder: {e}")
        return

    items = result.get("files", [])

    if not items:
        log.info(f"{indent}  ⚠️  Folder is EMPTY — no files or subfolders found")
        return

    folders = [i for i in items if i["mimeType"] == "application/vnd.google-apps.folder"]
    files   = [i for i in items if i["mimeType"] != "application/vnd.google-apps.folder"]

    log.info(f"{indent}  Found: {len(folders)} subfolder(s), {len(files)} file(s)")

    # List subfolders
    for f in folders:
        log.info(f"{indent}  📂 Subfolder: '{f['name']}' (id={f['id']})")

    # List files
    for f in files:
        size_kb = int(f.get("size", 0)) // 1024
        modified = f.get("modifiedTime", "unknown")[:10]
        log.info(f"{indent}  📄 File: '{f['name']}' | type={f['mimeType']} | size={size_kb}KB | modified={modified}")

    # Recurse into subfolders (one level deep)
    if depth == 0:
        for folder in folders:
            check_folder(drive, folder["id"], folder["name"], depth=1)


def run():
    log.info("=" * 60)
    log.info("Nagel Law — Drive Diagnostic Starting")
    log.info("=" * 60)

    if not DRIVE_ROOT_ID:
        log.error("DRIVE_ROOT_FOLDER_ID is not set in secrets!")
        return

    log.info(f"Root folder ID: {DRIVE_ROOT_ID}")

    drive = get_drive_service()

    # Step 1: Can we access the root folder at all?
    log.info("")
    log.info("STEP 1 — Checking root folder access...")
    try:
        root_info = drive.files().get(
            fileId=DRIVE_ROOT_ID,
            fields="id, name, mimeType, capabilities",
            supportsAllDrives=True
        ).execute()
        log.info(f"  ✅ Root folder accessible: '{root_info['name']}'")
        caps = root_info.get("capabilities", {})
        can_edit = caps.get("canEdit", False)
        can_add  = caps.get("canAddChildren", False)
        log.info(f"  Can edit: {can_edit} | Can add files: {can_add}")
        if not can_edit:
            log.warning("  ⚠️  Service account does NOT have Editor access to root folder!")
            log.warning("  → Go to Drive → right-click Intake folder → Share → add service account as Editor")
    except Exception as e:
        log.error(f"  ❌ Cannot access root folder: {e}")
        log.error("  → Check that DRIVE_ROOT_FOLDER_ID is correct")
        log.error("  → Check that the folder is shared with the service account")
        return

    # Step 2: List everything in root folder and subfolders
    log.info("")
    log.info("STEP 2 — Scanning folder contents...")
    check_folder(drive, DRIVE_ROOT_ID, root_info["name"])

    # Step 3: Check for Excel file
    log.info("")
    log.info("STEP 3 — Looking for Firm_Expense_Tracker.xlsx...")
    result = drive.files().list(
        q=f"'{DRIVE_ROOT_ID}' in parents and name='Firm_Expense_Tracker.xlsx' and trashed=false",
        fields="files(id, name, modifiedTime)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True
    ).execute()
    excel_files = result.get("files", [])
    if excel_files:
        log.info(f"  ✅ Found Excel: '{excel_files[0]['name']}' (id={excel_files[0]['id']})")
    else:
        log.warning("  ⚠️  Excel file NOT found in root intake folder")
        log.warning("  → Make sure Firm_Expense_Tracker.xlsx is directly inside the Intake folder")

    log.info("")
    log.info("=" * 60)
    log.info("Diagnostic complete — check results above")
    log.info("=" * 60)


if __name__ == "__main__":
    run()
