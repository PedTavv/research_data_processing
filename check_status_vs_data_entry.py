# -*- coding: utf-8 -*-


import pandas as pd
import sys
import os  # For basename in error messages

# --- Configuration ---
# !!! IMPORTANT: Update these constants below to match your specific data !!!

# Input File Path
INPUT_FILENAME = "participant_event_data.csv"  # Replace with your data file path

# Define Column Names used in your data file
ID_COL = "record_id"  # Column containing the unique participant identifier
EVENT_NAME_COL = "event_name"  # Column containing the event/visit name
# The date column to check for missing values across timepoints
DATE_COL_TO_CHECK = "test_date"
# Column indicating participant status
PARTICIPANT_STATUS_COL = "participant_status"

# Define the list of timepoint event names (unique event identifiers in your data)
# where the DATE_COL_TO_CHECK should be assessed for missing values.
# Replace these placeholders with the actual event names from your study.
# This list defines the scope of the check (e.g., all scheduled visits).
EVENTS_TO_CHECK_FOR_MISSING_DATE = [
    "study_baseline",  # Example: Baseline event name
    "followup_visit_1",  # Example: Represents the event expected around month 2
    "followup_visit_2",  # Example: Represents the event expected around month 4
    "followup_visit_3",  # Example: Represents the event expected around month 6
    "followup_visit_4",  # Example: Represents the event expected around month 8
    "followup_visit_5",  # Example: Represents the event expected around month 10
    "followup_visit_6",  # Example: Represents the event expected around month 12
    "followup_visit_7",  # Example: Represents the event expected around month 14
    "followup_visit_8",  # Example: Represents the event expected around month 16
    "end_of_study_visit",  # Example: Represents the final event around month 18
]

# Define the participant statuses that should ideally have *some* date entries
# if they participated. Participants found with these statuses but absolutely NO
# date entries in DATE_COL_TO_CHECK across ALL EVENTS_TO_CHECK_FOR_MISSING_DATE
# will be flagged by this script.
# Replace these example statuses with the actual numeric codes used in your study.
STATUSES_TO_CHECK = [
    1,
    2,
    4,
]  # Example: Statuses for 'Active', 'Completed - Outcome', 'Withdrawn'

# Define the status code that these participants might be candidates for changing to.
# This is just for context in the output message.
# Example: A status code for 'Withdrawn - No Data' or similar
POTENTIAL_TARGET_STATUS = 5


# --- Main Script Logic ---
try:
    print("--- Identifying Participants with Status Mismatches vs Data Entry ---")
    print("Configuration:")
    print(f"  Input File          : '{INPUT_FILENAME}'")
    print(f"  Participant ID Col  : '{ID_COL}'")
    print(f"  Event Name Col      : '{EVENT_NAME_COL}'")
    print(f"  Date Col Checked    : '{DATE_COL_TO_CHECK}'")
    print(f"  Status Col          : '{PARTICIPANT_STATUS_COL}'")
    print(f"  Events Checked      : {len(EVENTS_TO_CHECK_FOR_MISSING_DATE)} events")
    print(f"  Statuses Checked    : {STATUSES_TO_CHECK}")
    print(f"  Potential Target Status: {POTENTIAL_TARGET_STATUS} (for context)")

    # --- Load Data ---
    print(f"\nLoading data from '{os.path.basename(INPUT_FILENAME)}'...")
    try:
        # Using low_memory=False might help with mixed types but consumes more memory
        df = pd.read_csv(INPUT_FILENAME, low_memory=False)
    except UnicodeDecodeError:
        print("  - UnicodeDecodeError reading file, trying latin-1...")
        df = pd.read_csv(INPUT_FILENAME, low_memory=False, encoding="latin-1")
    print(f"  - Successfully loaded {df.shape[0]} rows.")

    # --- Prepare Data ---
    print("Preparing data...")
    # Clean column names (remove BOM, strip whitespace)
    df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()

    # Check required columns exist
    required_cols = [ID_COL, EVENT_NAME_COL, DATE_COL_TO_CHECK, PARTICIPANT_STATUS_COL]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"\nError: Missing required columns: {missing_cols}.")
        print("Please ensure column names in Configuration match your file.")
        sys.exit(1)

    # Convert date column to datetime, coercing errors to NaT (Not a Time)
    df[DATE_COL_TO_CHECK] = pd.to_datetime(df[DATE_COL_TO_CHECK], errors="coerce")
    print(f"  - Prepared column '{DATE_COL_TO_CHECK}' as datetime.")

    # Ensure ID and Event columns are strings and cleaned
    df[ID_COL] = df[ID_COL].astype(str).str.strip()
    df[EVENT_NAME_COL] = df[EVENT_NAME_COL].astype(str).str.strip()
    # Fill NaN IDs that might result from casting to empty strings
    df[ID_COL] = df[ID_COL].replace("nan", "")
    # Remove fully blank rows potentially introduced
    df = df[df[ID_COL] != ""].copy()
    if df.empty:
        print("Error: No valid data rows remain after cleaning ID column.")
        sys.exit(1)
    print(f"  - Cleaned '{ID_COL}' and '{EVENT_NAME_COL}' columns.")

    # --- Determine Participant Status (Using First Row Strategy) ---
    # This assumes the first row accurately reflects the status for this check.
    print("Determining participant status (using first row per participant)...")
    df_first_rows = df.drop_duplicates(subset=[ID_COL], keep="first").copy()

    # Convert status column to numeric, coercing errors
    df_first_rows[PARTICIPANT_STATUS_COL] = pd.to_numeric(
        df_first_rows[PARTICIPANT_STATUS_COL], errors="coerce"
    )

    # Create a mapping from record_id to participant_status
    record_id_status_map = df_first_rows.set_index(ID_COL)[
        PARTICIPANT_STATUS_COL
    ].dropna()
    print(f"  - Determined status for {len(record_id_status_map)} unique Record IDs.")

    # --- Identify Candidates for Status Review ---
    # Identify record_ids whose status is in the STATUSES_TO_CHECK list
    record_ids_to_check = record_id_status_map[
        record_id_status_map.isin(STATUSES_TO_CHECK)
    ].index.tolist()
    msg = f"\nIdentified {len(record_ids_to_check)} Record IDs with status in {STATUSES_TO_CHECK}."
    print(msg)
    print(f"Checking these Record IDs for missing '{DATE_COL_TO_CHECK}' values...")

    # Filter DataFrame to include only rows for events being checked
    df_events_to_check = df[
        df[EVENT_NAME_COL].isin(EVENTS_TO_CHECK_FOR_MISSING_DATE)
    ].copy()

    # Dictionary to store candidates, keyed by original status
    potential_candidates_by_status = {status: [] for status in STATUSES_TO_CHECK}

    # Iterate through the record_ids to check
    checked_count = 0
    candidates_found_count = 0
    for record_id in record_ids_to_check:
        checked_count += 1
        # Filter the event DataFrame for the current record_id
        df_record_events = df_events_to_check[df_events_to_check[ID_COL] == record_id]

        # Check if ALL entries in date column are missing for this record_id
        if (
            df_record_events.empty
            or df_record_events[DATE_COL_TO_CHECK].notna().sum() == 0
        ):
            status = record_id_status_map.get(record_id)
            # Ensure status is valid and is one we are checking before adding
            if pd.notna(status) and status in potential_candidates_by_status:
                potential_candidates_by_status[status].append(record_id)
                candidates_found_count += 1

    msg = f"  - Checked {checked_count} participants. Found {candidates_found_count} candidates."
    print(msg)

    # --- Report Results ---
    print("\n--- Results: Potential Candidates for Status Review ---")
    print(
        "(Participants listed have Status in {STATUSES_TO_CHECK} but NO dates for ANY event)"
    )

    total_potential_candidates = 0
    any_found = False
    # Ensure statuses are sorted numerically for consistent report order
    for status_code in sorted(STATUSES_TO_CHECK):
        # Check if the status code exists as a key
        if status_code in potential_candidates_by_status:
            candidates = sorted(potential_candidates_by_status[status_code])
            count = len(candidates)
            total_potential_candidates += count
            if candidates:
                any_found = True
                msg = (
                    f"\nRecord IDs with Status {int(status_code)} "
                    f"and EMPTY dates for ALL events ({count} found):"
                )
                print(msg)
                # Print in a slightly more readable list format if many IDs
                if count < 20:
                    print(f"  {candidates}")
                else:
                    # Print comma-separated for longer lists
                    print("  " + ", ".join(map(str, candidates)))
            else:
                msg = (
                    f"\nNo Record IDs found with Status {int(status_code)} "
                    f"that have EMPTY dates for ALL events."
                )
                print(msg)
        else:
            msg = f"\nStatus code {status_code} not found among participants (unexpected)."
            print(msg)

    print("-" * 87)
    if any_found:
        msg = (
            f"TOTAL potential candidates found across statuses {STATUSES_TO_CHECK}: "
            f"{total_potential_candidates}"
        )
        print(msg)
        msg = (
            f"These participants might warrant status review, potentially changing to "
            f"Status {POTENTIAL_TARGET_STATUS}."
        )
        print(msg)
    else:
        msg = (
            f"✅ No participants found matching criteria "
            f"(Status {STATUSES_TO_CHECK} and no dates)"
        )
        print(msg)
    print("-" * 87)

    print("\n--- Script Finished ---")

except FileNotFoundError:
    print(f"\nError: The file '{INPUT_FILENAME}' was not found.")
    print("Please ensure the INPUT_FILENAME constant is set correctly.")
except KeyError as e:
    print("\nError: A required column defined in the configuration was not found.")
    print(f"Missing column: {e}")
    print("Please ensure column name constants match your file.")
except Exception as e:
    print(f"\nAn unexpected error occurred: {e}")
    # Print traceback for debugging unexpected errors
    import traceback

    traceback.print_exc()
