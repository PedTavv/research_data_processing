# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from collections import Counter # Used for counting duplicates efficiently
import sys
from dateutil.relativedelta import relativedelta # Needed for baseline calc mirroring

# --- Configuration ---
# !!! IMPORTANT: Update these constants to match your specific data file, column names, and event names !!!
INPUT_FILE = "participant_event_data.csv" # Replace with your data file path

# --- Column Names ---
# !! Update these variable values if your column names are different !!
ID_COL = "record_id"                  # Column containing the unique participant identifier
EVENT_NAME_COL = "event_name"           # Column containing the event/visit name
TEST_DATE_COL = "test_date"             # Column with the actual test date
RESULT_COL = "result"                   # Column with the test result
VISIT_DATE_COL = "visit_date"           # Column for the general visit date (used for baseline calculation)
PARTICIPANT_STATUS_COL = "participant_status" # Needed for baseline calculation mirroring (if applicable)
ENDPOINT_DATE_1_COL = "primary_endpoint_date"   # Needed for baseline calc mirroring (if applicable, e.g., primary endpoint date)

# --- Expected Event Structure ---
# !! Update BASELINE_EVENT_NAME and EXPECTED_EVENT_LIST to match your study protocol !!
BASELINE_EVENT_NAME = 'study_baseline' # Define the specific baseline event name
# Define the full list of events expected for each participant IN ORDER
EXPECTED_EVENT_LIST = [
    BASELINE_EVENT_NAME, # Ensure baseline is included if it's an expected row
    'followup_visit_1',
    'followup_visit_2',
    'followup_visit_3',
    'followup_visit_4',
    'followup_visit_5',
    'followup_visit_6',
    'followup_visit_7',
    'followup_visit_8',
    'end_of_study_visit'
]
# Convert to a set for efficient checking of presence/absence
EXPECTED_EVENT_SET = set(EXPECTED_EVENT_LIST)
EXPECTED_COUNT = len(EXPECTED_EVENT_LIST) # Expected number of rows per participant matching this list
# Create a mapping for checking date order based on the list index
EVENT_ORDER_MAP = {event: i for i, event in enumerate(EXPECTED_EVENT_LIST)}

print(f"--- Checking Event Structure & Data Consistency in: {INPUT_FILE} ---")
print(f"Expecting {EXPECTED_COUNT} specific events per participant based on EXPECTED_EVENT_LIST:")
# Print first few expected events for clarity
for i, event in enumerate(EXPECTED_EVENT_LIST):
    if i < 3 or i == len(EXPECTED_EVENT_LIST) - 1:
        print(f"  - {event}")
    elif i == 3:
        print("  - ...")
print(f"Checks include: Missing/Duplicate/Unexpected Events, Date/Result presence, Date Order, Tests Before Baseline.")

# --- Load Data ---
print(f"\nLoading data from {INPUT_FILE}...")
try:
    # Try reading with different encodings
    try:
        df = pd.read_csv(INPUT_FILE, low_memory=False)
    except UnicodeDecodeError:
        print(f" - UnicodeDecodeError with default encoding, trying latin-1...")
        df = pd.read_csv(INPUT_FILE, encoding='latin-1', low_memory=False)
    print(f"Loaded {INPUT_FILE}. Shape: {df.shape}")
except FileNotFoundError:
    print(f"Error: File not found: {INPUT_FILE}")
    sys.exit(1)
except Exception as e:
    print(f"Error loading file: {e}")
    sys.exit(1)

# --- Prepare Data ---
print("Preparing data for checks...")
# Clean column names
df.columns = df.columns.str.replace('\ufeff', '', regex=False).str.strip()

# Check required columns exist after cleaning
required_cols = [ID_COL, EVENT_NAME_COL, TEST_DATE_COL, RESULT_COL, VISIT_DATE_COL, PARTICIPANT_STATUS_COL, ENDPOINT_DATE_1_COL]
missing_cols = [col for col in required_cols if col not in df.columns]
if missing_cols:
     print(f"Error: Missing required columns needed for checks: {missing_cols}")
     sys.exit(1)

# Convert columns to appropriate types, fill NaNs
df[ID_COL] = df[ID_COL].fillna('').astype(str).str.strip()
df[EVENT_NAME_COL] = df[EVENT_NAME_COL].fillna('MISSING_EVENT_NAME').astype(str).str.strip()
df[TEST_DATE_COL] = pd.to_datetime(df[TEST_DATE_COL], errors='coerce')
df[VISIT_DATE_COL] = pd.to_datetime(df[VISIT_DATE_COL], errors='coerce') # Needed for baseline calc
df[ENDPOINT_DATE_1_COL] = pd.to_datetime(df[ENDPOINT_DATE_1_COL], errors='coerce') # Needed for baseline filtering logic mirror (if used)
df[PARTICIPANT_STATUS_COL] = pd.to_numeric(df[PARTICIPANT_STATUS_COL], errors='coerce').astype('Int64') # Needed for baseline filtering logic mirror (if used)

# Prepare result column
df[RESULT_COL] = df[RESULT_COL].fillna('').astype(str).str.strip()
MISSING_RESULT_VALUES = ['', 'nan'] # Define what constitutes a missing result based on your data export

# Filter out rows that might represent intentional blank separators (where ID is empty)
df_data_rows = df[df[ID_COL] != ''].copy()

if df_data_rows.empty:
    print("Error: No data rows with valid Record IDs found after initial filtering.")
    sys.exit(1)
print(f" - Data prepared. {len(df_data_rows)} rows with valid IDs to check.")

# --- Calculate Effective Baseline (Mirroring Calculation Script Logic) ---
# This step is crucial for the "Test Date Before Baseline" check.
# Assumes the logic uses TEST_DATE_COL and VISIT_DATE_COL from the BASELINE_EVENT_NAME row.
print("Calculating effective baseline dates for participants...")
baseline_event_rows = df_data_rows.loc[df_data_rows[EVENT_NAME_COL] == BASELINE_EVENT_NAME].copy()
baseline_event_rows = baseline_event_rows.sort_values([ID_COL]).drop_duplicates(ID_COL, keep='first')

# Determine effective baseline date: prioritize TEST_DATE_COL, fallback to VISIT_DATE_COL
baseline_event_rows['effective_baseline'] = pd.NaT # Initialize with NaT (Not a Time)
if TEST_DATE_COL in baseline_event_rows.columns and VISIT_DATE_COL in baseline_event_rows.columns:
    baseline_event_rows['effective_baseline'] = baseline_event_rows[TEST_DATE_COL].fillna(baseline_event_rows[VISIT_DATE_COL])
elif TEST_DATE_COL in baseline_event_rows.columns:
     baseline_event_rows['effective_baseline'] = baseline_event_rows[TEST_DATE_COL]
elif VISIT_DATE_COL in baseline_event_rows.columns:
     baseline_event_rows['effective_baseline'] = baseline_event_rows[VISIT_DATE_COL]
# Ensure the result is datetime
baseline_event_rows["effective_baseline"] = pd.to_datetime(baseline_event_rows["effective_baseline"], errors='coerce')

# Keep only the ID and the calculated effective baseline
base_dates = baseline_event_rows[[ID_COL, "effective_baseline"]].copy()

# Merge effective baseline date back onto the main data rows
# Use left merge to keep all data rows, even if a baseline row was missing (baseline date will be NaT)
df_data_rows = pd.merge(df_data_rows, base_dates, on=ID_COL, how='left')
missing_baseline_count = df_data_rows['effective_baseline'].isna().sum()
if missing_baseline_count > 0:
     print(f" - Warning: Effective baseline date could not be determined for {missing_baseline_count} rows (likely missing baseline event row or dates). 'Test Before Baseline' check might be affected.")
print(f" - Effective baseline dates calculated and merged.")


# --- Group by Participant and Perform Checks ---
print("Checking structure and consistency per participant...")
discrepancies = [] # List to store details of participants with issues

# Group by record ID
grouped = df_data_rows.groupby(ID_COL)
total_participants_checked = len(grouped)
participants_with_issues = 0

for participant_id, group in grouped:
    participant_issues = {} # Store issues for this participant

    # Get the effective baseline for this participant (should be unique per participant)
    effective_baseline_date = group['effective_baseline'].dropna().unique()
    # Handle cases where baseline might be missing or duplicated (though drop_duplicates should prevent latter)
    effective_baseline_date = effective_baseline_date[0] if len(effective_baseline_date) > 0 else pd.NaT

    # --- Structural Checks (Presence, Duplicates, Unexpected) ---
    actual_events_in_group = group[EVENT_NAME_COL].tolist()
    actual_event_set_in_group = set(actual_events_in_group)
    event_counts_in_group = Counter(actual_events_in_group)

    # Check 1: Missing Events from EXPECTED_EVENT_SET
    missing_events = list(EXPECTED_EVENT_SET - actual_event_set_in_group)
    if missing_events:
        participant_issues["missing_expected_events"] = sorted(missing_events)

    # Check 2: Duplicate Events from EXPECTED_EVENT_SET
    duplicate_events = [event for event in EXPECTED_EVENT_SET if event_counts_in_group[event] > 1]
    if duplicate_events:
        participant_issues["duplicate_expected_events"] = sorted([f"{event} (Count: {event_counts_in_group[event]})" for event in duplicate_events])

    # Check 3: Unexpected Events found (not in EXPECTED_EVENT_SET)
    unexpected_events = list(actual_event_set_in_group - EXPECTED_EVENT_SET)
    if unexpected_events:
        # Filter out the generic 'MISSING_EVENT_NAME' if it was filled earlier
        unexpected_events_filtered = [e for e in unexpected_events if e != 'MISSING_EVENT_NAME']
        if unexpected_events_filtered:
            participant_issues["unexpected_events_found"] = sorted(unexpected_events_filtered)

    # Check 4: Incorrect total number of rows matching expected events
    num_expected_event_rows_found = group[EVENT_NAME_COL].isin(EXPECTED_EVENT_SET).sum()
    if num_expected_event_rows_found != EXPECTED_COUNT:
        participant_issues["incorrect_expected_event_row_count"] = f"Found {num_expected_event_rows_found}, Expected {EXPECTED_COUNT}"


    # --- Data Consistency Checks ---
    # Check A: Date entered without a Result
    date_no_result_rows = group[ group[TEST_DATE_COL].notna() & group[RESULT_COL].isin(MISSING_RESULT_VALUES) ]
    if not date_no_result_rows.empty:
        participant_issues["date_without_result"] = sorted(date_no_result_rows[EVENT_NAME_COL].tolist())

    # Check B: Result entered without a Date
    result_no_date_rows = group[ group[TEST_DATE_COL].isna() & (~group[RESULT_COL].isin(MISSING_RESULT_VALUES)) ]
    if not result_no_date_rows.empty:
        participant_issues["result_without_date"] = sorted(result_no_date_rows[EVENT_NAME_COL].tolist())

    # Check C: Date Order Check (based on EXPECTED_EVENT_LIST order)
    dates_for_order_check = group[ group[EVENT_NAME_COL].isin(EXPECTED_EVENT_SET) & group[TEST_DATE_COL].notna() ].copy()
    if len(dates_for_order_check) > 1:
        dates_for_order_check['event_order'] = dates_for_order_check[EVENT_NAME_COL].map(EVENT_ORDER_MAP)
        # Handle cases where an event might not be in the map (e.g., unexpected event sneaked in - though check 3 should catch it)
        dates_for_order_check.dropna(subset=['event_order'], inplace=True)
        dates_for_order_check = dates_for_order_check.sort_values('event_order')

        if not dates_for_order_check[TEST_DATE_COL].is_monotonic_increasing:
            out_of_order_details = []
            dates_series = dates_for_order_check[TEST_DATE_COL]
            event_series = dates_for_order_check[EVENT_NAME_COL]
            for i in range(len(dates_series) - 1):
                 # Check specifically for decrease (later event date < earlier event date)
                 if pd.notna(dates_series.iloc[i]) and pd.notna(dates_series.iloc[i+1]) and dates_series.iloc[i] > dates_series.iloc[i+1]:
                     out_of_order_details.append(
                         f"'{event_series.iloc[i+1]}' ({dates_series.iloc[i+1].date()}) "
                         f"before '{event_series.iloc[i]}' ({dates_series.iloc[i].date()})"
                     )
            if out_of_order_details: # Only add if specific decreases found
                 participant_issues["date_order_violation"] = "; ".join(out_of_order_details)

    # Check D: Test Date Before Baseline Date
    if pd.notna(effective_baseline_date): # Only perform check if baseline date is valid
        # Select non-baseline events that have a test date
        follow_ups_with_date = group[
            (group[EVENT_NAME_COL] != BASELINE_EVENT_NAME) &
            (group[TEST_DATE_COL].notna())
        ].copy()
        if not follow_ups_with_date.empty:
            # Find rows where the test date is strictly before the baseline date
            tests_before_baseline = follow_ups_with_date[follow_ups_with_date[TEST_DATE_COL] < effective_baseline_date]
            if not tests_before_baseline.empty:
                 participant_issues["test_date_before_baseline"] = sorted([
                     f"{row[EVENT_NAME_COL]} ({row[TEST_DATE_COL].date()})" for index, row in tests_before_baseline.iterrows()
                 ])
    # elif participant_id not in [p['record_id'] for p in discrepancies if 'missing_expected_events' in p and BASELINE_EVENT_NAME in p['missing_expected_events']]:
    #      # Optionally flag participants missing a baseline date if not already caught by missing baseline event
    #      participant_issues["missing_baseline_date_info"] = "Effective baseline date could not be determined"


    # If any issues were found for this participant, add them to the main list
    if participant_issues:
        participant_issues["record_id"] = participant_id # Ensure ID is the first key
        discrepancies.append(participant_issues)
        participants_with_issues += 1

# --- Report Results ---
print(f"\n--- Structure & Consistency Validation Summary ---")
print(f"Total unique participants checked: {total_participants_checked}")
print(f"Number of participants with issues found: {participants_with_issues}")

if discrepancies:
    print("\nDetails of Issues Found:")
    # Convert list of dicts to DataFrame for potentially better structured output if needed
    discrepancy_df = pd.DataFrame(discrepancies)
    # Reorder columns for better readability: ID first, then common issues
    cols_order = ["record_id", "incorrect_expected_event_row_count", "missing_expected_events",
                  "duplicate_expected_events", "unexpected_events_found", "date_without_result",
                  "result_without_date", "date_order_violation", "test_date_before_baseline"]
    # Ensure columns exist before trying to order/display them
    report_cols_exist = [col for col in cols_order if col in discrepancy_df.columns]
    # Add any other columns that might exist but weren't in the preferred order list
    remaining_cols = [col for col in discrepancy_df.columns if col not in report_cols_exist]
    discrepancy_df = discrepancy_df[report_cols_exist + remaining_cols] # Combine ordered and remaining

    # Print using a method that handles potentially long list/string entries well
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 2000) # Set large width
    pd.set_option('display.max_colwidth', 100) # Limit column width to prevent extreme wrapping
    print(discrepancy_df.to_string(index=False, na_rep='-')) # Use '-' for missing details
    pd.reset_option('all') # Reset display options to default

    # Optional: Save to CSV
    try:
        output_filename = "data_structure_consistency_issues.csv"
        discrepancy_df.to_csv(output_filename, index=False, encoding='utf-8')
        print(f"\nIssue details saved to: {output_filename}")
    except Exception as e:
        print(f"\nError saving issue details to CSV: {e}", file=sys.stderr)

else:
    print("\n✅ No event structure or data consistency issues found based on the defined checks.")

print("\n--- Script Finished ---")