# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from datetime import timedelta
from dateutil.relativedelta import (
    relativedelta,
)  # Import relativedelta for month offsets
import sys

# --- Configuration ---
# !!! IMPORTANT: Update these constants to match your specific data file and column names !!!
# Input File: Replace with the path to your data file
INPUT_FILE = "participant_data.csv"
# Output File: Specify the desired output file name (can be the same as input to overwrite)
OUTPUT_FILE = "participant_data_processed.csv"  # Changed to avoid accidental overwrite

# Calculation Parameters
GRACE_DAYS = 14  # Grace period in days (+/-) for late/early tests

# --- Column Names ---
# !! Update these variable values if your column names are different !!
PARTICIPANT_ID_COL = "record_id"  # Column containing the unique participant identifier
EVENT_NAME_COL = "event_name"  # Column containing the event/visit name
VISIT_DATE_COL = (
    "visit_date"  # Column for the general visit date (used as baseline proxy)
)
TEST_DATE_COL = (
    "test_date"  # Column for the specific date the test/assessment was performed
)
STATUS_COL = "participant_status"  # Column indicating participant status (e.g., active, withdrawn)
ENDPOINT_DATE_1_COL = (
    "primary_endpoint_date"  # e.g., Date of event , or end-of-study date
)
ENDPOINT_DATE_2_COL = (
    "secondary_endpoint_date"  # e.g., Date of withdrawal or loss to follow-up
)
# Output columns (these will be created or overwritten)
MISSED_TEST_COUNT_COL = (
    "missed_test_count"  # Count of expected tests with no date recorded
)
OUTSIDE_WINDOW_COUNT_COL = (
    "outside_window_test_count"  # Count of tests outside grace period
)
TOTAL_DEVIATION_COUNT_COL = "total_test_deviations"  # Sum of the above two counts

# --- Event Names ---
# !! Update these event names to match those used in your 'EVENT_NAME_COL' !!
BASELINE_EVENT_NAME = (
    "study_baseline"  # The specific name for the baseline/screening event
)
# Define the scheduled follow-up events and their expected month offset from the baseline event
# Format: {month_offset_from_baseline: 'event_name_in_data'}
EXPECTED_FOLLOWUP_SCHEDULE = {
    2: "followup_visit_1",
    4: "followup_visit_2",
    6: "followup_visit_3",
    8: "followup_visit_4",
    10: "followup_visit_5",
    12: "followup_visit_6",
    14: "followup_visit_7",
    16: "followup_visit_8",
    18: "end_of_study_visit",
}

# List of all event names associated with tests/assessments being tracked
ALL_TEST_EVENT_NAMES = [BASELINE_EVENT_NAME] + list(EXPECTED_FOLLOWUP_SCHEDULE.values())

# --- Script Start ---
print("--- Calculating Test Schedule Deviations ---")
print(f"Input File: {INPUT_FILE}")
print(f"Output File: {OUTPUT_FILE}")
print(
    f"Output Columns: '{MISSED_TEST_COUNT_COL}', '{OUTSIDE_WINDOW_COUNT_COL}', "
    f"'{TOTAL_DEVIATION_COUNT_COL}'"
)
print(f"Grace Period: +/- {GRACE_DAYS} days (for '{OUTSIDE_WINDOW_COUNT_COL}')")
print(f"Baseline Event Name: {BASELINE_EVENT_NAME}")
print(
    "Baseline Anchor Date: Prioritizing '{TEST_DATE_COL}', "
    "fallback to '{VISIT_DATE_COL}' from baseline event."
)
print(
    "Status Handling: Assumes specific logic based on values in '{STATUS_COL}' "
    "(e.g., Status 1 expects full schedule, Status 2 uses '{ENDPOINT_DATE_1_COL}', "
    "Status 3/4 use '{ENDPOINT_DATE_2_COL}'). Status 5 is excluded."
)
print("Note: Intentionally blank rows in the input file will be preserved.")
print("Configuration:")
print(
    "!!! IMPORTANT: Review configuration constants (file paths, column/event names) "
    "before running. !!!"
)
if INPUT_FILE == OUTPUT_FILE:
    print("!!! WARNING: The input file will be overwritten. Proceed with caution. !!!")


# --- Load Data ---
print("\nLoading data...")
try:
    # Attempt to read with default UTF-8 first, then fallback encodings
    try:
        df = pd.read_csv(INPUT_FILE, low_memory=False)
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(INPUT_FILE, encoding="latin1", low_memory=False)
            print(" - Loaded using latin1 encoding.")
        except UnicodeDecodeError:
            df = pd.read_csv(INPUT_FILE, encoding="cp1252", low_memory=False)
            print(" - Loaded using cp1252 encoding.")

    print("Loaded data. Shape: {0}".format(df.shape))
except FileNotFoundError:
    print("Error: File not found")
    sys.exit(1)
except Exception as e:
    print("Error loading CSV file: {0}".format(e))
    sys.exit(1)

# --- Data Cleaning ---
# Clean column names (remove BOM and whitespace)
df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()
print(" - Cleaned column names (removed BOM and whitespace).")

# Clean participant ID column
if PARTICIPANT_ID_COL in df.columns:
    # Fill potential NaNs in ID column before stripping and converting to string
    df[PARTICIPANT_ID_COL] = df[PARTICIPANT_ID_COL].fillna("").astype(str).str.strip()
    print(
        f" - Cleaned '{PARTICIPANT_ID_COL}' column "
        "(removed leading/trailing whitespace from values)."
    )
else:
    print(
        f"Error: Participant ID column '{PARTICIPANT_ID_COL}' not found. "
        "Check configuration."
    )
    sys.exit(1)  # Exit if the core ID column is missing


# --- Store Initial Values for Reporting (Optional) ---
print("\nStoring initial count values (if columns exist)...")
initial_counts = pd.DataFrame(
    index=pd.Index([], name=PARTICIPANT_ID_COL)
)  # Empty DataFrame
count_cols_to_check = [
    MISSED_TEST_COUNT_COL,
    OUTSIDE_WINDOW_COUNT_COL,
    TOTAL_DEVIATION_COUNT_COL,
]
existing_count_cols = [col for col in count_cols_to_check if col in df.columns]

if existing_count_cols:
    initial_counts = df[[PARTICIPANT_ID_COL] + existing_count_cols].copy()
    # Set index for easier lookup later, drop rows where ID is empty after cleaning
    initial_counts = (
        initial_counts[initial_counts[PARTICIPANT_ID_COL] != ""]
        .drop_duplicates(subset=[PARTICIPANT_ID_COL], keep="first")
        .set_index(PARTICIPANT_ID_COL)
    )
    print(f" - Stored initial values from existing columns: {existing_count_cols}")
else:
    print(" - No existing count columns found to store initial values.")


# --- Prepare Output Columns ---
print("\nPreparing output columns (clearing or adding)...")
for col in [MISSED_TEST_COUNT_COL, OUTSIDE_WINDOW_COUNT_COL, TOTAL_DEVIATION_COUNT_COL]:
    if col not in df.columns:
        df[col] = pd.NA  # Add column if it doesn't exist
        print(f" - Added missing column: {col}")
    # Clear existing calculated columns using .loc to avoid SettingWithCopyWarning
    df.loc[:, col] = pd.NA  # Use pandas.NA for nullable integer/float assignment
    print(f" - Cleared existing values in '{col}'.")


# --- Data Preparation (Type Conversion) ---
print("\nPreparing data (standardizing types)...")
# Convert date columns to datetime objects, handling potential errors
date_cols_to_convert = [
    TEST_DATE_COL,
    VISIT_DATE_COL,
    ENDPOINT_DATE_1_COL,
    ENDPOINT_DATE_2_COL,
]
for col in date_cols_to_convert:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")
        print(f" - Converted '{col}' to datetime (errors coerced to NaT).")
    else:
        print(
            f"Warning: Date column '{col}' not found. Check configuration. "
            "Calculations might be affected if this column is required."
        )
        # Depending on which date column is missing, you might want to add sys.exit() here.

# Ensure participant status is numeric, coercing errors to NaN
if STATUS_COL in df.columns:
    df[STATUS_COL] = pd.to_numeric(df[STATUS_COL], errors="coerce").astype("Int64")
    print(f" - Converted '{STATUS_COL}' to nullable integer.")
else:
    print(
        f"Error: Status column '{STATUS_COL}' not found. Cannot filter by status. "
        "Check configuration."
    )
    sys.exit(1)  # Exit if status column is missing

# Ensure Event column is string for reliable filtering and lookup
if EVENT_NAME_COL in df.columns:
    df[EVENT_NAME_COL] = df[EVENT_NAME_COL].astype(str).fillna("MISSING_EVENT_NAME")
    print(f" - Converted '{EVENT_NAME_COL}' to string.")
else:
    print(
        f"Error: Event name column '{EVENT_NAME_COL}' not found. Cannot filter by "
        "event name. Check configuration."
    )
    sys.exit(1)  # Exit if event column is missing


# --- Calculate Baseline Anchor Dates ---
print("\nCalculating baseline anchor dates...")
# Get baseline event rows with non-empty IDs
baseline_event_rows = df.loc[
    (df[EVENT_NAME_COL] == BASELINE_EVENT_NAME) & (df[PARTICIPANT_ID_COL] != "")
].copy()

# Handle potential duplicate baseline events for a participant, keeping the first occurrence
baseline_event_rows = baseline_event_rows.sort_values(
    [PARTICIPANT_ID_COL]
).drop_duplicates(PARTICIPANT_ID_COL, keep="first")

# Determine effective baseline date: prioritize TEST_DATE_COL, fallback to VISIT_DATE_COL
baseline_event_rows["effective_baseline"] = pd.NaT  # Initialize with NaT (Not a Time)

if (
    TEST_DATE_COL in baseline_event_rows.columns
    and VISIT_DATE_COL in baseline_event_rows.columns
):
    baseline_event_rows["effective_baseline"] = baseline_event_rows[
        TEST_DATE_COL
    ].fillna(baseline_event_rows[VISIT_DATE_COL])
    print(
        f" - Using '{TEST_DATE_COL}' as baseline date, falling back to "
        f"'{VISIT_DATE_COL}'."
    )
elif TEST_DATE_COL in baseline_event_rows.columns:
    baseline_event_rows["effective_baseline"] = baseline_event_rows[TEST_DATE_COL]
    print(
        f" - Using '{TEST_DATE_COL}' as baseline date (Warning: '{VISIT_DATE_COL}' "
        "not found for fallback)."
    )
elif VISIT_DATE_COL in baseline_event_rows.columns:
    baseline_event_rows["effective_baseline"] = baseline_event_rows[VISIT_DATE_COL]
    print(
        f" - Using '{VISIT_DATE_COL}' as baseline date (Warning: '{TEST_DATE_COL}' "
        "not found)."
    )
else:
    print(
        f"Error: Neither '{TEST_DATE_COL}' nor '{VISIT_DATE_COL}' found in baseline "
        "rows. Cannot determine baseline date. Check configuration."
    )
    sys.exit(1)

# Keep only participants with a valid effective baseline date
final_baselines = baseline_event_rows.dropna(subset=["effective_baseline"])[
    [PARTICIPANT_ID_COL, "effective_baseline"]
].copy()
final_baselines[PARTICIPANT_ID_COL] = final_baselines[PARTICIPANT_ID_COL].astype(str)

print(
    f" - Determined final effective baseline anchor date for {len(final_baselines)} "
    "participants."
)

# Count how many used the proxy baseline date (if both date columns exist)
used_proxy_count = 0
if (
    TEST_DATE_COL in baseline_event_rows.columns
    and VISIT_DATE_COL in baseline_event_rows.columns
):
    used_proxy_count = baseline_event_rows[
        baseline_event_rows[TEST_DATE_COL].isna()
        & baseline_event_rows[VISIT_DATE_COL].notna()
    ].shape[0]
    print(
        f"   ({used_proxy_count} participants using proxy baseline: '{VISIT_DATE_COL}')"
    )


# --- Extract Participant Info (Status & Endpoint Dates) ---
# Pull status and endpoint dates ONLY from the baseline event row for consistency
print("\nExtracting participant status and endpoint dates from baseline event rows...")
endpoint_cols_to_extract = [ENDPOINT_DATE_1_COL, ENDPOINT_DATE_2_COL]
endpoint_cols_exist = [col for col in endpoint_cols_to_extract if col in df.columns]

# Select relevant columns from baseline rows
pinfo_raw = (
    df.loc[
        (df[PARTICIPANT_ID_COL] != "") & (df[EVENT_NAME_COL] == BASELINE_EVENT_NAME),
        [PARTICIPANT_ID_COL, STATUS_COL] + endpoint_cols_exist,
    ]
    .drop_duplicates(subset=[PARTICIPANT_ID_COL], keep="first")
    .copy()
)
pinfo_raw[PARTICIPANT_ID_COL] = pinfo_raw[PARTICIPANT_ID_COL].astype(str)

# Merge with effective baselines to get participants with status and baseline date
participants_for_recalc = pd.merge(
    pinfo_raw,
    final_baselines,
    on=PARTICIPANT_ID_COL,
    how="inner",  # Only keep participants with a baseline row, status, and baseline date
)

# Filter for relevant statuses (e.g., 1, 2, 3, 4 based on original logic)
# Adjust this list if your status codes are different
valid_statuses_for_calc = [1, 2, 3, 4]
participants_for_recalc = participants_for_recalc[
    participants_for_recalc[STATUS_COL].isin(valid_statuses_for_calc)
].copy()

print(
    f"\nProcessing {len(participants_for_recalc)} participants with relevant status "
    f"({valid_statuses_for_calc}) and effective baseline."
)


# --- Create Lookup for Performed Tests ---
# Filter for rows that represent a test event, have a valid test date, and a non-empty ID
performed_tests_df = df.loc[
    (df[EVENT_NAME_COL].isin(ALL_TEST_EVENT_NAMES))
    & (df[TEST_DATE_COL].notna())
    & (df[PARTICIPANT_ID_COL] != "")
].copy()

# Create a lookup dictionary: {(participant_id, event_name): test_date}
# Handle potential duplicate test entries for the same event, keeping the first recorded date
performed_tests_df = performed_tests_df.sort_values(
    [PARTICIPANT_ID_COL, EVENT_NAME_COL, TEST_DATE_COL]
).drop_duplicates(subset=[PARTICIPANT_ID_COL, EVENT_NAME_COL], keep="first")

performed_dict = {
    (str(row[PARTICIPANT_ID_COL]), row[EVENT_NAME_COL]): row[TEST_DATE_COL]
    for _, row in performed_tests_df.iterrows()
}

print(
    f"Created lookup dictionary for {performed_tests_df.shape[0]} performed test "
    "instances (with date)."
)


# --- Calculate Missed / Outside Window Tests ---
print(
    f"\nCalculating counts ('{MISSED_TEST_COUNT_COL}', '{OUTSIDE_WINDOW_COUNT_COL}')..."
)

calculated_counts = {}  # {participant_id: {'missed_count': count, 'outside_window_count': count}}

if not participants_for_recalc.empty:
    for _, participant_row in participants_for_recalc.iterrows():
        pid = participant_row[PARTICIPANT_ID_COL]
        base_dt = participant_row["effective_baseline"]
        status = participant_row[STATUS_COL]  # Already filtered for valid statuses

        # Ensure status is not NA before converting to int
        if pd.isna(status):
            continue
        status = int(status)

        # Get endpoint dates safely using .get() in case columns were missing
        endpoint1_dt = participant_row.get(ENDPOINT_DATE_1_COL, pd.NaT)
        endpoint2_dt = participant_row.get(ENDPOINT_DATE_2_COL, pd.NaT)

        recalc_missed = 0
        recalc_outside_window = 0

        # Determine the end date for expected tests based on participant status
        # !! Adjust this logic based on the meaning of your status codes and endpoint dates !!
        participant_end_point_date = pd.NaT
        if status == 1:
            # Assumes status 1 follows the full schedule (e.g., 18 months)
            max_month_offset = (
                max(EXPECTED_FOLLOWUP_SCHEDULE.keys())
                if EXPECTED_FOLLOWUP_SCHEDULE
                else 0
            )
            participant_end_point_date = base_dt + relativedelta(
                months=max_month_offset
            )
        elif status == 2 and pd.notna(endpoint1_dt):
            # Assumes status 2 stops at the primary endpoint date
            participant_end_point_date = endpoint1_dt
        elif status in [3, 4] and pd.notna(endpoint2_dt):
            # Assumes status 3 and 4 stop at the secondary endpoint date (e.g., withdrawal)
            participant_end_point_date = endpoint2_dt
        # Add more elif conditions here if you have other statuses with specific endpoint logic

        # If no valid end date could be determined for statuses needing one, skip recalculation
        if status in [2, 3, 4] and pd.isna(participant_end_point_date):
            continue
        # For status 1 (full schedule), ensure a baseline date exists
        elif status == 1 and pd.isna(base_dt):
            continue

        # --- Check Baseline Test (Month 0) ---
        baseline_scheduled_date = base_dt  # Scheduled date is the baseline date itself
        baseline_is_expected = False
        # Baseline is always expected for status 1, or if its date is before/on the endpoint
        if status == 1:
            baseline_is_expected = True
        elif pd.notna(participant_end_point_date):
            # Check if baseline date is on or before the endpoint (inclusive)
            baseline_is_expected = baseline_scheduled_date <= participant_end_point_date

        if baseline_is_expected:
            baseline_performed_date = performed_dict.get((pid, BASELINE_EVENT_NAME))
            if pd.isna(baseline_performed_date):
                recalc_missed += 1
            else:
                # Check if performed baseline is outside the grace window
                baseline_lower_bound = baseline_scheduled_date - timedelta(
                    days=GRACE_DAYS
                )
                baseline_upper_bound = baseline_scheduled_date + timedelta(
                    days=GRACE_DAYS
                )
                if (
                    pd.notna(baseline_performed_date)
                    and pd.notna(baseline_scheduled_date)
                    and not (
                        baseline_lower_bound
                        <= baseline_performed_date
                        <= baseline_upper_bound
                    )
                ):
                    recalc_outside_window += 1

        # --- Check Follow-up Tests ---
        for month_offset, event_name in EXPECTED_FOLLOWUP_SCHEDULE.items():
            try:
                scheduled_date = base_dt + relativedelta(months=month_offset)
            except Exception as e:
                print(
                    f"Warning: Could not calculate scheduled date for participant '{pid}' "
                    f"at month offset {month_offset}. Error: {e}. Skipping this event."
                )
                continue

            # Determine if this scheduled test is expected based on the participant's end date
            is_expected = False
            if pd.notna(participant_end_point_date):
                # Test is expected if its scheduled date is *before or exactly at* the endpoint
                is_expected = scheduled_date <= participant_end_point_date
            elif (
                status == 1
            ):  # If status 1 means full schedule regardless of endpoint date
                is_expected = True

            if is_expected:
                followup_key = (pid, event_name)
                performed_date = performed_dict.get(followup_key)

                if pd.isna(performed_date):
                    recalc_missed += 1  # Expected but not performed
                else:
                    # Check if performed test is outside the grace period window
                    lower_bound = scheduled_date - timedelta(days=GRACE_DAYS)
                    upper_bound = scheduled_date + timedelta(days=GRACE_DAYS)

                    if (
                        pd.notna(performed_date)
                        and pd.notna(scheduled_date)
                        and not (lower_bound <= performed_date <= upper_bound)
                    ):
                        recalc_outside_window += 1

        # Store calculated values
        calculated_counts[pid] = {
            MISSED_TEST_COUNT_COL: recalc_missed,
            OUTSIDE_WINDOW_COUNT_COL: recalc_outside_window,
            TOTAL_DEVIATION_COUNT_COL: recalc_missed + recalc_outside_window,
        }

print(f"Finished calculating counts for {len(calculated_counts)} participants.")

# --- Add/Update Columns to DataFrame ---
print(
    f"\nAdding/Updating columns '{MISSED_TEST_COUNT_COL}', '{OUTSIDE_WINDOW_COUNT_COL}', "
    f"'{TOTAL_DEVIATION_COUNT_COL}' on baseline row per participant..."
)

# Create a mapping from participant ID to the DataFrame index of their baseline row
baseline_index_map = (
    df.loc[(df[EVENT_NAME_COL] == BASELINE_EVENT_NAME) & (df[PARTICIPANT_ID_COL] != "")]
    .set_index(PARTICIPANT_ID_COL)
    .index
)

updated_participants_count = 0
# Iterate through the calculated counts
for participant_id, counts in calculated_counts.items():
    # Find the index of the baseline row for this participant
    # Use .loc for robust index lookup based on participant ID and baseline event name
    try:
        target_indices = df.loc[
            (df[PARTICIPANT_ID_COL] == participant_id)
            & (df[EVENT_NAME_COL] == BASELINE_EVENT_NAME)
        ].index
        if not target_indices.empty:
            idx = target_indices[0]  # Get the first matching index

            # Update the count columns on this specific baseline row using .loc
            df.loc[idx, MISSED_TEST_COUNT_COL] = counts[MISSED_TEST_COUNT_COL]
            df.loc[idx, OUTSIDE_WINDOW_COUNT_COL] = counts[OUTSIDE_WINDOW_COUNT_COL]
            df.loc[idx, TOTAL_DEVIATION_COUNT_COL] = counts[TOTAL_DEVIATION_COUNT_COL]
            updated_participants_count += 1

    except KeyError:
        pass


print(
    f" - Applied updates to the baseline row for {updated_participants_count} unique "
    "participants processed."
)


# --- Handle Excluded Status Participants (e.g., Status 5) ---
# Ensure counts are blank for participants whose status excludes them from calculation
excluded_status_code = 5  # Define the status code to exclude
print(
    f"\nEnsuring count columns are blank for participants with Status "
    f"{excluded_status_code} on their baseline row..."
)

# Find all unique participant IDs with the excluded status on their baseline row
excluded_pids = []
if STATUS_COL in df.columns:
    baseline_rows_df = df.loc[
        (df[EVENT_NAME_COL] == BASELINE_EVENT_NAME) & (df[PARTICIPANT_ID_COL] != "")
    ].copy()
    # Filter for the excluded status, handling potential NAs in status
    excluded_participants_df = baseline_rows_df[
        baseline_rows_df[STATUS_COL].fillna(-1) == excluded_status_code
    ]
    excluded_pids = excluded_participants_df[PARTICIPANT_ID_COL].unique().tolist()
else:
    print(
        f"Warning: Status column '{STATUS_COL}' not found, cannot blank counts for "
        "excluded status."
    )

print(
    f" - Found {len(excluded_pids)} unique participants with status "
    f"{excluded_status_code} on their baseline row."
)

# Set the count columns to blank (NA) for all rows belonging to these excluded participants
if excluded_pids:
    # Use .loc for safe assignment across all rows for these participants
    count_cols_to_blank = [
        MISSED_TEST_COUNT_COL,
        OUTSIDE_WINDOW_COUNT_COL,
        TOTAL_DEVIATION_COUNT_COL,
    ]
    df.loc[df[PARTICIPANT_ID_COL].isin(excluded_pids), count_cols_to_blank] = pd.NA
    print(
        f" - Set count columns {count_cols_to_blank} to blank (NA) for all rows of "
        f"status {excluded_status_code} participants."
    )
else:
    print(
        f" - No participants with status {excluded_status_code} found to blank out "
        "count columns."
    )


# --- Report Changes (Optional Comparison) ---
print(
    "\nComparing initial values with final calculated values (if initial values "
    "existed)..."
)

if not initial_counts.empty:
    # Get final counts from the baseline row only for comparison
    final_counts = (
        df.loc[df[EVENT_NAME_COL] == BASELINE_EVENT_NAME]
        .set_index(PARTICIPANT_ID_COL)[existing_count_cols]
        .copy()
    )

    # Ensure index types match for joining
    initial_counts.index = initial_counts.index.astype(str)
    final_counts.index = final_counts.index.astype(str)

    # Merge initial and final counts
    comparison_df = initial_counts.join(
        final_counts, lsuffix="_initial", rsuffix="_final", how="outer"
    )

    # Identify rows where any of the count columns have changed value (handling NA/NaN)
    changed_rows_mask = pd.Series(False, index=comparison_df.index)
    for col_base in existing_count_cols:
        col_initial = f"{col_base}_initial"
        col_final = f"{col_base}_final"
        # Compare string representations after filling NA/NaN to handle type differences
        changed_rows_mask |= comparison_df[col_initial].fillna("NA").astype(
            str
        ) != comparison_df[col_final].fillna("NA").astype(str)

    changed_participants_df = comparison_df[changed_rows_mask].copy()

    # Replace NA/NaN with 'BLANK' for display purposes
    changed_participants_df = changed_participants_df.astype(object).replace(
        {pd.NA: "BLANK", np.nan: "BLANK"}
    )

    print("\n--- Summary of Changes Made by Script ---")
    # Filter comparison to only include participants that were actually processed
    processed_pids = set(calculated_counts.keys())
    changed_processed_participants_df = changed_participants_df[
        changed_participants_df.index.isin(processed_pids)
    ]

    print(
        f"Number of participants processed (relevant status) with count values changed: "
        f"{changed_processed_participants_df.shape[0]}"
    )

    if not changed_processed_participants_df.empty:
        print("\nDetails of Changes for processed participants:")
        # Prepare columns for report display
        report_cols_map = {
            f"{col}_initial": f"initial_{col}" for col in existing_count_cols
        }
        report_cols_map.update(
            {f"{col}_final": f"final_{col}" for col in existing_count_cols}
        )
        changed_processed_participants_df = changed_processed_participants_df.rename(
            columns=report_cols_map
        )
        cols_to_print_report = [
            item
            for col in existing_count_cols
            for item in (f"initial_{col}", f"final_{col}")
        ]

        print(changed_processed_participants_df[cols_to_print_report].to_markdown())
    else:
        print(
            "No count values were changed for the participants processed by the script."
        )

else:
    print(
        " - Initial count columns were not present; cannot generate comparison report."
    )

# Report on excluded participants whose counts were blanked
if excluded_pids:
    print(
        f"\nNumber of participants (Status {excluded_status_code}) whose counts were "
        f"blanked: {len(excluded_pids)}"
    )


# --- Remove Specific Record ID Check ---
# This section has been removed as it contained potentially identifying hardcoded IDs.
# If you need to check specific participants, filter the final DataFrame manually.
print("\n--- Specific Record ID Check Section Removed (contained identifying info) ---")
print("If you need to check specific participants, please filter the output CSV file.")


# --- Preview of Updated Data ---
print("\nPreview of data with updated columns (first 5 baseline rows with counts):")
# Filter for baseline rows where at least one count column is not blank
count_cols_exist_in_df = [
    col
    for col in [
        MISSED_TEST_COUNT_COL,
        OUTSIDE_WINDOW_COUNT_COL,
        TOTAL_DEVIATION_COUNT_COL,
    ]
    if col in df.columns
]
if count_cols_exist_in_df:
    baseline_rows_with_counts = df.loc[
        (df[EVENT_NAME_COL] == BASELINE_EVENT_NAME)
        & (df[count_cols_exist_in_df].notna().any(axis=1))
    ].head()

    if not baseline_rows_with_counts.empty:
        # Define columns to print in the preview
        cols_to_print_preview = [
            PARTICIPANT_ID_COL,
            EVENT_NAME_COL,
            STATUS_COL,
            ENDPOINT_DATE_1_COL,
            ENDPOINT_DATE_2_COL,
        ] + count_cols_exist_in_df
        cols_to_print_preview_exist = [
            col
            for col in cols_to_print_preview
            if col in baseline_rows_with_counts.columns
        ]

        # Prepare for printing (handle NA/NaN)
        preview_df_print = (
            baseline_rows_with_counts[cols_to_print_preview_exist]
            .astype(object)
            .replace({pd.NA: "", np.nan: ""})
        )
        print(
            preview_df_print.to_markdown(index=False, numalign="left", stralign="left")
        )
    else:
        print("No baseline rows with calculated counts found to preview.")
else:
    print("Count columns not found in the final DataFrame to preview.")


# --- Save Updated Data ---
print(f"\nSaving updated data to {OUTPUT_FILE}...")
try:
    # Save with UTF-8 encoding, which is generally preferred
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    print("File saved successfully.")
except Exception as e:
    print(f"Error saving file: {e}")


print(f"\nTotal rows in final file: {df.shape[0]}")
print("\nScript finished.")
