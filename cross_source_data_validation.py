# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import sys
import os
import re # Import re for integer extraction

# --- Configuration ---
# !!! IMPORTANT: Update these constants to match your specific data files, sheet, column names, and event names !!!

# Input Files & Sheet
# Replace with paths to your data files
COMPARISON_EXCEL_FILE = "comparison_data_source.xlsx" # The Excel file name (e.g., master data source)
EXCEL_SHEET_NAME = "data_input_sheet"           # The specific sheet name containing the data within the Excel file

GROUP2_CSV_FILE = "group2_data_source.csv"      # e.g., Intervention group data source
GROUP1_CSV_FILE = "group1_data_source.csv"      # e.g., Control group data source

# Define column names for clarity and robustness
# !! Update these variable values if your column names are different !!
ID_COL = "record_id"                  # Column containing the unique participant identifier
EVENT_NAME_COL = "event_name"           # Column containing the event/visit name
REPEAT_INSTANCE_COL = "repeat_instance"     # Column indicating repeating instrument instance (often relevant in exports)
STATUS_COL = "participant_status"       # Column indicating participant status (e.g., active, withdrawn)
ENDPOINT_DATE_1_COL = "primary_endpoint_date"   # e.g., Date of primary outcome event or EOS date for some statuses
ENDPOINT_DATE_2_COL = "secondary_endpoint_date" # e.g., Date of withdrawal or loss to follow-up for other statuses
VISIT_DATE_COL = "visit_date"           # Column for the general visit date (used for baseline)
TEST_DATE_COL = "test_date"             # Column for the specific date the test/assessment was performed
ASSESSMENT_COLLECTED_COL = "assessment_collected" # Example: Column indicating if assessment data/sample was collected
RESULT_COL = "result"                   # Column containing a result value (e.g., test score, measurement)

# Define the target event names
# !! Update these event names to match those used in your 'EVENT_NAME_COL' !!
TARGET_BASELINE_EVENTS = ['study_baseline', 'study_baseline'] # List of possible baseline event names (used in both sources)
TARGET_EOS_EVENTS = ['end_of_study_visit', 'end_of_study_visit']                 # List of possible End-of-Study event names (used in Excel)

# --- Define Column Mappings for Cross-File Comparison ---
# Columns expected on the CSV Baseline row AND Excel Baseline row (filtered to no repeat instance)
# These will be compared CSV Baseline vs Excel Baseline (filtered)
BASELINE_CSV_VS_EXCEL_BASE_COLS = [
    VISIT_DATE_COL,
    ASSESSMENT_COLLECTED_COL,
    RESULT_COL,
    TEST_DATE_COL
]

# Columns expected on CSV Baseline row but Excel EOS row (filtered to no repeat instance)
# These will be compared CSV Baseline vs Excel EOS (filtered)
CSV_BASELINE_VS_EXCEL_EOS_COLS = [
    STATUS_COL,
    ENDPOINT_DATE_1_COL,
    ENDPOINT_DATE_2_COL
]

# List of all date columns to normalize across all files
ALL_DATE_COLS = [VISIT_DATE_COL, TEST_DATE_COL, ENDPOINT_DATE_1_COL, ENDPOINT_DATE_2_COL]

# Output file names
DISCREPANCY_VALUE_FILE = 'value_comparison_discrepancies.csv'
ARM_MISMATCH_FILE = 'arm_classification_mismatches.csv'


# --- Helper Function to extract integer from result string ---
def extract_integer_result(result_str):
    """Extracts integer part from a string, handling NaN and non-digit chars."""
    if pd.isna(result_str):
        return None
    result_str = str(result_str) # Ensure string type
    match = re.search(r'\d+', result_str)
    if match:
        return int(match.group(0))
    return None


# --- Helper Function to Load and Clean Data ---
def load_and_clean_data(file_path, file_type='csv', sheet_name=None, id_col=ID_COL, event_col=EVENT_NAME_COL, repeat_col=REPEAT_INSTANCE_COL, all_date_cols=ALL_DATE_COLS):
    """Loads and cleans data from CSV or Excel, handling common issues."""
    print(f"Loading and cleaning {file_type.upper()} data from: {file_path}" + (f" (Sheet: '{sheet_name}')" if sheet_name else ""))

    # Define all potential columns needed based on configuration
    potential_cols = list(set([id_col, event_col, repeat_col] + BASELINE_CSV_VS_EXCEL_BASE_COLS + CSV_BASELINE_VS_EXCEL_EOS_COLS + all_date_cols))
    potential_cols = list(set(col for col in potential_cols if isinstance(col, str) and col)) # Keep valid string names

    try:
        if file_type.lower() == 'excel':
            if not sheet_name:
                raise ValueError("Sheet name must be provided for Excel files.")
            df = pd.read_excel(file_path, sheet_name=sheet_name, usecols=lambda c: c in potential_cols) # Read only relevant cols
        elif file_type.lower() == 'csv':
            try:
                df = pd.read_csv(file_path, usecols=lambda c: c in potential_cols)
            except UnicodeDecodeError:
                print(f" - UnicodeDecodeError reading {os.path.basename(file_path)}, trying latin-1...", file=sys.stderr)
                df = pd.read_csv(file_path, encoding='latin-1', usecols=lambda c: c in potential_cols)
        else:
            raise ValueError(f"Unsupported file_type: {file_type}")

        print(f" - Successfully loaded {df.shape[0]} rows.")
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}", file=sys.stderr)
        return None
    except ValueError as e:
         print(f"Error loading specified columns or sheet from {os.path.basename(file_path)}: {e}", file=sys.stderr)
         print("   Please ensure the required columns/sheet exist.", file=sys.stderr)
         return None
    except Exception as e:
        print(f"Error loading file {os.path.basename(file_path)}: {e}", file=sys.stderr)
        return None

    # Clean column names (remove BOM and strip whitespace)
    df.columns = df.columns.str.replace('\ufeff', '', regex=False).str.strip()

    # Ensure core required columns exist after cleaning names
    core_required = [id_col, event_col]
    if file_type.lower() == 'excel':
        core_required.append(repeat_col) # Repeat instance column is essential for Excel filtering
    for col in core_required:
         if col not in df.columns:
             print(f"Error: Core required column '{col}' not found after cleaning names in {os.path.basename(file_path)}.", file=sys.stderr)
             return None

    # Clean and convert core column types
    df[id_col] = df[id_col].astype(str).str.strip()
    df[event_col] = df[event_col].astype(str).str.strip()

    # Handle repeat instance column if it exists (mainly for Excel)
    if repeat_col in df.columns:
        df[repeat_col] = df[repeat_col].astype(str).str.strip().replace('nan', np.nan)

    # Convert 'nan' strings to actual NaN values globally
    df.replace('nan', np.nan, inplace=True)

    # Convert date columns to datetime objects and normalize
    for col in all_date_cols:
         if col in df.columns:
             df[col] = pd.to_datetime(df[col], errors='coerce').dt.normalize()

    # Convert Status to numeric if it exists
    if STATUS_COL in df.columns:
         df[STATUS_COL] = pd.to_numeric(df[STATUS_COL], errors='coerce')

    print(f" - Data cleaning complete for {os.path.basename(file_path)}.")
    return df


# --- Main Comparison Logic ---
def perform_cross_source_validation(df_group1_csv, df_group2_csv, df_excel):
    """
    Performs data validation by comparing values between CSV sources (combined)
    and an Excel source, focusing on baseline and EOS events according to predefined rules.
    Reports discrepancies and summary counts.
    """
    print("\n--- Starting Cross-Source Data Validation ---")

    # Check if dataframes were loaded successfully
    if df_group1_csv is None or df_group2_csv is None or df_excel is None:
        print("Error: One or more dataframes failed to load. Aborting validation.", file=sys.stderr)
        return

    # --- Filter DataFrames for Comparison ---
    print("--- Filtering data for baseline and EOS events ---")

    # Combine individual CSVs, filter for baseline events, and get first row per ID
    df_csv_baseline_combined = pd.concat([
        df_group1_csv[df_group1_csv[EVENT_NAME_COL].isin(TARGET_BASELINE_EVENTS)].copy(),
        df_group2_csv[df_group2_csv[EVENT_NAME_COL].isin(TARGET_BASELINE_EVENTS)].copy()
    ], ignore_index=True)
    df_csv_baseline_filtered = df_csv_baseline_combined.groupby(ID_COL).head(1).reset_index(drop=True)
    print(f" - Filtered CSV Baseline Rows: {len(df_csv_baseline_filtered)}")

    # Filter Excel for baseline events AND empty repeat instance, get first row per ID
    df_excel_baseline_filtered = df_excel[
        (df_excel[EVENT_NAME_COL].isin(TARGET_BASELINE_EVENTS)) &
        (df_excel[REPEAT_INSTANCE_COL].isnull()) # isnull checks for NaN/None correctly after cleaning
    ].groupby(ID_COL).head(1).reset_index(drop=True)
    print(f" - Filtered Excel Baseline Rows (No Repeat Instance): {len(df_excel_baseline_filtered)}")

    # Filter Excel for EOS events AND empty repeat instance, get first row per ID
    df_excel_eos_filtered = df_excel[
        (df_excel[EVENT_NAME_COL].isin(TARGET_EOS_EVENTS)) &
        (df_excel[REPEAT_INSTANCE_COL].isnull()) # isnull checks for NaN/None correctly after cleaning
    ].groupby(ID_COL).head(1).reset_index(drop=True)
    print(f" - Filtered Excel EOS Rows (No Repeat Instance): {len(df_excel_eos_filtered)}")


    # --- Comparison Logic ---
    print("--- Comparing data points across sources ---")
    discrepancies = []
    csv_baseline_ids = set(df_csv_baseline_filtered[ID_COL].unique()) # Use set for efficiency

    # Iterate through unique IDs found in the filtered CSV baseline data
    for record_id in csv_baseline_ids:
        csv_baseline_row = df_csv_baseline_filtered[df_csv_baseline_filtered[ID_COL] == record_id].iloc[0]

        # Find corresponding filtered rows in Excel
        excel_baseline_row_match = df_excel_baseline_filtered[df_excel_baseline_filtered[ID_COL] == record_id]
        excel_eos_row_match = df_excel_eos_filtered[df_excel_eos_filtered[ID_COL] == record_id]

        excel_baseline_exists = not excel_baseline_row_match.empty
        excel_eos_exists = not excel_eos_row_match.empty

        excel_baseline_row = excel_baseline_row_match.iloc[0] if excel_baseline_exists else pd.Series(dtype='object')
        excel_eos_row = excel_eos_row_match.iloc[0] if excel_eos_exists else pd.Series(dtype='object')

        # Function to compare two values based on specified rules
        def check_mismatch(val1, val2, col_name):
            note_msg = None
            # Rule 1: Both missing/NaN -> Match
            if pd.isna(val1) and pd.isna(val2):
                return False, note_msg
            # Rule 2: One missing, one present -> Not considered a mismatch here
            elif pd.isna(val1) or pd.isna(val2):
                return False, note_msg # Could add note 'Inconsistency (one empty)' if desired
            # Rule 3: Both present -> Must be equal
            else:
                if col_name == RESULT_COL:
                    int1 = extract_integer_result(val1)
                    int2 = extract_integer_result(val2)
                    if int1 != int2:
                        note_msg = f"Integer part differs: CSV={int1}, Excel={int2} (Originals: '{val1}', '{val2}')"
                        return True, note_msg
                elif col_name in ALL_DATE_COLS:
                    if val1 != val2: # Assumes already normalized datetime objects
                        note_msg = "Dates differ"
                        return True, note_msg
                elif col_name == STATUS_COL:
                    # Compare integer part as string to handle 1 vs 1.0
                    str1 = str(val1).split('.')[0].strip()
                    str2 = str(val2).split('.')[0].strip()
                    if str1 != str2:
                        note_msg = f"Status values differ (Originals: '{val1}', '{val2}')"
                        return True, note_msg
                else: # General string comparison
                    if str(val1).strip() != str(val2).strip():
                        note_msg = "Values differ"
                        return True, note_msg
            return False, note_msg # Return no mismatch if none of the above conditions met

        # Compare columns: CSV Baseline vs Excel Baseline (Filtered)
        for col in BASELINE_CSV_VS_EXCEL_BASE_COLS:
            if col not in csv_baseline_row.index or (excel_baseline_exists and col not in excel_baseline_row.index):
                print(f"Warning: Column '{col}' missing during comparison for ID {record_id}. Check data sources.", file=sys.stderr)
                continue # Skip comparison for this column if missing

            csv_val = csv_baseline_row.get(col)
            excel_val = excel_baseline_row.get(col)
            is_mismatch, note = check_mismatch(csv_val, excel_val, col)
            if is_mismatch:
                discrepancies.append({
                    'record_id': record_id, 'field': col, 'Source_CSV_Row': 'Baseline', 'Source_Excel_Row': 'Baseline (Filtered)',
                    'CSV_value': csv_val.strftime('%Y-%m-%d') if isinstance(csv_val, pd.Timestamp) else csv_val,
                    'Excel_value': excel_val.strftime('%Y-%m-%d') if isinstance(excel_val, pd.Timestamp) else excel_val,
                    'note': note
                })

        # Compare columns: CSV Baseline vs Excel EOS (Filtered)
        for col in CSV_BASELINE_VS_EXCEL_EOS_COLS:
             if col not in csv_baseline_row.index or (excel_eos_exists and col not in excel_eos_row.index):
                print(f"Warning: Column '{col}' missing during comparison for ID {record_id}. Check data sources.", file=sys.stderr)
                continue # Skip comparison for this column if missing

             csv_val = csv_baseline_row.get(col)
             excel_val = excel_eos_row.get(col)
             is_mismatch, note = check_mismatch(csv_val, excel_val, col)
             if is_mismatch:
                 discrepancies.append({
                    'record_id': record_id, 'field': col, 'Source_CSV_Row': 'Baseline', 'Source_Excel_Row': 'EOS (Filtered)',
                    'CSV_value': csv_val.strftime('%Y-%m-%d') if isinstance(csv_val, pd.Timestamp) else csv_val,
                    'Excel_value': excel_val.strftime('%Y-%m-%d') if isinstance(excel_val, pd.Timestamp) else excel_val,
                    'note': note
                 })

    # --- Identify Records Existing Only in Filtered Excel ---
    excel_baseline_ids = set(df_excel_baseline_filtered[ID_COL].unique())
    excel_eos_ids = set(df_excel_eos_filtered[ID_COL].unique())

    ids_excel_base_only = list(excel_baseline_ids - csv_baseline_ids)
    for record_id in ids_excel_base_only:
        excel_row = df_excel_baseline_filtered[df_excel_baseline_filtered[ID_COL] == record_id].iloc[0]
        discrepancies.append({
            'record_id': record_id, 'field': 'Record Existence', 'Source_CSV_Row': 'N/A', 'Source_Excel_Row': 'Baseline (Filtered)',
            'CSV_value': 'Missing in Filtered CSV Baseline', 'Excel_value': f"Exists ({excel_row.get(EVENT_NAME_COL, 'Event N/A')})",
            'note': 'Record found in filtered Excel Baseline but not filtered CSV Baseline.'
        })

    ids_excel_eos_only = list(excel_eos_ids - csv_baseline_ids - excel_baseline_ids) # Exclude those already caught
    for record_id in ids_excel_eos_only:
        excel_row = df_excel_eos_filtered[df_excel_eos_filtered[ID_COL] == record_id].iloc[0]
        discrepancies.append({
            'record_id': record_id, 'field': 'Record Existence', 'Source_CSV_Row': 'N/A', 'Source_Excel_Row': 'EOS (Filtered)',
            'CSV_value': 'Missing in Filtered CSV Baseline', 'Excel_value': f"Exists ({excel_row.get(EVENT_NAME_COL, 'Event N/A')})",
            'note': 'Record found in filtered Excel EOS but not filtered CSV Baseline (and not caught as Excel Baseline only).'
        })


    # --- Check for Arm Mismatches ---
    # Assumes arm is indicated by event names like '...arm_1', '...arm_2'
    arm1_baseline_events = [e for e in TARGET_BASELINE_EVENTS if 'arm_1' in e]
    arm2_baseline_events = [e for e in TARGET_BASELINE_EVENTS if 'arm_2' in e]
    arm1_eos_events = [e for e in TARGET_EOS_EVENTS if 'arm_1' in e]
    arm2_eos_events = [e for e in TARGET_EOS_EVENTS if 'arm_2' in e]

    csv_arm1_ids = set(df_csv_baseline_filtered[df_csv_baseline_filtered[EVENT_NAME_COL].isin(arm1_baseline_events)][ID_COL])
    csv_arm2_ids = set(df_csv_baseline_filtered[df_csv_baseline_filtered[EVENT_NAME_COL].isin(arm2_baseline_events)][ID_COL])
    excel_arm1_ids = set(df_excel_eos_filtered[df_excel_eos_filtered[EVENT_NAME_COL].isin(arm1_eos_events)][ID_COL])
    excel_arm2_ids = set(df_excel_eos_filtered[df_excel_eos_filtered[EVENT_NAME_COL].isin(arm2_eos_events)][ID_COL])

    arm_mismatches = []
    # Find IDs present in both sources but with conflicting arm assignments
    common_ids = (csv_arm1_ids | csv_arm2_ids) & (excel_arm1_ids | excel_arm2_ids)
    for pid in common_ids:
        csv_is_arm1 = pid in csv_arm1_ids
        excel_is_arm1 = pid in excel_arm1_ids
        if csv_is_arm1 != excel_is_arm1: # Mismatch if one is arm 1 and the other isn't (must be arm 2)
             csv_event = df_csv_baseline_filtered.loc[df_csv_baseline_filtered[ID_COL]==pid, EVENT_NAME_COL].iloc[0]
             excel_event = df_excel_eos_filtered.loc[df_excel_eos_filtered[ID_COL]==pid, EVENT_NAME_COL].iloc[0]
             arm_mismatches.append({
                 'record_id': pid,
                 'issue': f"Arm Mismatch: CSV Baseline Event '{csv_event}' vs Excel EOS Event '{excel_event}'"
             })

    print(f"--- Comparison checks finished. Found {len(discrepancies)} value/existence discrepancies and {len(arm_mismatches)} potential arm mismatches. ---")


    # --- Generate Summary Counts ---
    print("\n=== SUMMARY COUNTS ===")

    # Function to print status counts for a dataframe
    def print_status_counts(df, description):
        print(f"\n  Participant Status Counts ({description}):")
        if STATUS_COL in df.columns:
            status_counts = df[STATUS_COL].dropna().astype(int).value_counts().sort_index()
            total_with_status = status_counts.sum()
            if total_with_status > 0:
                for status, count in status_counts.items(): print(f"    Status {status}: {count}")
                missing_status = df[STATUS_COL].isnull().sum()
                if missing_status > 0: print(f"    Status Missing/Invalid: {missing_status}")
            else: print(f"    No valid '{STATUS_COL}' values found.")
        else: print(f"  '{STATUS_COL}' column not found.")

    # Filtered CSV Baseline Counts
    print("\n--- Counts from Filtered CSV Baseline ---")
    print("  Arm Counts:")
    arm1_count_csv = len(csv_arm1_ids)
    arm2_count_csv = len(csv_arm2_ids)
    print(f"    Arm 1: {arm1_count_csv}")
    print(f"    Arm 2: {arm2_count_csv}")
    print(f"    Total: {arm1_count_csv + arm2_count_csv}")
    print_status_counts(df_csv_baseline_filtered, "Filtered CSV Baseline")

    # Filtered Excel EOS Counts
    print("\n--- Counts from Filtered Excel EOS ---")
    print("  Arm Counts:")
    arm1_count_excel_eos = len(excel_arm1_ids)
    arm2_count_excel_eos = len(excel_arm2_ids)
    print(f"    Arm 1: {arm1_count_excel_eos}")
    print(f"    Arm 2: {arm2_count_excel_eos}")
    print(f"    Total: {arm1_count_excel_eos + arm2_count_excel_eos}")
    # Status Counts By Arm (Excel EOS)
    print_status_counts(df_excel_eos_filtered[df_excel_eos_filtered[ID_COL].isin(excel_arm1_ids)], "Filtered Excel EOS - Arm 1")
    print_status_counts(df_excel_eos_filtered[df_excel_eos_filtered[ID_COL].isin(excel_arm2_ids)], "Filtered Excel EOS - Arm 2")


    # --- Report Discrepancies ---
    print("\n=== VALIDATION FINDINGS ===")

    if not discrepancies and not arm_mismatches:
        print("✅ No value/existence discrepancies or arm mismatches found based on comparison criteria.")
    else:
        print("❌ Validation Issues Found:")

    if discrepancies:
        print(f"\n--- Value and Existence Discrepancies ({len(discrepancies)} found) ---")
        discrepancies_df = pd.DataFrame(discrepancies)
        discrepancy_cols_order = ['record_id', 'field', 'Source_CSV_Row', 'Source_Excel_Row', 'CSV_value', 'Excel_value', 'note']
        discrepancies_df = discrepancies_df[[col for col in discrepancy_cols_order if col in discrepancies_df.columns]]
        print(discrepancies_df.to_string(index=False))
        try:
            discrepancies_df.to_csv(DISCREPANCY_VALUE_FILE, index=False, encoding='utf-8')
            print(f"\n  -> Discrepancies saved to: {DISCREPANCY_VALUE_FILE}")
        except Exception as e:
            print(f"\nError saving value discrepancies: {e}", file=sys.stderr)

    if arm_mismatches:
        print(f"\n--- Arm Classification Mismatches ({len(arm_mismatches)} found) ---")
        arm_mismatches_df = pd.DataFrame(arm_mismatches)
        print(arm_mismatches_df.to_string(index=False))
        try:
            arm_mismatches_df.to_csv(ARM_MISMATCH_FILE, index=False, encoding='utf-8')
            print(f"\n  -> Arm mismatches saved to: {ARM_MISMATCH_FILE}")
        except Exception as e:
            print(f"\nError saving arm mismatches: {e}", file=sys.stderr)


# --- Script Execution ---
if __name__ == "__main__":
    print("--- Initializing Cross-Source Data Validation Script ---")
    # Load data using the helper function
    df_excel = load_and_clean_data(COMPARISON_EXCEL_FILE, file_type='excel', sheet_name=EXCEL_SHEET_NAME)
    df_group1_csv = load_and_clean_data(GROUP1_CSV_FILE, file_type='csv')
    df_group2_csv = load_and_clean_data(GROUP2_CSV_FILE, file_type='csv')

    # Perform the comparison using the loaded dataframes
    perform_cross_source_validation(df_group1_csv, df_group2_csv, df_excel)

    print("\n--- Script Finished ---")