# -*- coding: utf-8 -*-
import pandas as pd
import re
import sys
import os
from collections import defaultdict # Import defaultdict for grouping issues

# --- Configuration ---
# !!! IMPORTANT: Update these constants to match your specific data files, sheet, column names, and event names !!!

# Define the filenames and sheet name
# Replace with paths to your data files
GROUP1_CSV_FILE = 'group1_data.csv'         # e.g., Control group data source
GROUP2_CSV_FILE = 'group2_data.csv'         # e.g., Intervention group data source
COMPARISON_EXCEL_FILE = 'comparison_data.xlsx' # e.g., Master data or alternative source for comparison
EXCEL_SHEET_NAME = 'data_sheet'             # The sheet name within the Excel file containing the data

# --- Column Names ---
# !! Update these variable values if your column names are different !!
ID_COL = "record_id"                  # Column containing the unique participant identifier
EVENT_NAME_COL = "event_name"           # Column containing the event/visit name
VISIT_DATE_COL = "visit_date"           # Column for the general visit date
ASSESSMENT_COMPLETE_COL = "assessment_complete" # Example: Column indicating if a specific assessment/test was completed (used for existence check)
ASSESSMENT_COLLECTED_COL = "assessment_collected" # Example: Column indicating if assessment data/sample was collected
RESULT_COL = "result"                   # Column containing a result value (e.g., test score, measurement)
TEST_DATE_COL = "test_date"             # Column for the specific date the test/assessment was performed
PARTICIPANT_STATUS_COL = "participant_status" # Column indicating participant status (e.g., active, withdrawn)
ENDPOINT_DATE_1_COL = "primary_endpoint_date"   # e.g., Date of primary outcome event or EOS date for some statuses
ENDPOINT_DATE_2_COL = "secondary_endpoint_date" # e.g., Date of withdrawal or loss to follow-up for other statuses
REPEAT_INSTANCE_COL = "repeat_instance"     # Column indicating repeating instrument instance (often relevant in exports )

# --- Event Names ---
# !! Update these event names to match those used in your 'EVENT_NAME_COL' !!
BASELINE_EVENT_NAMES = ['study_baseline', 'study_baseline'] # List of possible baseline event names
EOS_EVENT_NAMES = ['end_of_study_visit', 'end_of_study_visit']               # List of possible End-of-Study event names

# --- Comparison Columns Configuration ---
# Columns expected on the baseline row in BOTH CSV and Excel (when REPEAT_INSTANCE_COL is empty in Excel)
# These will be compared: CSV Baseline Row vs Excel Baseline Row (Filtered)
# Note: ASSESSMENT_COMPLETE_COL was intentionally excluded from this list based on original script comment.
BASELINE_CSV_VS_EXCEL_BASE_COLS = [
    VISIT_DATE_COL,
    ASSESSMENT_COLLECTED_COL,
    RESULT_COL,
    TEST_DATE_COL, # This is the test date column
]

# Columns expected on CSV Baseline row but compared against the Excel EOS row (when REPEAT_INSTANCE_COL is empty in Excel)
# These will be compared: CSV Baseline Row vs Excel EOS Row (Filtered)
CSV_BASELINE_VS_EXCEL_EOS_COLS = [
    PARTICIPANT_STATUS_COL,
    ENDPOINT_DATE_1_COL,
    ENDPOINT_DATE_2_COL
]

# Output file for discrepancies
DISCREPANCY_OUTPUT_FILE = 'data_comparison_discrepancies.csv'


# --- Helper Function ---
# Function to extract integer from result string
# Handles potential NaN values and strings with non-digit characters
def extract_integer_result(result_str):
    """Attempts to extract the first integer found within a string value."""
    if pd.isna(result_str):
        return None
    # Convert to string to handle various input types gracefully
    result_str = str(result_str)
    # Use regex to find the first sequence of digits in the string
    match = re.search(r'\d+', result_str)
    if match:
        return int(match.group(0))
    return None # Return None if no digits are found

# --- Main Comparison Logic ---
def compare_data_sources(
    group1_csv_path,
    group2_csv_path,
    excel_path,
    sheet_name,
    baseline_event_names,
    eos_event_names,
    base_compare_cols,
    eos_compare_cols
    ):
    """
    Compares baseline and EOS data between two CSV sources (combined) and an Excel source.

    Focuses on comparing specific columns from the baseline row in the CSVs
    against corresponding columns in baseline and EOS rows (filtered by empty repeat instance)
    in the Excel file. Also reports summary statistics and discrepancies.
    """
    print("--- Starting Data Comparison ---")
    print(f"Group 1 CSV: {group1_csv_path}")
    print(f"Group 2 CSV: {group2_csv_path}")
    print(f"Comparison Excel: {excel_path} (Sheet: '{sheet_name}')")
    print(f"Baseline Events: {baseline_event_names}")
    print(f"EOS Events: {eos_event_names}")

    try:
        # Load the CSV files
        # Specify encoding='latin-1' based on original script; adjust if needed for your data
        try:
            df_group1 = pd.read_csv(group1_csv_path, encoding='latin-1')
            df_group2 = pd.read_csv(group2_csv_path, encoding='latin-1')
        except UnicodeDecodeError:
             print("Warning: Failed to read CSV with latin-1 encoding. Trying utf-8.", file=sys.stderr)
             df_group1 = pd.read_csv(group1_csv_path, encoding='utf-8')
             df_group2 = pd.read_csv(group2_csv_path, encoding='utf-8')


        # Concatenate the two CSV dataframes for the main comparison loop
        df_csv_combined = pd.concat([df_group1, df_group2], ignore_index=True)
        print(f"Combined CSV data loaded. Shape: {df_csv_combined.shape}")

        # Load the Excel file, specifying the sheet name
        df_excel = pd.read_excel(excel_path, sheet_name=sheet_name)
        print(f"Excel data loaded. Shape: {df_excel.shape}")

    except FileNotFoundError as e:
        print(f"Error loading file: {e}", file=sys.stderr)
        sys.exit(1) # Exit the script if files are not found
    except Exception as e:
        print(f"An error occurred while reading the data files: {e}", file=sys.stderr)
        sys.exit(1)

    # --- Data Cleaning and Preparation ---
    print("\n--- Cleaning and Preparing Data ---")
    # Clean column names (strip leading/trailing whitespace)
    df_group1.columns = df_group1.columns.str.strip()
    df_group2.columns = df_group2.columns.str.strip()
    df_csv_combined.columns = df_csv_combined.columns.str.strip()
    df_excel.columns = df_excel.columns.str.strip()
    print(" - Column names stripped of whitespace.")

    # Ensure required columns exist in dataframes
    all_required_cols = list(set(
        base_compare_cols +
        eos_compare_cols +
        [ID_COL, EVENT_NAME_COL, REPEAT_INSTANCE_COL, ASSESSMENT_COMPLETE_COL] # Add essential cols
    ))
    missing_csv_cols = [col for col in all_required_cols if col not in df_csv_combined.columns and col != REPEAT_INSTANCE_COL] # Allow REPEAT_INSTANCE_COL to be missing in CSV
    missing_excel_cols = [col for col in all_required_cols if col not in df_excel.columns]

    if missing_csv_cols:
        print(f"Error: Required columns missing in combined CSV data: {missing_csv_cols}", file=sys.stderr)
        sys.exit(1)
    if missing_excel_cols:
        print(f"Error: Required columns missing in {excel_path} (sheet '{sheet_name}'): {missing_excel_cols}", file=sys.stderr)
        sys.exit(1)
    print(" - Required columns checked.")

    # Ensure ID and Event Name are strings and strip whitespace
    for df in [df_group1, df_group2, df_csv_combined, df_excel]:
        df[ID_COL] = df[ID_COL].astype(str).str.strip()
        df[EVENT_NAME_COL] = df[EVENT_NAME_COL].astype(str).str.strip()
    print(f" - '{ID_COL}' and '{EVENT_NAME_COL}' standardized to string.")

    # Normalize date columns to datetime objects, coercing errors, in all relevant dataframes
    date_cols_to_normalize = [VISIT_DATE_COL, TEST_DATE_COL, ENDPOINT_DATE_1_COL, ENDPOINT_DATE_2_COL]
    for df in [df_group1, df_group2, df_csv_combined, df_excel]:
        for col in date_cols_to_normalize:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.normalize()
    print(f" - Date columns ({date_cols_to_normalize}) normalized.")


    # --- Filter DataFrames ---
    print("\n--- Filtering Data for Comparison ---")
    # Filter individual CSVs for baseline events (for status counts)
    df_group1_baseline = df_group1[df_group1[EVENT_NAME_COL].isin(baseline_event_names)].groupby(ID_COL).head(1).reset_index(drop=True)
    df_group2_baseline = df_group2[df_group2[EVENT_NAME_COL].isin(baseline_event_names)].groupby(ID_COL).head(1).reset_index(drop=True)

    # Filter combined CSV for baseline events (main comparison loop source)
    df_csv_baseline_filtered = df_csv_combined[df_csv_combined[EVENT_NAME_COL].isin(baseline_event_names)].groupby(ID_COL).head(1).reset_index(drop=True)
    print(f" - Filtered CSV baseline rows: {len(df_csv_baseline_filtered)}")

    # Filter Excel for baseline events AND where repeat instance is null/empty
    df_excel_baseline_filtered = df_excel[
        (df_excel[EVENT_NAME_COL].isin(baseline_event_names)) &
        (df_excel[REPEAT_INSTANCE_COL].isnull() | (df_excel[REPEAT_INSTANCE_COL] == '')) # Check for null or empty string
    ].groupby(ID_COL).head(1).reset_index(drop=True)
    print(f" - Filtered Excel baseline rows (empty repeat instance): {len(df_excel_baseline_filtered)}")

    # Filter Excel for EOS events AND where repeat instance is null/empty
    df_excel_eos_filtered = df_excel[
        (df_excel[EVENT_NAME_COL].isin(eos_event_names)) &
        (df_excel[REPEAT_INSTANCE_COL].isnull() | (df_excel[REPEAT_INSTANCE_COL] == '')) # Check for null or empty string
    ].groupby(ID_COL).head(1).reset_index(drop=True)
    print(f" - Filtered Excel EOS rows (empty repeat instance): {len(df_excel_eos_filtered)}")


    # --- Comparison Logic ---
    print("\n--- Performing Comparisons ---")
    discrepancies = []
    compared_record_ids_count = 0
    csv_baseline_ids = set(df_csv_baseline_filtered[ID_COL].unique()) # Use set for faster lookups

    # Iterate through the filtered CSV baseline data (main source for comparison)
    for index, csv_baseline_row in df_csv_baseline_filtered.iterrows():
        record_id = csv_baseline_row[ID_COL]
        compared_record_ids_count += 1

        # Find the corresponding baseline row in the filtered Excel data
        excel_baseline_row_match = df_excel_baseline_filtered[df_excel_baseline_filtered[ID_COL] == record_id]

        # Find the corresponding EOS row in the filtered Excel data
        excel_eos_row_match = df_excel_eos_filtered[df_excel_eos_filtered[ID_COL] == record_id]

        # --- Scenario 1: Compare Baseline Columns (CSV Baseline vs Excel Baseline) ---
        if excel_baseline_row_match.empty:
            discrepancies.append({
                'record_id': record_id,
                'field': 'Baseline Row Existence',
                'CSV_value': f"Row exists ({csv_baseline_row[EVENT_NAME_COL]})",
                'Excel_value': 'Row Missing in Excel (filtered)',
                'note': 'Record ID exists in CSV baseline but relevant baseline row is missing in filtered Excel.'
            })
        else:
            excel_baseline_row = excel_baseline_row_match.iloc[0] # Get the first matching row

            for col in base_compare_cols:
                csv_value = csv_baseline_row.get(col)
                excel_value = excel_baseline_row.get(col)

                # Comparison logic (handling results, dates, general values, and NaNs)
                is_discrepant = False
                note = "Values differ"

                if col == RESULT_COL:
                    csv_result_int = extract_integer_result(csv_value)
                    excel_result_int = extract_integer_result(excel_value)
                    if csv_result_int != excel_result_int:
                        is_discrepant = True
                        note = f"Integer part differs: CSV={csv_result_int}, Excel={excel_result_int}"
                elif col in date_cols_to_normalize:
                    if not (pd.isna(csv_value) and pd.isna(excel_value)) and (csv_value != excel_value):
                         is_discrepant = True
                         note = 'Dates differ'
                         csv_value = csv_value.strftime('%Y-%m-%d') if pd.notna(csv_value) else 'No Date' # Format for output
                         excel_value = excel_value.strftime('%Y-%m-%d') if pd.notna(excel_value) else 'No Date' # Format for output
                else: # General comparison
                    if not (pd.isna(csv_value) and pd.isna(excel_value)) and (str(csv_value).strip() != str(excel_value).strip()):
                        is_discrepant = True

                if is_discrepant:
                    discrepancies.append({
                        'record_id': record_id,
                        'field': col,
                        'Source_CSV_Row': 'Baseline',
                        'Source_Excel_Row': 'Baseline (Filtered)',
                        'CSV_value': csv_value,
                        'Excel_value': excel_value,
                        'note': note
                    })

        # --- Scenario 2: Compare Status/EOS Date Columns (CSV Baseline vs Excel EOS) ---
        if excel_eos_row_match.empty:
             # Check if ANY relevant CSV baseline columns have data
             has_relevant_csv_data = any(pd.notna(csv_baseline_row.get(col)) for col in eos_compare_cols)
             if has_relevant_csv_data:
                 discrepancies.append({
                     'record_id': record_id,
                     'field': 'EOS Row Existence',
                     'CSV_value': f"Relevant data exists in CSV baseline for EOS fields",
                     'Excel_value': 'Relevant EOS Row Missing in Excel (filtered)',
                     'note': f"CSV baseline has data for EOS fields ({eos_compare_cols}), but relevant EOS row missing in filtered Excel."
                 })
        else:
            excel_eos_row = excel_eos_row_match.iloc[0] # Get the first matching relevant EOS row

            for col in eos_compare_cols:
                csv_value = csv_baseline_row.get(col)
                excel_value = excel_eos_row.get(col)

                is_discrepant = False
                note = "Values differ"

                if col in [ENDPOINT_DATE_1_COL, ENDPOINT_DATE_2_COL]:
                    if not (pd.isna(csv_value) and pd.isna(excel_value)) and (csv_value != excel_value):
                        is_discrepant = True
                        note = 'Dates differ'
                        csv_value = csv_value.strftime('%Y-%m-%d') if pd.notna(csv_value) else 'No Date'
                        excel_value = excel_value.strftime('%Y-%m-%d') if pd.notna(excel_value) else 'No Date'
                else: # General comparison (like participant_status)
                    # Compare integer part for statuses to handle float vs int (e.g., 1.0 vs 1)
                    csv_comp = str(csv_value).split('.')[0].strip() if pd.notna(csv_value) else 'NA'
                    excel_comp = str(excel_value).split('.')[0].strip() if pd.notna(excel_value) else 'NA'
                    if not (pd.isna(csv_value) and pd.isna(excel_value)) and (csv_comp != excel_comp):
                        is_discrepant = True

                if is_discrepant:
                    discrepancies.append({
                        'record_id': record_id,
                        'field': col,
                        'Source_CSV_Row': 'Baseline',
                        'Source_Excel_Row': 'EOS (Filtered)',
                        'CSV_value': csv_value, # Report original value before comparison formatting
                        'Excel_value': excel_value, # Report original value before comparison formatting
                        'note': note
                    })

    # --- Scenario 3: Record IDs in filtered Excel baseline NOT in filtered CSV baseline ---
    excel_baseline_ids = set(df_excel_baseline_filtered[ID_COL].unique())
    excel_ids_missing_in_csv_baseline = excel_baseline_ids - csv_baseline_ids
    if excel_ids_missing_in_csv_baseline:
         print(f" - Found {len(excel_ids_missing_in_csv_baseline)} Record IDs in Excel Baseline (filtered) but not in CSV Baseline.")
         for record_id in excel_ids_missing_in_csv_baseline:
             excel_row = df_excel_baseline_filtered[df_excel_baseline_filtered[ID_COL] == record_id].iloc[0]
             discrepancies.append({
                 'record_id': record_id,
                 'field': 'Baseline Row Existence',
                 'CSV_value': 'Row Missing in CSV baseline',
                 'Excel_value': f"Row exists ({excel_row[EVENT_NAME_COL]})",
                 'note': 'Record ID exists in filtered Excel baseline but is missing in CSV baseline data.'
             })

    # --- Scenario 4: Record IDs in filtered Excel EOS NOT in filtered CSV baseline ---
    excel_eos_ids = set(df_excel_eos_filtered[ID_COL].unique())
    excel_eos_ids_not_in_csv_baseline = excel_eos_ids - csv_baseline_ids
    if excel_eos_ids_not_in_csv_baseline:
        print(f" - Found {len(excel_eos_ids_not_in_csv_baseline)} Record IDs in Excel EOS (filtered) but not in CSV Baseline.")
        for record_id in excel_eos_ids_not_in_csv_baseline:
            excel_row = df_excel_eos_filtered[df_excel_eos_filtered[ID_COL] == record_id].iloc[0]
            discrepancies.append({
                'record_id': record_id,
                'field': 'EOS Row Existence vs CSV Baseline',
                'CSV_value': 'Baseline Row Missing in CSV',
                'Excel_value': f"EOS Row exists ({excel_row[EVENT_NAME_COL]})",
                'note': 'Relevant EOS row exists in filtered Excel but corresponding baseline row is missing in CSV.'
            })

    print(f"--- Comparison Complete. Found {len(discrepancies)} total discrepancies. ---")

    # --- Report Summary ---
    print("\n=== SUMMARY OF DATA COMPARISON (CSV vs Excel) ===")
    print(f"Total unique record IDs with relevant baseline event in combined CSVs: {len(csv_baseline_ids)}")
    print(f"Total unique record IDs with relevant baseline event in filtered Excel: {len(excel_baseline_ids)}")
    print(f"Total unique record IDs with relevant EOS event in filtered Excel: {len(excel_eos_ids)}")
    print(f"Total record IDs from CSV baseline compared: {compared_record_ids_count}")

    # --- Arm and Participant Status Counts ---

    # Determine Arm based on Event Names (using first event name in lists as representative)
    # !! This assumes event names clearly map to arms, like '...arm_1', '...arm_2' !!
    arm1_baseline_event = baseline_event_names[0] if baseline_event_names else None
    arm2_baseline_event = baseline_event_names[1] if len(baseline_event_names) > 1 else None
    arm1_eos_event = eos_event_names[0] if eos_event_names else None
    arm2_eos_event = eos_event_names[1] if len(eos_event_names) > 1 else None

    # Combined CSV Baseline Counts
    print("\n--- Counts from Combined CSV Baseline ---")
    print("  Arm Counts:")
    arm1_count_csv = 0
    arm2_count_csv = 0
    if arm1_baseline_event:
        arm1_count_csv = len(df_csv_baseline_filtered[df_csv_baseline_filtered[EVENT_NAME_COL] == arm1_baseline_event])
        print(f"    Arm 1: {arm1_count_csv}")
    if arm2_baseline_event:
        arm2_count_csv = len(df_csv_baseline_filtered[df_csv_baseline_filtered[EVENT_NAME_COL] == arm2_baseline_event])
        print(f"    Arm 2: {arm2_count_csv}")
    print(f"    Total Records with Relevant Baseline Event: {arm1_count_csv + arm2_count_csv}")

    print("\n  Participant Status Counts and Percentages:")
    if PARTICIPANT_STATUS_COL in df_csv_baseline_filtered.columns:
        status_counts_csv = df_csv_baseline_filtered[PARTICIPANT_STATUS_COL].dropna().astype(int).value_counts().sort_index()
        total_with_status_csv = status_counts_csv.sum()
        if total_with_status_csv > 0:
            for status, count in status_counts_csv.items():
                percentage = (count / total_with_status_csv) * 100
                print(f"    Status {status}: {count} ({percentage:.2f}%)")
            missing_status_csv = df_csv_baseline_filtered[PARTICIPANT_STATUS_COL].isnull().sum()
            if missing_status_csv > 0: print(f"    Status Missing/Invalid: {missing_status_csv}")
        else: print(f"    No valid '{PARTICIPANT_STATUS_COL}' values found.")
    else: print(f"  '{PARTICIPANT_STATUS_COL}' column not found.")

    # Filtered Excel EOS Counts
    print("\n--- Counts from Filtered Excel EOS ---")
    print("  Arm Counts:")
    arm1_count_excel_eos = 0
    arm2_count_excel_eos = 0
    if arm1_eos_event:
        arm1_count_excel_eos = len(df_excel_eos_filtered[df_excel_eos_filtered[EVENT_NAME_COL] == arm1_eos_event])
        print(f"    Arm 1: {arm1_count_excel_eos}")
    if arm2_eos_event:
        arm2_count_excel_eos = len(df_excel_eos_filtered[df_excel_eos_filtered[EVENT_NAME_COL] == arm2_eos_event])
        print(f"    Arm 2: {arm2_count_excel_eos}")
    print(f"    Total Records with Relevant EOS Event: {arm1_count_excel_eos + arm2_count_excel_eos}")

    print("\n  Participant Status Counts and Percentages by Arm:")
    # Arm 1 (Excel EOS)
    print("    Arm 1:")
    if arm1_eos_event:
        df_excel_eos_arm1 = df_excel_eos_filtered[df_excel_eos_filtered[EVENT_NAME_COL] == arm1_eos_event].copy()
        if PARTICIPANT_STATUS_COL in df_excel_eos_arm1.columns:
            status_counts_excel_arm1 = df_excel_eos_arm1[PARTICIPANT_STATUS_COL].dropna().astype(int).value_counts().sort_index()
            total_with_status_excel_arm1 = status_counts_excel_arm1.sum()
            if total_with_status_excel_arm1 > 0:
                for status, count in status_counts_excel_arm1.items():
                    percentage = (count / total_with_status_excel_arm1) * 100
                    print(f"      Status {status}: {count} ({percentage:.2f}%)")
                missing_status_excel_arm1 = df_excel_eos_arm1[PARTICIPANT_STATUS_COL].isnull().sum()
                if missing_status_excel_arm1 > 0: print(f"      Status Missing/Invalid: {missing_status_excel_arm1}")
            else: print("      No valid status values found for Arm 1.")
        else: print(f"    '{PARTICIPANT_STATUS_COL}' column not found for Arm 1.")
    else: print("    Arm 1 EOS event not defined.")

    # Arm 2 (Excel EOS)
    print("\n    Arm 2:")
    if arm2_eos_event:
        df_excel_eos_arm2 = df_excel_eos_filtered[df_excel_eos_filtered[EVENT_NAME_COL] == arm2_eos_event].copy()
        if PARTICIPANT_STATUS_COL in df_excel_eos_arm2.columns:
            status_counts_excel_arm2 = df_excel_eos_arm2[PARTICIPANT_STATUS_COL].dropna().astype(int).value_counts().sort_index()
            total_with_status_excel_arm2 = status_counts_excel_arm2.sum()
            if total_with_status_excel_arm2 > 0:
                for status, count in status_counts_excel_arm2.items():
                    percentage = (count / total_with_status_excel_arm2) * 100
                    print(f"      Status {status}: {count} ({percentage:.2f}%)")
                missing_status_excel_arm2 = df_excel_eos_arm2[PARTICIPANT_STATUS_COL].isnull().sum()
                if missing_status_excel_arm2 > 0: print(f"      Status Missing/Invalid: {missing_status_excel_arm2}")
            else: print("      No valid status values found for Arm 2.")
        else: print(f"    '{PARTICIPANT_STATUS_COL}' column not found for Arm 2.")
    else: print("    Arm 2 EOS event not defined.")


    # Individual CSV Baseline Counts
    print("\n--- Counts from Individual CSVs (Filtered Baseline) ---")
    # Group 1 CSV
    print(f"  {os.path.basename(group1_csv_path)} (Filtered Baseline):")
    if PARTICIPANT_STATUS_COL in df_group1_baseline.columns:
        status_counts_g1_csv = df_group1_baseline[PARTICIPANT_STATUS_COL].dropna().astype(int).value_counts().sort_index()
        total_with_status_g1_csv = status_counts_g1_csv.sum()
        if total_with_status_g1_csv > 0:
            for status, count in status_counts_g1_csv.items():
                percentage = (count / total_with_status_g1_csv) * 100
                print(f"    Status {status}: {count} ({percentage:.2f}%)")
            missing_status_g1_csv = df_group1_baseline[PARTICIPANT_STATUS_COL].isnull().sum()
            if missing_status_g1_csv > 0: print(f"    Status Missing/Invalid: {missing_status_g1_csv}")
        else: print(f"    No valid '{PARTICIPANT_STATUS_COL}' values found.")
    else: print(f"  '{PARTICIPANT_STATUS_COL}' column not found.")

    # Group 2 CSV
    print(f"\n  {os.path.basename(group2_csv_path)} (Filtered Baseline):")
    if PARTICIPANT_STATUS_COL in df_group2_baseline.columns:
        status_counts_g2_csv = df_group2_baseline[PARTICIPANT_STATUS_COL].dropna().astype(int).value_counts().sort_index()
        total_with_status_g2_csv = status_counts_g2_csv.sum()
        if total_with_status_g2_csv > 0:
            for status, count in status_counts_g2_csv.items():
                percentage = (count / total_with_status_g2_csv) * 100
                print(f"    Status {status}: {count} ({percentage:.2f}%)")
            missing_status_g2_csv = df_group2_baseline[PARTICIPANT_STATUS_COL].isnull().sum()
            if missing_status_g2_csv > 0: print(f"    Status Missing/Invalid: {missing_status_g2_csv}")
        else: print(f"    No valid '{PARTICIPANT_STATUS_COL}' values found.")
    else: print(f"  '{PARTICIPANT_STATUS_COL}' column not found.")


    # --- Report Discrepancies ---
    if discrepancies:
        print("\n=== DISCREPANCIES FOUND ===")
        discrepancies_df = pd.DataFrame(discrepancies)
        # Reorder columns for clarity
        discrepancy_cols_order = ['record_id', 'field', 'Source_CSV_Row', 'Source_Excel_Row', 'CSV_value', 'Excel_value', 'note']
        discrepancies_df = discrepancies_df[[col for col in discrepancy_cols_order if col in discrepancies_df.columns]]

        # Print to console (using to_string for full output)
        print(discrepancies_df.to_string(index=False))

        # Save discrepancies to CSV
        try:
            discrepancies_df.to_csv(DISCREPANCY_OUTPUT_FILE, index=False, encoding='utf-8')
            print(f"\nDetailed discrepancies saved to {DISCREPANCY_OUTPUT_FILE}")
        except Exception as e:
            print(f"\nError saving discrepancies to CSV: {e}", file=sys.stderr)
    else:
        print("\n✅ No discrepancies found based on the comparison criteria.")

    # --- Report Arm Classification Differences (Optional but potentially useful) ---
    # This logic assumes arm is strictly determined by the specific baseline/EOS event names provided
    if arm1_baseline_event and arm2_baseline_event and arm1_eos_event and arm2_eos_event:
        csv_arm1_ids = set(df_csv_baseline_filtered[df_csv_baseline_filtered[EVENT_NAME_COL] == arm1_baseline_event][ID_COL])
        csv_arm2_ids = set(df_csv_baseline_filtered[df_csv_baseline_filtered[EVENT_NAME_COL] == arm2_baseline_event][ID_COL])
        excel_arm1_ids = set(df_excel_eos_filtered[df_excel_eos_filtered[EVENT_NAME_COL] == arm1_eos_event][ID_COL])
        excel_arm2_ids = set(df_excel_eos_filtered[df_excel_eos_filtered[EVENT_NAME_COL] == arm2_eos_event][ID_COL])

        # IDs potentially switching arms (present in both sources but different arms)
        csv1_excel2 = list((csv_arm1_ids & excel_arm2_ids) - (csv_arm1_ids & excel_arm1_ids) - (csv_arm2_ids & excel_arm2_ids))
        csv2_excel1 = list((csv_arm2_ids & excel_arm1_ids) - (csv_arm1_ids & excel_arm1_ids) - (csv_arm2_ids & excel_arm2_ids))

        if csv1_excel2 or csv2_excel1:
            print("\n--- Potential Arm Classification Differences (CSV Baseline vs Excel EOS) ---")
            if csv1_excel2:
                print(f"  Record ID(s) appearing as Arm 1 in CSV Baseline but Arm 2 in Excel EOS: {', '.join(csv1_excel2)}")
            if csv2_excel1:
                print(f"  Record ID(s) appearing as Arm 2 in CSV Baseline but Arm 1 in Excel EOS: {', '.join(csv2_excel1)}")
        else:
            print("\n--- No apparent arm classification differences found between CSV Baseline and Excel EOS for records present in both. ---")


# --- Script Execution ---
if __name__ == "__main__":
    # Ensure configuration is set at the top before calling the function
    compare_data_sources(
        GROUP1_CSV_FILE,
        GROUP2_CSV_FILE,
        COMPARISON_EXCEL_FILE,
        EXCEL_SHEET_NAME,
        BASELINE_EVENT_NAMES,
        EOS_EVENT_NAMES,
        BASELINE_CSV_VS_EXCEL_BASE_COLS,
        CSV_BASELINE_VS_EXCEL_EOS_COLS
    )
    print("\n--- Script Finished ---")