# Issues Found in app_vGemini.py and Their Fixes

# ===== ISSUE 1: Incorrect import =====
# PROBLEM: write_deltalax doesn't exist
# WRONG:
from deltalake import DeltaTable, write_deltalax

# CORRECT:
from deltalake import DeltaTable, write_deltalake
# OR use the newer write_deltalake function

# ===== ISSUE 2: Incorrect merge operation =====
# PROBLEM: write_deltalax with merge mode doesn't work as expected
# The deltalake library doesn't support direct merge operations like this

# WRONG:
def save_data_to_delta(df_to_save: pd.DataFrame):
    write_deltalax(  # Function doesn't exist
        DELTA_TABLE_PATH,
        df_to_save,
        mode='merge',  # 'merge' mode doesn't work this way in deltalake
        merge_schema=True,
        predicate="s.id = t.id"
    )

# CORRECT APPROACH 1: Using write_deltalake (overwrite entire table)
def save_data_to_delta_overwrite(df_to_save: pd.DataFrame):
    """Save data by overwriting the entire table (simple but not efficient for large tables)"""
    try:
        st.info(f"Saving {len(df_to_save)} records to Delta Lake...")
        
        write_deltalake(
            DELTA_TABLE_PATH,
            df_to_save,
            mode='overwrite'  # This will replace the entire table
        )
        st.success("Changes successfully saved to Delta Lake!")
    except Exception as e:
        st.error(f"Failed to save data to Delta Lake: {e}")

# CORRECT APPROACH 2: Manual merge logic
def save_data_to_delta_merge(df_to_save: pd.DataFrame, original_df: pd.DataFrame):
    """Identify changes and perform manual merge"""
    try:
        # Identify changed rows
        changed_rows = identify_changed_rows(original_df, df_to_save)
        
        if changed_rows.empty:
            st.info("No changes detected.")
            return
            
        st.info(f"Saving {len(changed_rows)} changed records to Delta Lake...")
        
        # Load current table
        dt = DeltaTable(DELTA_TABLE_PATH)
        current_df = dt.to_pandas()
        
        # Update the changed rows in current_df
        for idx, row in changed_rows.iterrows():
            # Assuming 'id' is the primary key
            mask = current_df['id'] == row['id']
            for col in changed_rows.columns:
                current_df.loc[mask, col] = row[col]
        
        # Write back the entire updated dataframe
        write_deltalake(
            DELTA_TABLE_PATH,
            current_df,
            mode='overwrite'
        )
        
        st.success("Changes successfully saved to Delta Lake!")
    except Exception as e:
        st.error(f"Failed to save data to Delta Lake: {e}")

def identify_changed_rows(original_df: pd.DataFrame, updated_df: pd.DataFrame) -> pd.DataFrame:
    """Identify rows that have been changed"""
    # Compare dataframes and return only changed rows
    if original_df.equals(updated_df):
        return pd.DataFrame()
    
    # Simple approach: find rows where any column has changed
    changed_mask = ~original_df.eq(updated_df).all(axis=1)
    return updated_df[changed_mask]

# ===== ISSUE 3: Hardcoded column names =====
# PROBLEM: Code assumes specific column names that may not exist

# WRONG:
def load_data_from_delta(start_date: date, end_date: date) -> pd.DataFrame:
    df['your_date_column'] = pd.to_datetime(df['your_date_column'])  # Column may not exist
    mask = (df['your_date_column'] >= start_datetime) & (df['your_date_column'] <= end_datetime)

# CORRECT: Dynamic column detection
def load_data_from_delta_fixed(start_date: date, end_date: date, date_column: str = None) -> pd.DataFrame:
    """Load data with proper date column handling"""
    try:
        dt = DeltaTable(DELTA_TABLE_PATH)
        df = dt.to_pandas()
        
        if df.empty:
            st.warning("Delta table is empty")
            return df
            
        # If no date column specified, try to detect one
        if date_column is None:
            date_columns = []
            for col in df.columns:
                if 'date' in col.lower() or 'time' in col.lower():
                    try:
                        pd.to_datetime(df[col].dropna().iloc[0])
                        date_columns.append(col)
                    except:
                        pass
            
            if date_columns:
                date_column = date_columns[0]
                st.info(f"Using date column: {date_column}")
            else:
                st.warning("No date column found, returning all data")
                return df
        
        # Check if date column exists
        if date_column not in df.columns:
            st.error(f"Date column '{date_column}' not found in table")
            return df
            
        # Convert to datetime and filter
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())
        
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
        
        # Remove rows where date conversion failed
        df = df.dropna(subset=[date_column])
        
        # Filter by date range
        mask = (df[date_column] >= start_datetime) & (df[date_column] <= end_datetime)
        filtered_df = df.loc[mask]
        
        return filtered_df
        
    except Exception as e:
        st.error(f"Failed to load data from Delta Lake: {e}")
        return pd.DataFrame()

# ===== ISSUE 4: No error handling for empty/missing table =====
# PROBLEM: Code doesn't handle case where Delta table doesn't exist

# CORRECT: Add table existence check
def check_delta_table_exists(table_path: str) -> bool:
    """Check if Delta table exists"""
    try:
        dt = DeltaTable(table_path)
        return True
    except Exception:
        return False

# ===== ISSUE 5: Session state not properly managed =====
# PROBLEM: Session state could get out of sync

# CORRECT: Better session state management
def reset_session_state():
    """Reset session state when loading new data"""
    st.session_state.df = pd.DataFrame()
    st.session_state.original_df = pd.DataFrame()
    st.session_state.has_changes = False

# ===== ISSUE 6: AgGrid configuration issues =====
# PROBLEM: Configuration may not work with all data types

# CORRECT: Improved AgGrid configuration
def create_aggrid_config(df: pd.DataFrame, non_editable_columns: list = None):
    """Create AgGrid configuration with proper handling"""
    gb = GridOptionsBuilder.from_dataframe(df)
    
    # Configure default column behavior
    gb.configure_default_column(
        editable=True,
        groupable=True,
        value=True,
        enableRowGroup=True,
        sortable=True,
        filter=True,
        resizable=True
    )
    
    # Make specific columns non-editable
    if non_editable_columns:
        for col in non_editable_columns:
            if col in df.columns:
                gb.configure_column(col, editable=False)
    
    # Configure selection
    gb.configure_selection("multiple", use_checkbox=True)
    
    # Configure pagination for large datasets
    gb.configure_pagination(paginationAutoPageSize=True)
    gb.configure_side_bar()
    
    return gb.build()

# ===== COMPLETE FIXED VERSION =====
import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
from datetime import datetime, date
from deltalake import DeltaTable, write_deltalake  # FIXED: Correct import
import os

# Configuration
DELTA_TABLE_PATH = "./dt"

def check_delta_table_exists(table_path: str) -> bool:
    """Check if Delta table exists"""
    try:
        DeltaTable(table_path)
        return True
    except Exception:
        return False

def get_table_info(table_path: str) -> dict:
    """Get information about the Delta table"""
    try:
        dt = DeltaTable(table_path)
        df_sample = dt.to_pandas().head(1)  # Get just one row for schema
        
        # Detect date columns
        date_columns = []
        for col in df_sample.columns:
            if df_sample[col].dtype == 'datetime64[ns]' or 'date' in col.lower():
                date_columns.append(col)
        
        return {
            'columns': list(df_sample.columns),
            'date_columns': date_columns,
            'exists': True
        }
    except Exception as e:
        return {'exists': False, 'error': str(e)}

def load_data_from_delta(start_date: date, end_date: date, date_column: str = None) -> pd.DataFrame:
    """Load data from Delta table with date filtering"""
    try:
        dt = DeltaTable(DELTA_TABLE_PATH)
        df = dt.to_pandas()
        
        if df.empty:
            st.warning("Delta table is empty")
            return df
            
        if date_column and date_column in df.columns:
            start_datetime = datetime.combine(start_date, datetime.min.time())
            end_datetime = datetime.combine(end_date, datetime.max.time())
            
            df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
            df = df.dropna(subset=[date_column])
            
            mask = (df[date_column] >= start_datetime) & (df[date_column] <= end_datetime)
            df = df.loc[mask]
        
        return df
        
    except Exception as e:
        st.error(f"Failed to load data from Delta Lake: {e}")
        return pd.DataFrame()

def save_data_to_delta(df_to_save: pd.DataFrame):
    """Save data to Delta table using overwrite mode"""
    try:
        st.info(f"Saving {len(df_to_save)} records to Delta Lake...")
        
        write_deltalake(
            DELTA_TABLE_PATH,
            df_to_save,
            mode='overwrite'  # FIXED: Use correct mode
        )
        st.success("Changes successfully saved to Delta Lake!")
        
    except Exception as e:
        st.error(f"Failed to save data to Delta Lake: {e}")

# Streamlit App
st.set_page_config(layout="wide")
st.title("Delta Lake Interactive Editor - Fixed Version")

# Check if table exists
if not check_delta_table_exists(DELTA_TABLE_PATH):
    st.error(f"Delta table not found at: {DELTA_TABLE_PATH}")
    st.info("Please check the path or create the Delta table first.")
    st.stop()

# Get table info
table_info = get_table_info(DELTA_TABLE_PATH)
if not table_info['exists']:
    st.error(f"Error accessing table: {table_info['error']}")
    st.stop()

# Initialize session state
if 'df' not in st.session_state:
    st.session_state.df = pd.DataFrame()
if 'original_df' not in st.session_state:
    st.session_state.original_df = pd.DataFrame()

# Sidebar
with st.sidebar:
    st.header("Data Selection")
    
    # Show table info
    st.subheader("Table Info")
    st.write(f"Columns: {len(table_info['columns'])}")
    st.write(f"Date columns: {table_info['date_columns']}")
    
    # Date column selection
    date_column = None
    if table_info['date_columns']:
        date_column = st.selectbox(
            "Select date column for filtering:",
            options=['None'] + table_info['date_columns']
        )
        date_column = None if date_column == 'None' else date_column
    
    # Date inputs
    today = date.today()
    start_date = st.date_input("Start Date", today)
    end_date = st.date_input("End Date", today)
    
    if st.button("Load Data", type="primary"):
        with st.spinner("Loading data from Delta Lake..."):
            df = load_data_from_delta(start_date, end_date, date_column)
            st.session_state.df = df
            st.session_state.original_df = df.copy()
            st.success(f"Loaded {len(df)} records.")

# Main area
if not st.session_state.df.empty:
    st.header("Editable Data Table")
    
    # AgGrid configuration
    gb = GridOptionsBuilder.from_dataframe(st.session_state.df)
    gb.configure_default_column(editable=True, groupable=True)
    
    # Make ID columns non-editable (if they exist)
    for col in ['id', 'ID', 'Id']:
        if col in st.session_state.df.columns:
            gb.configure_column(col, editable=False)
    
    gb.configure_selection(selection_mode="multiple", use_checkbox=True)
    gridOptions = gb.build()
    
    # Display grid
    grid_response = AgGrid(
        st.session_state.df,
        gridOptions=gridOptions,
        data_return_mode=DataReturnMode.AS_INPUT,
        update_mode=GridUpdateMode.MODEL_CHANGED,
        fit_columns_on_grid_load=True,
        height=500,
        width='100%'
    )
    
    updated_df = grid_response['data']
    
    # Show changes
    if not updated_df.equals(st.session_state.original_df):
        st.warning("⚠️ You have unsaved changes!")
        
        # Show what changed
        changes = []
        for idx in updated_df.index:
            for col in updated_df.columns:
                if str(updated_df.loc[idx, col]) != str(st.session_state.original_df.loc[idx, col]):
                    changes.append({
                        'Row': idx,
                        'Column': col,
                        'Old Value': st.session_state.original_df.loc[idx, col],
                        'New Value': updated_df.loc[idx, col]
                    })
        
        if changes:
            st.subheader("Changes Preview:")
            st.dataframe(pd.DataFrame(changes))
    
    # Save button
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Save Changes", type="primary"):
            if not updated_df.equals(st.session_state.original_df):
                save_data_to_delta(updated_df)
                st.session_state.df = updated_df
                st.session_state.original_df = updated_df.copy()
            else:
                st.info("No changes to save.")
    
    with col2:
        if st.button("Discard Changes"):
            st.session_state.df = st.session_state.original_df.copy()
            st.experimental_rerun()

else:
    st.info("Click 'Load Data' in the sidebar to get started.")
    
    # Show sample of what the table contains
    with st.expander("Table Preview"):
        try:
            dt = DeltaTable(DELTA_TABLE_PATH)
            sample_df = dt.to_pandas().head(5)
            st.dataframe(sample_df)
        except Exception as e:
            st.error(f"Could not load table preview: {e}")