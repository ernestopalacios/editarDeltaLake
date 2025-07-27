import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
from datetime import datetime, date
from deltalake import DeltaTable, write_deltalake  # FIXED: Correct import
import os

# Configuration
DELTA_TABLE_PATH = "/home/vlad/GIT/eerssa_gh/ordenes_de_trabajo/test/deltalake_2025"

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
