import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
from datetime import datetime, date
# You will need to have the deltalake library installed
# pip install deltalake
from deltalake import DeltaTable, write_deltalax

# --- Configuration ---
# TODO: Set the path to your Delta Lake table
DELTA_TABLE_PATH = "./dt" 

# --- Helper Functions for Delta Lake Interaction ---

def load_data_from_delta(start_date: date, end_date: date) -> pd.DataFrame:
    """
    Loads data from the Delta Lake table for a given date range.

    Args:
        start_date: The start of the date range.
        end_date: The end of the date range.

    Returns:
        A pandas DataFrame containing the filtered data.
    """
    try:
        # Convert dates to datetime objects for comparison
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())

        dt = DeltaTable(DELTA_TABLE_PATH)
        
        # Load the entire table and filter with pandas
        # For very large tables, consider using pyarrow filters for more efficiency
        # Ex: ds = dt.to_pyarrow_dataset(partitions=[("date", ">=", start_date.strftime('%Y-%m-%d'))])
        df = dt.to_pandas()

        # Ensure the date column is in datetime format for proper filtering
        # TODO: Replace 'your_date_column' with the actual name of your date column
        df['your_date_column'] = pd.to_datetime(df['your_date_column'])

        # Filter the DataFrame based on the selected date range
        mask = (df['your_date_column'] >= start_datetime) & (df['your_date_column'] <= end_datetime)
        filtered_df = df.loc[mask]
        
        return filtered_df

    except Exception as e:
        st.error(f"Failed to load data from Delta Lake: {e}")
        # On failure, return an empty DataFrame with a placeholder schema
        # TODO: Adjust the columns to match your actual schema
        return pd.DataFrame(columns=['id', 'your_date_column', 'data_field_1', 'editable_field_2'])


def save_data_to_delta(df_to_save: pd.DataFrame):
    """
    Saves the updated DataFrame back to the Delta Lake table using merge.

    Args:
        df_to_save: The DataFrame with the changes to be saved.
    """
    try:
        st.info(f"Saving {len(df_to_save)} updated records to Delta Lake...")
        
        # The 'merge' mode is crucial for updating existing records.
        # It will update rows that match the predicate and insert rows that do not.
        # TODO: Make sure 'id' is your unique identifier for each row.
        write_deltalax(
            DELTA_TABLE_PATH,
            df_to_save,
            mode='merge',
            merge_schema=True,
            predicate="s.id = t.id" # s = source (our df), t = target (delta table)
        )
        st.success("Changes successfully saved to Delta Lake!")

    except Exception as e:
        st.error(f"Failed to save data to Delta Lake: {e}")

# --- Streamlit App Layout ---

st.set_page_config(layout="wide")

st.title("Delta Lake Interactive Editor")
st.write(
    "This application allows you to load data from a Delta Lake table within a specific date range, "
    "edit the records in an interactive grid, and save the changes back to the Delta Lake."
)

# --- Initialize Session State ---
# Session state is used to store data between user interactions
if 'df' not in st.session_state:
    st.session_state.df = pd.DataFrame()
if 'original_df' not in st.session_state:
    st.session_state.original_df = pd.DataFrame()

# --- Sidebar for User Inputs ---
with st.sidebar:
    st.header("Data Selection")
    
    # Date input widgets
    today = date.today()
    start_date = st.date_input("Start Date", today)
    end_date = st.date_input("End Date", today)

    if st.button("Load Data", type="primary"):
        with st.spinner("Loading data from Delta Lake..."):
            # Load data and store it in the session state
            df = load_data_from_delta(start_date, end_date)
            st.session_state.df = df
            st.session_state.original_df = df.copy() # Keep a copy to compare changes
            st.success(f"Loaded {len(df)} records.")

# --- Main Area for Data Display and Interaction ---

if not st.session_state.df.empty:
    st.header("Editable Data Table")

    # --- AgGrid Configuration ---
    # Configure the grid to be editable
    gb = GridOptionsBuilder.from_dataframe(st.session_state.df)
    gb.configure_default_column(editable=True, groupable=True)
    
    # Make specific columns non-editable if needed
    # TODO: Adjust the column names as needed (e.g., 'id', 'your_date_column')
    gb.configure_column("id", editable=False)
    gb.configure_column("your_date_column", editable=False)
    
    # Enable selection to track changes
    gb.configure_selection(selection_mode="multiple", use_checkbox=True)
    gridOptions = gb.build()

    # --- Display the Grid ---
    grid_response = AgGrid(
        st.session_state.df,
        gridOptions=gridOptions,
        data_return_mode=DataReturnMode.AS_INPUT,
        update_mode=GridUpdateMode.MODEL_CHANGED,
        fit_columns_on_grid_load=True,
        allow_unsafe_jscode=True, # Set to True to allow jsfunction to be injected
        enable_enterprise_modules=False,
        height=500,
        width='100%',
        reload_data=False
    )

    # The grid_response dictionary contains the updated data
    updated_df = grid_response['data']

    # --- Save Changes Logic ---
    if st.button("Save Changes"):
        if not updated_df.equals(st.session_state.original_df):
            with st.spinner("Saving changes..."):
                # For simplicity, we save the entire updated dataframe.
                # A more advanced implementation could identify only the changed rows.
                save_data_to_delta(updated_df)
                # Refresh the state after saving
                st.session_state.df = updated_df
                st.session_state.original_df = updated_df.copy()
        else:
            st.info("No changes to save.")
else:
    st.info("Click 'Load Data' in the sidebar to get started.")

