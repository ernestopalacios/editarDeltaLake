import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode, JsCode
from st_aggrid.shared import GridUpdateMode, DataReturnMode
import delta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit
from pyspark.sql.types import *
from datetime import datetime, date, timedelta
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure Streamlit page
st.set_page_config(
    page_title="Delta Lake Data Editor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_resource
def get_spark_session():
    """Create and return a Spark session configured for Delta Lake"""
    try:
        spark = (SparkSession.builder
                .appName("DeltaLakeEditor")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate())
        
        spark.sparkContext.setLogLevel("WARN")
        return spark
    except Exception as e:
        st.error(f"Failed to create Spark session: {str(e)}")
        return None

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_delta_table_info(_spark, table_path):
    """Load basic information about the Delta table"""
    try:
        # Read the Delta table
        df = _spark.read.format("delta").load(table_path)
        
        # Get schema information
        schema = df.schema
        columns = [field.name for field in schema.fields]
        
        # Get row count (approximate for large tables)
        row_count = df.count()
        
        # Detect date columns
        date_columns = [field.name for field in schema.fields 
                       if field.dataType in [DateType(), TimestampType()] 
                       or 'date' in field.name.lower() 
                       or 'time' in field.name.lower()]
        
        return {
            'columns': columns,
            'row_count': row_count,
            'date_columns': date_columns,
            'schema': schema
        }
    except Exception as e:
        st.error(f"Error loading table info: {str(e)}")
        return None

def load_data_with_date_filter(spark, table_path, date_column, start_date, end_date, limit=1000):
    """Load data from Delta table with date filtering"""
    try:
        df = spark.read.format("delta").load(table_path)
        
        # Apply date filter if specified
        if date_column and start_date and end_date:
            df = df.filter(
                (col(date_column) >= lit(start_date.strftime('%Y-%m-%d'))) &
                (col(date_column) <= lit(end_date.strftime('%Y-%m-%d')))
            )
        
        # Limit results for performance
        df = df.limit(limit)
        
        # Convert to Pandas
        pandas_df = df.toPandas()
        
        # Convert date columns to string for AgGrid compatibility
        for col_name in pandas_df.columns:
            if pandas_df[col_name].dtype == 'datetime64[ns]':
                pandas_df[col_name] = pandas_df[col_name].dt.strftime('%Y-%m-%d %H:%M:%S')
            elif pandas_df[col_name].dtype == 'object':
                # Try to convert date strings to consistent format
                try:
                    pandas_df[col_name] = pd.to_datetime(pandas_df[col_name], errors='ignore')
                    if pandas_df[col_name].dtype == 'datetime64[ns]':
                        pandas_df[col_name] = pandas_df[col_name].dt.strftime('%Y-%m-%d %H:%M:%S')
                except:
                    pass
        
        return pandas_df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame()

def create_aggrid_options(df, editable_columns=None):
    """Create AgGrid options for data editing"""
    gb = GridOptionsBuilder.from_dataframe(df)
    
    # Configure grid options
    gb.configure_pagination(paginationAutoPageSize=True)
    gb.configure_side_bar()
    gb.configure_selection('multiple', use_checkbox=True, groupSelectsChildren=True, groupSelectsFiltered=True)
    gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, aggFunc='sum', editable=True)
    
    # Configure columns
    if editable_columns:
        for col in df.columns:
            if col in editable_columns:
                gb.configure_column(col, editable=True, cellEditor='agTextCellEditor')
            else:
                gb.configure_column(col, editable=False)
    
    # Configure grid update mode
    gb.configure_grid_options(
        enableRangeSelection=True,
        enableClipboard=True,
        suppressRowClickSelection=True,
        rowSelection='multiple'
    )
    
    return gb.build()

def save_changes_to_delta(spark, table_path, original_df, modified_df, key_columns):
    """Save changes back to Delta Lake using merge operation"""
    try:
        # Identify changed rows
        changes_df = identify_changes(original_df, modified_df, key_columns)
        
        if changes_df.empty:
            st.info("No changes detected to save.")
            return True
        
        # Convert pandas DataFrame to Spark DataFrame
        spark_changes_df = spark.createDataFrame(changes_df)
        
        # Create temporary view for merge operation
        spark_changes_df.createOrReplaceTempView("changes_temp")
        
        # Build merge condition
        merge_conditions = " AND ".join([f"target.{col} = source.{col}" for col in key_columns])
        
        # Build update set clause
        update_columns = [col for col in changes_df.columns if col not in key_columns]
        update_set = ", ".join([f"{col} = source.{col}" for col in update_columns])
        
        # Execute merge operation
        merge_sql = f"""
        MERGE INTO delta.`{table_path}` AS target
        USING changes_temp AS source
        ON {merge_conditions}
        WHEN MATCHED THEN
          UPDATE SET {update_set}
        """
        
        spark.sql(merge_sql)
        
        st.success(f"Successfully saved {len(changes_df)} changes to Delta Lake!")
        return True
        
    except Exception as e:
        st.error(f"Error saving changes: {str(e)}")
        logger.error(f"Save error: {str(e)}")
        return False

def identify_changes(original_df, modified_df, key_columns):
    """Identify rows that have been modified"""
    try:
        # Ensure both DataFrames have the same columns and order
        original_df = original_df.copy()
        modified_df = modified_df.copy()
        
        # Create comparison DataFrames
        original_df['_row_id'] = range(len(original_df))
        modified_df['_row_id'] = range(len(modified_df))
        
        # Find differences
        changes = []
        for idx in range(len(original_df)):
            if idx < len(modified_df):
                original_row = original_df.iloc[idx]
                modified_row = modified_df.iloc[idx]
                
                # Check if any non-key columns have changed
                has_changes = False
                for col in original_df.columns:
                    if col not in key_columns and col != '_row_id':
                        if str(original_row[col]) != str(modified_row[col]):
                            has_changes = True
                            break
                
                if has_changes:
                    # Include the key columns and changed data
                    change_row = {}
                    for col in modified_df.columns:
                        if col != '_row_id':
                            change_row[col] = modified_row[col]
                    changes.append(change_row)
        
        return pd.DataFrame(changes)
        
    except Exception as e:
        st.error(f"Error identifying changes: {str(e)}")
        return pd.DataFrame()

def main():
    """Main Streamlit application"""
    st.title("📊 Delta Lake Data Editor")
    st.markdown("Edit your Delta Lake data with date filtering and real-time updates")
    
    # Initialize session state
    if 'original_data' not in st.session_state:
        st.session_state.original_data = pd.DataFrame()
    if 'table_info' not in st.session_state:
        st.session_state.table_info = None
    
    # Sidebar configuration
    st.sidebar.header("🔧 Configuration")
    
    # Table path input
    table_path = st.sidebar.text_input(
        "Delta Table Path",
        value="/path/to/your/delta/table",
        placeholder="s3://bucket/path/to/table or /local/path/to/table",
        help="Enter the path to your Delta Lake table"
    )
    
    # Initialize Spark session
    spark = get_spark_session()
    if not spark:
        st.error("Cannot proceed without Spark session")
        return
    
    # Load table information
    if st.sidebar.button("🔍 Load Table Info") or st.session_state.table_info is None:
        with st.spinner("Loading table information..."):
            st.session_state.table_info = load_delta_table_info(spark, table_path)
    
    if not st.session_state.table_info:
        st.warning("Please enter a valid Delta table path and click 'Load Table Info'")
        return
    
    # Display table information
    with st.expander("📋 Table Information", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Columns:** {len(st.session_state.table_info['columns'])}")
            st.write(f"**Row Count:** {st.session_state.table_info['row_count']:,}")
        with col2:
            st.write(f"**Date Columns:** {', '.join(st.session_state.table_info['date_columns'])}")
    
    # Date filtering configuration
    st.sidebar.subheader("📅 Date Filtering")
    
    date_column = st.sidebar.selectbox(
        "Date Column for Filtering",
        options=['None'] + st.session_state.table_info['date_columns'],
        index=0,
        help="Select a date column to filter data"
    )
    
    date_column = None if date_column == 'None' else date_column
    
    # Date range selection
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date",
            value=date.today() - timedelta(days=30),
            help="Start date for data filtering"
        )
    with col2:
        end_date = st.date_input(
            "End Date",
            value=date.today(),
            help="End date for data filtering"
        )
    
    # Data loading configuration
    st.sidebar.subheader("⚙️ Loading Options")
    row_limit = st.sidebar.slider(
        "Row Limit",
        min_value=100,
        max_value=10000,
        value=1000,
        step=100,
        help="Maximum number of rows to load"
    )
    
    # Load data button
    if st.sidebar.button("📥 Load Data", type="primary"):
        with st.spinner("Loading data from Delta Lake..."):
            data = load_data_with_date_filter(
                spark, table_path, date_column, start_date, end_date, row_limit
            )
            st.session_state.original_data = data.copy()
            st.session_state.current_data = data.copy()
    
    # Main content area
    if not st.session_state.original_data.empty:
        st.subheader(f"📊 Data Editor ({len(st.session_state.original_data)} rows loaded)")
        
        # Column selection for editing
        editable_columns = st.multiselect(
            "Select columns to make editable:",
            options=list(st.session_state.original_data.columns),
            default=list(st.session_state.original_data.columns),
            help="Choose which columns can be edited in the grid"
        )
        
        # Key columns for merge operation
        key_columns = st.multiselect(
            "Select key columns for updates:",
            options=list(st.session_state.original_data.columns),
            default=[st.session_state.original_data.columns[0]] if len(st.session_state.original_data.columns) > 0 else [],
            help="These columns will be used to identify rows when saving changes"
        )
        
        # Create and display AgGrid
        grid_options = create_aggrid_options(st.session_state.original_data, editable_columns)
        
        grid_response = AgGrid(
            st.session_state.original_data,
            gridOptions=grid_options,
            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
            update_mode=GridUpdateMode.MODEL_CHANGED,
            fit_columns_on_grid_load=True,
            enable_enterprise_modules=False,
            height=600,
            width='100%',
            reload_data=False
        )
        
        # Get modified data
        modified_data = grid_response['data']
        
        # Display change summary
        if not modified_data.equals(st.session_state.original_data):
            changes_df = identify_changes(st.session_state.original_data, modified_data, key_columns)
            if not changes_df.empty:
                st.subheader("📝 Pending Changes")
                st.write(f"**{len(changes_df)} rows** have been modified:")
                st.dataframe(changes_df, use_container_width=True)
                
                # Save changes button
                col1, col2, col3 = st.columns([1, 1, 2])
                with col1:
                    if st.button("💾 Save Changes", type="primary"):
                        if not key_columns:
                            st.error("Please select at least one key column for updates")
                        else:
                            success = save_changes_to_delta(
                                spark, table_path, st.session_state.original_data, 
                                modified_data, key_columns
                            )
                            if success:
                                st.session_state.original_data = modified_data.copy()
                                st.experimental_rerun()
                
                with col2:
                    if st.button("🔄 Discard Changes"):
                        st.experimental_rerun()
        
        # Export functionality
        st.subheader("📤 Export Options")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            csv = modified_data.to_csv(index=False)
            st.download_button(
                label="📄 Download CSV",
                data=csv,
                file_name=f"delta_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        with col2:
            excel_buffer = modified_data.to_excel(index=False, engine='openpyxl')
            st.download_button(
                label="📊 Download Excel",
                data=excel_buffer,
                file_name=f"delta_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        with col3:
            json_data = modified_data.to_json(orient='records', indent=2)
            st.download_button(
                label="📋 Download JSON",
                data=json_data,
                file_name=f"delta_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
        
        # Statistics
        with st.expander("📊 Data Statistics", expanded=False):
            st.write("**Numeric Columns Summary:**")
            numeric_cols = modified_data.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 0:
                st.dataframe(modified_data[numeric_cols].describe())
            else:
                st.write("No numeric columns found")
    
    else:
        st.info("👆 Configure your Delta table path and click 'Load Data' to begin editing")
        
        # Sample configuration help
        with st.expander("💡 Quick Start Guide", expanded=True):
            st.markdown("""
            ### Getting Started:
            1. **Enter your Delta table path** in the sidebar (e.g., `/path/to/delta/table`)
            2. **Click 'Load Table Info'** to discover the table structure
            3. **Configure date filtering** if your table has date columns
            4. **Set row limit** based on your needs (start with 1000 for testing)
            5. **Click 'Load Data'** to fetch and display your data
            6. **Select editable columns** and **key columns** for updates
            7. **Edit data directly** in the grid
            8. **Save changes** back to Delta Lake
            
            ### Features:
            - ✅ Date range filtering
            - ✅ Real-time data editing
            - ✅ Change tracking and preview
            - ✅ Bulk operations
            - ✅ Export to CSV/Excel/JSON
            - ✅ Column sorting and filtering
            """)

if __name__ == "__main__":
    main()