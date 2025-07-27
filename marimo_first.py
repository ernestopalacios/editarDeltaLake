import marimo

__generated_with = "0.14.13"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Primer Marimo notebook

    Fecha: 26-julio-2025<br>Autor: Ernesto Palacios 🥇
    ## Objetivo

    - Cargar la base de datos desde DeltaLake
    - Seleccionar una rango de filas
    - Mostrar las filas en un dataframe
    """
    )
    return


@app.cell
def cargar_datos(load_dt_btn, mo):
    mo.md(
        f"""
    ## Cargar base de datos
    > Carga los datos actuales desde la base de datos: {"**Éxito**" if load_dt_btn.value > 0 else "Falta Cargar"}
    """
    )
    return


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    from deltalake import DeltaTable, write_deltalake
    from datetime import date, timedelta, datetime

    DELTA_TABLE_PATH = "/home/vlad/GIT/eerssa_gh/ordenes_de_trabajo/test/deltalake_2025"

    def load_delta_data():
        try:
            dt = DeltaTable(DELTA_TABLE_PATH)
            return dt.to_pandas()
        except Exception as e:
            mo.md(f"Error loading Delta table: {e}")
            return pd.DataFrame()

    # -----=   Date Picker   =---------
    # Set reasonable defaults (last 30 days)
    # Create date input widgets
    start_date = mo.ui.date(
        value=date.today()- timedelta(days=29),
        label="Fecha de Inicio"
    )

    end_date = mo.ui.date(
        value=date.today(),
        label="Fecha Final"
    )

    return (
        DELTA_TABLE_PATH,
        DeltaTable,
        end_date,
        load_delta_data,
        mo,
        start_date,
        write_deltalake,
    )


@app.cell
def _(end_date, mo, start_date):
    # 1. Check if the date range is valid
    if start_date.value <= end_date.value:
        # 2. Subtract the dates to get a 'timedelta' object
        delta = end_date.value - start_date.value
        # 3. Get the number of days and add 1 to include the last day
        num_days = delta.days + 1
        days_text = f"Se han seleccionado **{num_days}** dias ........"
    else:
        # Handle the case where the start date is after the end date
        days_text = "Invalid date range"

    # --- Display Output ---

    mo.hstack([start_date, end_date, mo.md(days_text)])
    return


@app.cell
def _(mo):
    load_dt_btn = mo.ui.run_button(label="Cargar Datos")
    load_dt_btn
    return (load_dt_btn,)


@app.cell
def _(load_delta_data, load_dt_btn, mo):
    if load_dt_btn.value:
        load_delta_data()
    else:
        mo.md(text="Primer paso, cargar la base de datos")
    return


@app.cell
def _(mo):
    mo.md(r"""## Escoger fecha y Cuadrilla de la cual desea cargar los datos para editar""")
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


@app.cell
def _(mo):
    mo.md(r"""## Mostrar la información""")
    return


@app.cell
def _():
    # Define the column and value to filter by
    column_to_filter = "id_ot"
    value_to_match = 156201

    return column_to_filter, value_to_match


@app.cell
def _(DELTA_TABLE_PATH, DeltaTable, column_to_filter, value_to_match):
    # Create a DeltaTable object
    dt = DeltaTable(DELTA_TABLE_PATH)

    # Define the filter condition as a list of tuples
    # The format is (column_name, operator, value)
    # Common operators include '==', '=', '!=', '<', '<=', '>', '>='
    filters = [(column_to_filter, "=", value_to_match)]

    # Read the filtered data into a Pandas DataFrame
    # The 'filters' argument pushes down the predicate to the Delta Lake storage
    filtered_df = dt.to_pandas(filters=filters)

    # Print the resulting DataFrame (optional)
    print(f"Successfully loaded {len(filtered_df)} rows matching '{column_to_filter}' = {value_to_match}:")
    return (filtered_df,)


@app.cell
def _(mo):
    mo.md(r"""## Revisar y editar tabla""")
    return


@app.cell
def _(filtered_df, mo):
    data_editor = mo.ui.data_editor(filtered_df)
    mo.md(f"**Editing {len(filtered_df)} filtered records:**")
    data_editor
    return (data_editor,)


@app.cell
def _(mo):
    # Cell 4 - Save changes back to Delta Lake
    if 'data_editor' in locals():
        save_button = mo.ui.button(label="Save to Delta Lake")
    return (save_button,)


@app.cell
def _(DELTA_TABLE_PATH, data_editor, save_button, write_deltalake):
    if save_button.value:
        try:
            edited_data = data_editor.value

            # Save back to Delta Lake
            write_deltalake(DELTA_TABLE_PATH, edited_data, mode='overwrite')
            print("✅ **Successfully saved changes to Delta Lake!**")
        except Exception as e:
            print(f"❌ **Error saving to Delta Lake:** {e}")

    save_button
    return


if __name__ == "__main__":
    app.run()
