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

    def borra_time_zone( fecha ):
      """
      Esta función elimina el componenete de Time Zone y deja solamente la fecha y hora. 
      En caso de que no contenga este componente deja el String intacto. 
      """
      if not isinstance( fecha, str ):
        return pd.NaT
  
      if len(fecha) < 4:
        return pd.NaT

      fecha_inicio = fecha.replace('T', ' ').split()
      fecha_inicio = fecha_inicio[0]+' '+fecha_inicio[-1]
      return fecha_inicio

    cuadrilla_sel = {"Seleccionar Cuadrilla":"sin_seleccion"}

    # Carga y transformacion del DataFrame completo:

    df = DeltaTable(DELTA_TABLE_PATH).to_pandas()

    df['Fecha'] = df['Fecha'].apply( lambda x: borra_time_zone(x))
    df['InicioEvento'] = df['InicioEvento'].apply( lambda x: borra_time_zone(x))
    df['FinEvento'] = df['FinEvento'].apply( lambda x: borra_time_zone(x))

    df["Fecha"] = pd.to_datetime(df["Fecha"]).apply(lambda x: x.date())
    df["InicioEvento"] = pd.to_datetime(df["InicioEvento"])  #, format='mixed'
    df["FinEvento"] = pd.to_datetime(df["FinEvento"])

    df['Cuenta']    = df['Cuenta'].astype('category')
    df['Actividad'] = df['Actividad'].fillna('·').astype('category')



    return DELTA_TABLE_PATH, df, load_delta_data, mo, write_deltalake


@app.cell
def _(mo):
    # Create a form with multiple elements
    formDates = (
        mo.md('''
        **Seleccionar Cuadrilla y Fechas**

        {grupo}

        {inicio}

        {fin}
    ''')
        .batch(
            inicio=mo.ui.date(label="Desde"),
            fin   =mo.ui.date(label="Hasta"),
            grupo=mo.ui.dropdown(
                options={
                    "= Seleccionar grupo ="   : "sin_grupo",
                    "Cuadrilla Zamora"    : "Zamora Z1 (Cuadrilla. Nro. 6",
                    "Alumbrado Zamora"    : "Zamora Z1 (Cuadrilla. AP Nro. 4)",
                    "Cuadrilla Yacuambi"  : "Yacuambi Z1 (Cuadrilla. Nro. 8)",
                    "Cuadrilla Yantzaza"  : "Yantzaza Z1 (Cuadrilla. Nro. 5)",
                    "Energizados Yantzaza": "Líneas Energizadas (Cuadrilla  Nro.6)",
                    "Cuadrilla Paquisha"  : "Paquisha Z1 (Cuadrilla Nro. 10)",
                    "Cuadrilla Guayzimi"  : "Guayzimi Z1 (Cuadrilla. Nro. 7)",
                    "Cuadrilla El Pangui" : "El Pangui Z1 (Cuadrilla. Nro. 4)",
                    "Cuadrilla Gualaquiza": "Gualaquiza Z1 (Cuadrilla. Nro. 3)",
                    "Agencia Zamora"   : "Zamora (Agencia)",
                    "Agencia Yantzaza" : "Yanzatza (Agencia)",
                    "Agencia Pangui"   : "El Pangui (Agencia)",
                    "Agencia Gualaquiza": "Gualaquiza (Agencia)",

                },
                value="= Seleccionar grupo =",
                label="Escoger grupo de trabajo"
                ),

        )
        .form(show_clear_button=True, bordered=False)
    )
    return (formDates,)


@app.cell
def _(formDates):
    formDates
    return


@app.cell
def _(formDates):
    formDates.value["inicio"]
    return


@app.cell
def _(mo):
    load_dt_btn = mo.ui.run_button(label="Cargar Datos")
    load_dt_btn
    return (load_dt_btn,)


@app.cell
def _(load_delta_data, load_dt_btn, mo):
    if load_dt_btn.value:
        df_glob = load_delta_data()
        print(df_glob.info())
    else:
        mo.md(text="Primer paso, cargar la base de datos")
    return


@app.cell
def _(mo):
    mo.md(r"""## Escoger fecha y Cuadrilla de la cual desea cargar los datos para editar""")
    return


@app.cell
def _(df, formDates):
    desdeFecha = formDates.value["inicio"]
    hastaFecha = formDates.value["fin"]
    filtered_df = df.query("@desdeFecha <= Fecha <= @hastaFecha")
    return (filtered_df,)


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
