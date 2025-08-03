import marimo

__generated_with = "0.14.13"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    from datetime import datetime, date, timedelta

    # Create date input widgets
    start_date = mo.ui.date(
        value=date.today()- timedelta(days=29),
        label="Fecha de Inicio"
    )

    end_date = mo.ui.date(
        value=date.today(),
        label="Fecha Final"
    )



    return end_date, mo, start_date


@app.cell(hide_code=True)
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
    drop = mo.ui.dropdown(
            options={
                "Last 7 Days": "last_7",
                "Last 30 Days": "last_30",
                "This Month": "this_month",
                "Year to Date": "year_to_date",
                "Custom": "custom",
            },
            value="Custom",
            label="Quick Select"
        )
    drop
    return (drop,)


@app.cell
def _(drop, mo):
    mo.md(f"""Valor seleccionado: {drop.value}""")
    return


@app.cell
def _(mo):
    # Create a form with multiple elements
    form = (
        mo.md('''
        **Your form.**

        {name}

        {date}
    ''')
        .batch(
            date=mo.ui.date(label="date"),
            name=mo.ui.dropdown(
                options={
                    "Last 7 Days": "last_7",
                    "Last 30 Days": "last_30",
                    "This Month": "this_month",
                    "Year to Date": "year_to_date",
                    "Custom": "custom",
                },
                value="Custom",
                label="Quick Select"
                ),

        )
        .form(show_clear_button=True, bordered=False)
    )
    return (form,)


@app.cell
def _(form):
    # Instantiate a form directly
    form
    return


@app.cell
def _(form):
    print(form.value['name'])
    return


if __name__ == "__main__":
    app.run()
