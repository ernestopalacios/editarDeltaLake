import ibis
import duckdb

# Connect to DuckDB with delta extension
con = ibis.duckdb.connect()
con.sql("INSTALL delta")
con.sql("LOAD delta")

# Read Delta table directly with DuckDB/Ibis
delta_table = con.sql("SELECT * FROM delta_scan('/home/vlad/GIT/eerssa_gh/ordenes_de_trabajo/test/deltalake_2025')")

# Now use Ibis operations on the Delta table
result = (delta_table
    .filter(delta_table.column_name > 100)
    .group_by("category")
    .aggregate(total=delta_table.amount.sum())
)

result.execute()
