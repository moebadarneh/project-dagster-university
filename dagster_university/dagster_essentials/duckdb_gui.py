import duckdb

# Load the GUI extension and start the GUI server
con = duckdb.connect()
con.sql("INSTALL gui; LOAD gui;")
con.sql("CALL gui();")
