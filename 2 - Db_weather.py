import duckdb
import pandas as pd

final_df = pd.read_parquet("weather_data.parquet")
final_df.to_parquet("weather_data.parquet", index=False)

# Save database on disk instead of in-memory
con = duckdb.connect("weather_data.duckdb")
con.execute("CREATE OR REPLACE TABLE weather_data AS SELECT * FROM final_df")
con.close()

#modeling with duckdb , creating dimensions city
con = duckdb.connect("weather_data.duckdb")
con.execute("""
CREATE OR REPLACE TABLE dim_city AS
    SELECT DISTINCT city
    FROM weather_data
""")
con.close()

#creating fact table
con = duckdb.connect("weather_data.duckdb")
con.execute("""
CREATE OR REPLACE TABLE fact_weather AS
    SELECT wd.*, dc.rowid AS city_id
    FROM weather_data wd
    JOIN dim_city dc ON wd.city = dc.city
""")
con.close()

#dropping city column from fact table
con = duckdb.connect("weather_data.duckdb")
con.execute("""
ALTER TABLE fact_weather DROP COLUMN city
""")
con.close()

#creating index on fact table
con = duckdb.connect("weather_data.duckdb")
con.execute("""
CREATE INDEX idx_city_id ON fact_weather(city_id)
""")
con.close()

#querying fact table
con = duckdb.connect("weather_data.duckdb")
result = con.execute("SELECT * FROM fact_weather LIMIT 10").fetchdf()
print(result)
con.close()



con = duckdb.connect("weather_data.duckdb")
tables = con.execute("SHOW TABLES").fetchdf()
print(tables)


# Exportando todas as tabelas para parquet na pasta weather_tables
import os
os.makedirs("weather_tables", exist_ok=True) # criando a pasta weather_tables se nao existir

tabelas = tables["name"].tolist()  # pega o nome das tabelas criadas e coloca em uma lista

for tabela in tabelas: # percorre a lista de tabelas
    df = con.execute(f"SELECT * FROM {tabela}").fetchdf()
    df.to_parquet(f"weather_tables/{tabela}.parquet", index=False)

con.close()

#modificando o arquivo parquet dim_city adicionando uma coluna id , a traves do duckdb


