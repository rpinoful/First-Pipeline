# Obtendo dados de uma API

import pandas as pd
import requests
import json
import duckdb

# Importando dados desde uma API utilizando parâmetros e GET
url = "https://api.open-meteo.com/v1/forecast"

# Cidades da Europa
cities = [
    {"nombre": "Madrid", "lat": 40.4168, "lon": -3.7038, "hourly": "temperature_2m"},
    {"nombre": "Paris", "lat": 48.8566, "lon": 2.3522, "hourly": "temperature_2m"},
    {"nombre": "Berlin", "lat": 52.5200, "lon": 13.4050, "hourly": "temperature_2m"},
    {"nombre": "Roma", "lat": 41.9028, "lon": 12.4964, "hourly": "temperature_2m"},
    {"nombre": "Lisboa", "lat": 38.7169, "lon": -9.1390, "hourly": "temperature_2m"},
    {"nombre": "Londres", "lat": 51.5074, "lon": -0.1278, "hourly": "temperature_2m"},
    {"nombre": "Viena", "lat": 48.2100, "lon": 16.3738, "hourly": "temperature_2m"},
    {"nombre": "Bruselas", "lat": 50.8503, "lon": 4.3517, "hourly": "temperature_2m"},
    {"nombre": "Praga", "lat": 50.0755, "lon": 14.4378, "hourly": "temperature_2m"},
    {"nombre": "Amsterdam", "lat": 52.3676, "lon": 4.9041, "hourly": "temperature_2m"}
]

# Loop para obter dados de cada cidade: lat, lon, hourly
data_frames = []
for city in cities:
    #passando os parametros para a API a partir do dicionario cities
    params = {
        "latitude": city["lat"], 
        "longitude": city["lon"], 
        "hourly": city["hourly"], 
    }
    #fazendo a requisição GET
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()

        # Extrai horários e temperaturas
        horas = data["hourly"]["time"] # extrai a lista de horas usando a chave "time"
        temperaturas = data["hourly"]["temperature_2m"]

        # Cria o DataFrame para cada cidade
        df = pd.DataFrame({
            "city": city["nombre"],
            "hour": horas,
            "temperature_2m": temperaturas,
        })
        # Adiciona o DataFrame à lista
        data_frames.append(df)
    else:
        print(f"Erro na solicitação para {city['nombre']}: {response.status_code}")
        continue
    
# Concatenando todos os DataFrames em um só
final_df = pd.concat(data_frames, ignore_index=True)

# limpando e organizando dados
final_df["hour"] = pd.to_datetime(final_df["hour"])
final_df["date"] = final_df["hour"].dt.date
final_df["time"] = final_df["hour"].dt.time
final_df["year"] = final_df["hour"].dt.year
final_df["month"] = final_df["hour"].dt.month

# Deletando coluna hour
final_df = final_df.drop(columns=["hour"])

# Salvando o arquivo em parquet
final_df.to_parquet("weather_data.parquet", index=False)


display(final_df)

