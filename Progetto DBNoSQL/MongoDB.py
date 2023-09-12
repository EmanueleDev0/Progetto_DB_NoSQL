import os
import pandas as pd
from pymongo import MongoClient

# Cartella in cui si trova lo script Python
script_directory = os.path.dirname(os.path.abspath(__file__))

# Connessione al database MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['frodi_ecommerce'] 

# Percentuali dei dataset rispetto al 100%
percentages = [100, 75, 50, 25]

# Tipi di dati
data_types = ['transazioni', 'utenti', 'prodotti']

for data_type in data_types:
    for percentage in percentages:
        collection_name = f'{data_type}{percentage}%'
        csv_filename = f'{data_type}_{percentage}%.csv'

        # Percorso completo al file CSV
        csv_path = os.path.join(script_directory, f'{percentage}%', csv_filename)

        # Legge i dati dal file CSV utilizzando pandas
        data = pd.read_csv(csv_path, encoding='ISO-8859-1')

        # Converte i dati in formato JSON
        data_json = data.to_dict(orient='records')

        # Inserisce i dati nella collezione del database
        collection = db[collection_name]
        collection.insert_many(data_json)

        print(f"Dati del dataset {collection_name} inseriti in MongoDB con successo.")

print("Inserimento completato per tutti i dataset.")