import os
import pandas as pd
from py2neo import Graph, Node, Relationship

# Cartella in cui si trova lo script Python
script_directory = os.path.dirname(os.path.abspath(__file__))

# Connessione ai database Neo4j
graph100 = Graph("bolt://localhost:7687", user="neo4j", password="password", name="dataset100")
graph75 = Graph("bolt://localhost:7687", user="neo4j", password="password", name="dataset75")
graph50 = Graph("bolt://localhost:7687", user="neo4j", password="password", name="dataset50")
graph25 = Graph("bolt://localhost:7687", user="neo4j", password="password", name="dataset25")

# Dizionario per mappare le percentuali ai grafi
graphs_by_percentage = {
    100: graph100,
    75: graph75,
    50: graph50,
    25: graph25
}

# Tipi di dati
data_types = ['utenti', 'prodotti', 'transazioni']

for data_type in data_types:
    for percentage in graphs_by_percentage:
        csv_filename = f'{data_type}_{percentage}%.csv'

        # Percorso completo al file CSV
        csv_path = os.path.join(script_directory, f'{percentage}%', csv_filename)

        # Legge i dati dal file CSV utilizzando pandas
        data = pd.read_csv(csv_path, encoding='ISO-8859-1')

        # Ottiene il grafo corrispondente alla percentuale
        graph = graphs_by_percentage[percentage]

        # Inserisce i dati nel grafo
        for index, row in data.iterrows():
            node = Node(data_type, **row.to_dict())
            graph.create(node)

            if data_type == 'transazioni':
                # Crea relazione con utente
                user_id = row['user_id']
                user_node = graph.nodes.match('utenti', user_id=user_id).first()
                if user_node:
                    transaction_to_user = Relationship(user_node, 'EFFETTUATO_DA', node)
                    graph.create(transaction_to_user)

                # Crea relazione con prodotto
                product_id = row['product_id']
                product_node = graph.nodes.match('prodotti', product_id=product_id).first()
                if product_node:
                    transaction_to_product = Relationship(node, 'CONTIENE', product_node)
                    graph.create(transaction_to_product)

        print(f"Dati del dataset {data_type}_{percentage}% inseriti in Neo4j con successo.")

print("Inserimento completato per tutti i dataset.")