from py2neo import Graph
import time
import csv
from scipy.stats import t
import numpy as np

percentages = [100, 75, 50, 25]

queries = [
    """
    MATCH (p:prodotti {name: 'macchina fotografica'}) RETURN p, p.product_id as product_id
    """,
    """
    MATCH (p:prodotti) WHERE p.name = 'macchina fotografica' RETURN count(p) as camera_product_count
    """,
    """
    MATCH (t:transazioni)
    WHERE t.payment_method = 'bitcoin'
    WITH count(t) as bitcoin_transactions, sum(t.amount) as total_spent_with_bitcoin
    MATCH (p:prodotti) WHERE p.name = 'macchina fotografica'
    RETURN bitcoin_transactions, total_spent_with_bitcoin, count(p) as camera_product_count
    """,
    """
    MATCH (u:utenti)-[:EFFETTUATO_DA]->(t:transazioni {payment_method: 'bitcoin'})
    WITH u, count(t) as transaction_count, sum(t.amount) as total_spent
    ORDER BY transaction_count DESC
    LIMIT 1
    OPTIONAL MATCH (p:prodotti {name: 'macchina fotografica'})<-[r:CONTIENE]-(t2:transazioni)
    RETURN u.name as user_name, transaction_count, total_spent, count(t2) as camera_transactions
    """
]

def calculate_confidence_interval(data):
    data = np.array(data[1:])  # Ignora il primo tempo (prima esecuzione)
    avg_execution_time = np.mean(data)
    std_dev = np.std(data, ddof=1)
    n = len(data)

    # Calcola l'intervallo di confidenza al 95%
    t_value = t.ppf(0.975, df=n-1)  # Trova il valore critico t per il 95% di confidenza
    margin_of_error = t_value * (std_dev / np.sqrt(n))

    confidence_interval = (avg_execution_time - margin_of_error, avg_execution_time + margin_of_error)

    return avg_execution_time, confidence_interval

execution_times = []

for percentage in percentages:
    print(f"Dimensioni dataset: {percentage}%")

    db_name = f"dataset{percentage}"
    graph = Graph(f"bolt://localhost:7687/{db_name}", user="neo4j", password="password", name=db_name)

    for query_idx, query in enumerate(queries):
        print(f"Query {query_idx + 1}")

        execution_times_query = []

        for _ in range(51):
            start_time = time.time()

            if query_idx == 0:
                result = graph.run(query).data()
                product_id = result[0]['product_id']
            else:
                result = graph.run(query, product_id=product_id).data()

            end_time = time.time()
            execution_time = (end_time - start_time) * 1000
            execution_times_query.append(execution_time)

            print(f"Risultati query {query_idx + 1}:\n{result}")

        avg_execution_time, confidence_interval = calculate_confidence_interval(execution_times_query)

        first_execution_time = execution_times_query[0]

        print(f"Tempo di esecuzione medio (ms): {avg_execution_time}")
        print(f"Tempo della prima esecuzione (ms): {first_execution_time}")
        print(f"Intervallo di confidenza al 95%: {confidence_interval}")

        execution_times.append({
            "Query": f"Query {query_idx + 1}",
            "Percentage": f'{percentage}%',
            "First Execution Time (ms)": first_execution_time,
            "Average Execution Time (ms)": avg_execution_time,
            "Confidence Interval (95%)": confidence_interval,
        })

        print("-" * 40)

csv_file = 'query_resultsNeo4j.csv'
with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['Query', 'Percentage', 'First Execution Time (ms)', 'Average Execution Time (ms)', 'Confidence Interval (95%)']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for data in execution_times:
        writer.writerow(data)