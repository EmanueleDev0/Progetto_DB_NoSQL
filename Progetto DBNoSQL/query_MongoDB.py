import time
import numpy as np
import pandas as pd
from pymongo import MongoClient
from scipy.stats import t
import logging

# Impostazioni di logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Costanti
PERCENTAGES = [100, 75, 50, 25]
NUM_EXPERIMENTS = 51
PRODUCTS_COLLECTION_NAME_TEMPLATE = 'prodotti{}%'
TRANSACTIONS_COLLECTION_NAME_TEMPLATE = 'transazioni{}%'
USERS_COLLECTION_NAME_TEMPLATE = 'utenti{}%'
PAYMENT_METHOD_BITCOIN = 'bitcoin'
PRODUCT_NAME = 'macchina fotografica'

# Connessione al database MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['frodi_ecommerce']

# Query 1: cerca il prodotto specifico "macchina fotografica"
def search_product_in_store(products_collection_name, product_name):
    result = db[products_collection_name].find_one({'name': product_name})
    if result:
        logger.info(f"Prodotto {product_name} trovato nell'e-commerce.")
    else:
        logger.info(f"Prodotto {product_name} non trovato.")

# Query 2: cerca il prodotto specifico "macchina fotografica" e il numero di transazioni relative a quel prodotto.
def find_transactions_for_product(transactions_collection_name, product_id):
    transaction_count = db[transactions_collection_name].count_documents({'product_id': product_id})
    logger.info(f"Numero di transazioni per il prodotto con product_id {product_id}: {transaction_count}")

# Query 3: Calcola il numero di volte in cui è stato utilizzato il metodo di pagamento "bitcoin" e l'importo speso usando quel metodo.
# Successivamente cerca un prodotto specifico "macchina fotografica" e il numero di transazioni relative a quel prodotto.
def find_payment_usage_and_product_transactions(transactions_collection_name, payment_method, products_collection_name, product_name):
    pipeline = [
        {
            '$match': {
                'payment_method': payment_method
            }
        },
        {
            '$group': {
                '_id': None,
                'bitcoin_transactions': {'$sum': 1},
                'total_spent_with_bitcoin': {'$sum': '$amount'}
            }
        }
    ]
    result = db[transactions_collection_name].aggregate(pipeline)
    bitcoin_info = list(result)

    if bitcoin_info:
        bitcoin_transactions = bitcoin_info[0]['bitcoin_transactions']
        total_spent_with_bitcoin = bitcoin_info[0]['total_spent_with_bitcoin']
        logger.info(f"Numero di transazioni con metodo di pagamento {payment_method}: {bitcoin_transactions}")
        logger.info(f"Importo totale speso con il metodo di pagamento {payment_method}: {total_spent_with_bitcoin:.2f}")

        product = db[products_collection_name].find_one({'name': product_name})
        if product:
            product_id = product['product_id']
            find_transactions_for_product(transactions_collection_name, product_id)
        else:
            logger.info(f"Prodotto {product_name} non trovato.")
    else:
        logger.info(f"Nessuna transazione con metodo di pagamento {payment_method} trovata.")

# Query 4: Trova il nome dell'utente che ha usato più volte il metodo di pagamento "bitcoin", l'importo speso da questo utente utilizzando questo
# metodo di pagamento.  Successivamente cerca un prodotto specifico "macchina fotografica" e il numero di transazioni relative a quel prodotto.
def find_most_frequent_bitcoin_user_and_product_transactions(users_collection_name, transactions_collection_name, products_collection_name, payment_method, product_name):
    pipeline = [
        {
            '$match': {
                'payment_method': payment_method,
            }
        },
        {
            '$group': {
                '_id': '$user_id',
                'transaction_count': {'$sum': 1},
                'total_spent': {'$sum': '$amount'}
            }
        },
        {
            '$sort': {'transaction_count': -1}
        },
        {
            '$limit': 1
        }
    ]
    result = db[transactions_collection_name].aggregate(pipeline)
    most_frequent_user = list(result)

    if most_frequent_user:
        most_frequent_user_id = most_frequent_user[0]['_id']
        user_info = db[users_collection_name].find_one({'user_id': most_frequent_user_id})
        user_name = user_info['name']
        total_spent = most_frequent_user[0]['total_spent']

        logger.info(f"Nome dell'utente che ha usato piu' volte il metodo di pagamento {payment_method}: {user_name}")
        logger.info(f"Importo totale speso da {user_name} utilizzando il metodo di pagamento {payment_method}: {total_spent:.2f}")

        product = db[products_collection_name].find_one({'name': product_name})
        if product:
            product_id = product['product_id']
            find_transactions_for_product(transactions_collection_name, product_id)
        else:
            logger.info(f"Prodotto {product_name} non trovato.")
    else:
        logger.info(f"Nessun utente con transazioni usando il metodo di pagamento {payment_method} trovato.")

# Funzione per eseguire un singolo esperimento
def run_experiment(query_func, *args):
    start_time = time.time()
    result = query_func(*args)
    end_time = time.time()
    execution_time = (end_time - start_time) * 1000  # Converte in millisecondi
    return execution_time, result

# Esegue gli esperimenti
results = []

query_functions = [
    search_product_in_store,
    find_transactions_for_product,
    find_payment_usage_and_product_transactions,
    find_most_frequent_bitcoin_user_and_product_transactions
]

query_names = [
    'Query 1',
    'Query 2',
    'Query 3',
    'Query 4'
]

# Esegue le query
for percentage in PERCENTAGES:
    logger.info(f"\nContenuto informativo al {percentage}%:")

    users_collection_name = USERS_COLLECTION_NAME_TEMPLATE.format(percentage)
    transactions_collection_name = TRANSACTIONS_COLLECTION_NAME_TEMPLATE.format(percentage)
    products_collection_name = PRODUCTS_COLLECTION_NAME_TEMPLATE.format(percentage)

    for query_func, query_name in zip(query_functions, query_names):
        experiment_data = {
            'Query': query_name,
            'Percentage': f"{percentage}%",
            'First Execution Time (ms)': None,
            'Average Execution Time (ms)': None,
            'Confidence Interval (95%)': None
        }

        logger.info(f"\nEsecuzione di {query_name}")

        if query_func == search_product_in_store:
            first_execution_time, _ = run_experiment(query_func, products_collection_name, PRODUCT_NAME)
        elif query_func == find_transactions_for_product:
            product = db[products_collection_name].find_one({'name': PRODUCT_NAME})
            if product:
                product_id = product['product_id']
                first_execution_time, _ = run_experiment(query_func, transactions_collection_name, product_id)
            else:
                logger.info("Prodotto 'computer' non trovato.")
                continue
        elif query_func == find_payment_usage_and_product_transactions:
            first_execution_time, _ = run_experiment(query_func, transactions_collection_name, PAYMENT_METHOD_BITCOIN, products_collection_name, PRODUCT_NAME)
        elif query_func == find_most_frequent_bitcoin_user_and_product_transactions:
            first_execution_time, _ = run_experiment(query_func, users_collection_name, transactions_collection_name, products_collection_name, PAYMENT_METHOD_BITCOIN, PRODUCT_NAME)

        experiment_data['First Execution Time (ms)'] = first_execution_time

        execution_times = []
        for _ in range(NUM_EXPERIMENTS - 1):
            if query_func == search_product_in_store:
                execution_time, _ = run_experiment(query_func, products_collection_name, PRODUCT_NAME)
            elif query_func == find_transactions_for_product:
                execution_time, _ = run_experiment(query_func, transactions_collection_name, product_id)
            elif query_func == find_payment_usage_and_product_transactions:
                execution_time, _ = run_experiment(query_func, transactions_collection_name, PAYMENT_METHOD_BITCOIN, products_collection_name, PRODUCT_NAME)
            elif query_func == find_most_frequent_bitcoin_user_and_product_transactions:
                execution_time, _ = run_experiment(query_func, users_collection_name, transactions_collection_name, products_collection_name, PAYMENT_METHOD_BITCOIN, PRODUCT_NAME)
            execution_times.append(execution_time)

        mean_execution_time = np.mean(execution_times)
        std_deviation = np.std(execution_times)
        confidence_interval = t.interval(0.95, NUM_EXPERIMENTS - 1, loc=mean_execution_time,
                                         scale=std_deviation / np.sqrt(NUM_EXPERIMENTS - 1))
        experiment_data['Average Execution Time (ms)'] = mean_execution_time
        experiment_data['Confidence Interval (95%)'] = confidence_interval

        results.append(experiment_data)

# Creazione del DataFrame
results_df = pd.DataFrame(results)

# Salvataggio su file CSV
results_df.to_csv('query_resultsMongo.csv', index=False)

# Stampa dei risultati
logger.info("Risultati:")
logger.info(results_df)