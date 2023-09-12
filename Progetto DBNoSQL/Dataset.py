from faker import Faker
import random
import csv

fake = Faker()

# Record del dataset 100% per gli utenti
total_users = 10000

# Percentuali dei dataset rispetto al 100%
percentages = [100, 75, 50, 25]

# Lista di parole chiave per i nomi di prodotti
product_names = ['palla', 'cellulare', 'computer', 'libro', 'orologio', 'scarpe', 'borsa', 'giocattolo', 'televisore', 'occhiali', 
                 'bracciale', 'collana', 'maglia', 'pantaloni', 'automobile', 'tablet', 'console', 'ocarina', 'frullatore', 'tappeto',
                 'bicicletta', 'stampante', 'ventilatore', 'occhiali da sole', 'telecamera', 'microfono', 'altoparlante', 'radio', 
                 'macchina fotografica', 'radiatore', 'ventola', 'trapano', 'frigorifero', 'tostapane', 'forno', 'cuffie', 'piatto', 
                 'bicchiere', 'cucchiaio', 'forchetta', 'coltello', 'tazza', 'tavolo', 'sedia', 'divano', 'letto', 'lampada', 'Auricolari wireless',
                 'Videogiochi', 'Stampante laser', 'Forno a microonde', 'Aspirapolvere robot', 'Macchina da caffè', 'Tablet Android', 'Monitor PC',
                 'Giocattoli per bambini', 'Frigorifero doppia porta', 'Telefono cellulare', 'Smart TV 4K', 'Tostapane', 'Borsa da viaggio', 
                 'Cuffie Bluetooth', 'Macchina per il fitness', 'Videocamera HD', 'Piumino', 'Sedia da ufficio', 'Bicicletta da corsa',
                 'Orologio da polso', 'Scarpe da ginnastica', 'Libri per bambini', 'Telefono cordless', 'Macchina fotografica digitale', 
                 'Tavolo da pranzo', 'Abbigliamento sportivo', 'Altoparlante Bluetooth', 'Letto a baldacchino', 'Lavatrice', 'Tappeto persiano',
                 'Penna stilografica', 'Fornello a gas', 'Pentola a pressione', 'Portatile', 'Orologio da parete', 'Pantaloni in jeans', 
                 'Ombrello pieghevole', 'Olio di oliva extra vergine', 'Set di pentole antiaderenti',  'Set di posate in acciaio inox',
                 'Tavolo da gioco', 'Kit per il trucco', 'Cuscino memory foam', 'Pantofole da casa', 'Videoproiettore HD', 'Valigia rigida',
                 'Lavastoviglie', 'Vino rosso pregiato', 'Smartwatch']

# Dizionario per tenere traccia degli ID dei prodotti e dei prezzi per ciascun nome di prodotto
product_info = {}

for name in product_names:
    product_id = len(product_info) + 1  # Incrementa l'ID del prodotto
    price = round(random.uniform(1, 200), 2)  # Genera un prezzo casuale
    
    # Aggiunge le informazioni del prodotto al dizionario
    product_info[name] = {
        'product_id': product_id,
        'price': price
    }

for percentage in percentages:
    num_records = int(total_users * (percentage / 100))

    # Apre un file CSV per scrivere gli utenti generati
    users_csv_filename = f'utenti_{percentage}%.csv'
    with open(users_csv_filename, 'w', newline='') as users_csvfile:
        users_fieldnames = ['user_id', 'name', 'address', 'contact_info', 'email']
        users_writer = csv.DictWriter(users_csvfile, fieldnames=users_fieldnames)
        users_writer.writeheader()

        users = []  # Lista per memorizzare gli utenti

        # Genera utenti
        for user_num in range(num_records):
            user_id = user_num + 1
            user = {
                'user_id': user_id,
                'name': fake.name(),
                'address': fake.address(),
                'contact_info': fake.phone_number(),
                'email': fake.email()
            }
            users.append(user)

            users_writer.writerow(user)

        print(f"File CSV utenti {percentage}% generato con successo.")

    # Apre un file CSV per scrivere i prodotti
    products_csv_filename = f'prodotti_{percentage}%.csv'
    with open(products_csv_filename, 'w', newline='') as products_csvfile:
        products_fieldnames = ['product_id', 'name', 'price']
        products_writer = csv.DictWriter(products_csvfile, fieldnames=products_fieldnames)
        products_writer.writeheader()

        products = []  # Lista per memorizzare i prodotti
        product_id_counter = 1  # Contatore per i product_id

        # Genera dati casuali per i prodotti utilizzando il dizionario product_info
        for _ in range(num_records * 3):  # Genera prodotti per ogni utente
            name = random.choice(product_names)
            product_id = product_info[name]['product_id']
            price = product_info[name]['price']

            products_writer.writerow({
                'product_id': product_id,
                'name': name,
                'price': price,
            })

            products.append({'product_id': product_id, 'price': price})

        print(f"File CSV prodotti {percentage}% generato con successo.")

    # Apre un file CSV per scrivere le transazioni
    transactions_csv_filename = f'transazioni_{percentage}%.csv'
    with open(transactions_csv_filename, 'w', newline='') as transactions_csvfile:
        transactions_fieldnames = ['transaction_id', 'user_id', 'product_id', 'amount', 'transaction_date', 'payment_method']
        transactions_writer = csv.DictWriter(transactions_csvfile, fieldnames=transactions_fieldnames)
        transactions_writer.writeheader()

        # Genera dati casuali per le transazioni
        for user in users:
            user_id = user['user_id']
            for _ in range(4):  # Genera transazioni per ogni utente
                transaction_id = fake.uuid4()
                product_id = fake.random_int(min=1, max=100)
                product_price = 0  # Inizializza il prezzo del prodotto a 0

                # Trova il prezzo corrispondente del prodotto
                for product in products:
                    if product['product_id'] == product_id:
                        product_price = product['price']
                        break

                amount = product_price  # Imposta l'amount al prezzo del prodotto

                # Verifica se l'amount è maggiore di 0 prima di scrivere la transazione
                if amount > 0:
                    transaction_date = fake.date_time_this_year()
                    payment_method = random.choice(['credit_card', 'paypal', 'bitcoin'])

                    transactions_writer.writerow({
                        'transaction_id': transaction_id,
                        'user_id': user_id,
                        'product_id': product_id,
                        'amount': amount,
                        'transaction_date': transaction_date,
                        'payment_method': payment_method
                    })

        print(f"File CSV transazioni {percentage}% generato con successo.")
