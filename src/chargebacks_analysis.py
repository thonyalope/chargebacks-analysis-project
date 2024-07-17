import pandas as pd
import numpy as np
import pyodbc
import matplotlib.pyplot as plt

def generate_chargebacks(orders, payments, customers):
    orders_payments = pd.merge(orders, payments, on='order_id')
    orders_payments['chargeback_status'] = np.random.choice(
        ['None', 'Requested', 'Denied', 'Approved'],
        size=len(orders_payments),
        p=[0.9, 0.03, 0.04, 0.03]
    )
    chargebacks = orders_payments[orders_payments['chargeback_status'] != 'None']
    chargebacks = pd.merge(chargebacks, customers, on='customer_id')
    chargebacks = chargebacks.rename(columns={
        'order_id': 'transaction_id',
        'payment_value': 'amount',
        'order_purchase_timestamp': 'transaction_date',
        'chargeback_status': 'status',
        'customer_city': 'city',
        'customer_state': 'state'
    })
    chargebacks = chargebacks[['transaction_id', 'customer_id', 'transaction_date', 'amount', 'status', 'city', 'state']]
    chargebacks.to_csv('data/processed/chargebacks_cleaned.csv', index=False)

def generate_unsettled_transactions(orders, payments, customers):
    orders_payments = pd.merge(orders, payments, on='order_id')
    orders_payments['unsettled_status'] = np.random.choice(
        ['Settled', 'Pending', 'Failed'],
        size=len(orders_payments),
        p=[0.85, 0.1, 0.05]
    )
    unsettled_transactions = orders_payments[orders_payments['unsettled_status'] != 'Settled']
    unsettled_transactions = pd.merge(unsettled_transactions, customers, on='customer_id')
    unsettled_transactions = unsettled_transactions.rename(columns={
        'order_id': 'transaction_id',
        'payment_value': 'amount',
        'order_purchase_timestamp': 'transaction_date',
        'unsettled_status': 'status',
        'customer_city': 'city',
        'customer_state': 'state'
    })
    unsettled_transactions = unsettled_transactions[['transaction_id', 'customer_id', 'transaction_date', 'amount', 'status', 'city', 'state']]
    unsettled_transactions.to_csv('data/processed/unsettled_transactions_cleaned.csv', index=False)

def connect_to_db():
    conn = pyodbc.connect('DRIVER={SQL Server};'
                          'SERVER=your_server_name;'
                          'DATABASE=TransactionAnalysis;'
                          'Trusted_Connection=yes;')
    return conn

def load_data_to_db(file_path, table_name):
    conn = connect_to_db()
    cursor = conn.cursor()
    df = pd.read_csv(file_path)
    for index, row in df.iterrows():
        cursor.execute(f"""
            INSERT INTO {table_name} (transaction_id, customer_id, transaction_date, amount, status, city, state)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            row.transaction_id, row.customer_id, row.transaction_date, row.amount, row.status, row.city, row.state)
    conn.commit()
    cursor.close()
    conn.close()

def run_query(query):
    conn = connect_to_db()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def plot_chargeback_by_country():
    query = """
    SELECT country, COUNT(*) AS ChargebackCount
    FROM Chargebacks
    GROUP BY country
    ORDER BY ChargebackCount DESC;
    """
    df = run_query(query)
    df.plot(kind='bar', x='country', y='ChargebackCount', legend=False)
    plt.title('Chargebacks by Country')
    plt.xlabel('Country')
    plt.ylabel('Number of Chargebacks')
    plt.show()

def plot_unsettled_by_reason():
    query = """
    SELECT reason, COUNT(*) AS UnsettledCount
    FROM UnsettledTransactions
    GROUP BY reason
    ORDER BY UnsettledCount DESC;
    """
    df = run_query(query)
    df.plot(kind='bar', x='reason', y='UnsettledCount', legend=False)
    plt.title('Unsettled Transactions by Reason')
    plt.xlabel('Reason')
    plt.ylabel('Number of Unsettled Transactions')
    plt.show()

if __name__ == "__main__":
    orders = pd.read_csv('data/raw/olist_orders_dataset.csv')
    payments = pd.read_csv('data/raw/olist_order_payments_dataset.csv')
    customers = pd.read_csv('data/raw/olist_customers_dataset.csv')

    generate_chargebacks(orders, payments, customers)
    generate_unsettled_transactions(orders, payments, customers)

    load_data_to_db('data/processed/chargebacks_cleaned.csv', 'Chargebacks')
    load_data_to_db('data/processed/unsettled_transactions_cleaned.csv', 'UnsettledTransactions')

    plot_chargeback_by_country()
    plot_unsettled_by_reason()
