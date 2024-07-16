import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    # Load datasets
    logging.info('Loading datasets...')
    orders_path = '../data/olist_orders_dataset.csv'
    payments_path = '../data/olist_order_payments_dataset.csv'
    customers_path = '../data/olist_customers_dataset.csv'
    orders = pd.read_csv(orders_path)
    payments = pd.read_csv(payments_path)
    customers = pd.read_csv(customers_path)

    # Merge datasets on 'order_id' and 'customer_id'
    logging.info('Merging datasets...')
    orders_payments = pd.merge(orders, payments, on='order_id')
    data = pd.merge(orders_payments, customers, on='customer_id')

    # Simulate chargebacks: Consider orders with status 'canceled' as chargebacks
    logging.info('Simulating chargebacks...')
    data['is_chargeback'] = data['order_status'].apply(lambda x: 1 if x == 'canceled' else 0)

    # Filter only chargeback transactions
    chargebacks_data = data[data['is_chargeback'] == 1]

    # Perform data cleaning and preprocessing
    chargebacks_data.dropna(inplace=True)

    # Exploratory Data Analysis (EDA) for chargebacks only
    logging.info('Performing Exploratory Data Analysis...')
    plt.figure(figsize=(10, 6))
    sns.histplot(chargebacks_data['payment_value'], bins=50, kde=True)
    plt.title('Distribution of Payment Values for Chargebacks')
    plt.xlabel('Payment Value')
    plt.ylabel('Count')
    plt.show()

    # Distribution of chargebacks over time
    plt.figure(figsize=(10, 6))
    sns.histplot(pd.to_datetime(chargebacks_data['order_purchase_timestamp']), bins=50, kde=True)
    plt.title('Distribution of Chargebacks Over Time')
    plt.xlabel('Order Purchase Timestamp')
    plt.ylabel('Count')
    plt.show()

    # SQL Database Integration
    logging.info('Integrating with SQL Database...')
    engine = create_engine('mssql+pyodbc://username:password@server/ChargebacksDB')
    chargebacks_data.to_sql('Transactions', con=engine, if_exists='replace', index=False)

    # Analysis and Insights for chargebacks only
    logging.info('Running SQL Queries...')
    query = """
    SELECT payment_type, buyer_city, COUNT(*) AS PaymentCount
    FROM Transactions
    GROUP BY payment_type, buyer_city
    ORDER BY PaymentCount DESC;
    """
    chargebacks_analysis = pd.read_sql(query, con=engine)
    print(chargebacks_analysis)

    logging.info('Chargebacks analysis completed successfully.')

except Exception as e:
    logging.error(f'Error occurred: {e}')
