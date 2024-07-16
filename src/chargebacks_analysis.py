import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

# Load dataset (replace with your dataset path)
dataset_path = '../data/raw/chargebacks_dataset.csv'
data = pd.read_csv(dataset_path)

# Perform data cleaning and preprocessing
# Example: Drop missing values
data.dropna(inplace=True)

# Exploratory Data Analysis (EDA)
# Example: Plotting transaction amounts
plt.figure(figsize=(10, 6))
sns.histplot(data['Amount'], bins=50, kde=True)
plt.title('Distribution of Transaction Amounts')
plt.xlabel('Amount')
plt.ylabel('Count')
plt.show()

# SQL Database Integration
# Example: Create SQL database and store data
engine = create_engine('mssql+pyodbc://username:password@server/database')
data.to_sql('Transactions', con=engine, if_exists='replace', index=False)

# Analysis and Insights
# Example: SQL queries for chargebacks analysis
query = """
SELECT Reason, COUNT(*) AS ChargebackCount
FROM Transactions
WHERE is_chargeback = 1
GROUP BY Reason
ORDER BY ChargebackCount DESC;
"""
chargebacks_analysis = pd.read_sql(query, con=engine)
print(chargebacks_analysis)