-- sql/analysis_queries.sql

-- Example: Count of chargebacks by reason
SELECT Reason, COUNT(*) AS ChargebackCount
FROM Transactions
WHERE is_chargeback = 1
GROUP BY Reason
ORDER BY ChargebackCount DESC;
