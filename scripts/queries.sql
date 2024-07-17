-- Query for analyzing unsettled transactions by reason
SELECT status AS reason, COUNT(*) AS UnsettledCount
FROM UnsettledTransactions
GROUP BY status
ORDER BY UnsettledCount DESC;

-- Query for analyzing chargebacks by country
SELECT state AS country, COUNT(*) AS ChargebackCount
FROM Chargebacks
GROUP BY state
ORDER BY ChargebackCount DESC;

