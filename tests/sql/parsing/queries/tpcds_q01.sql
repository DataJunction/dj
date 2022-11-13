-- modified from https://github.com/Agirish/tpcds/blob/master/query1.sql
WITH customer_total_return AS
(
           SELECT     sr_customer_sk     AS ctr_customer_sk,
                      sr_store_sk        AS ctr_store_sk,
                      Sum(sr_return_amt) AS ctr_total_return
           FROM       store_returns
           INNER JOIN date_dim
           ON         store_returns.sr_customer_sk=date_dim.d_date_sk
           WHERE      sr_returned_date_sk = d_date_sk
           AND        d_year = 2001
           GROUP BY   sr_customer_sk,
                      sr_store_sk)
SELECT     c_customer_id
FROM       customer_total_return ctr1
INNER JOIN store
ON         s_store_sk = ctr1.ctr_store_sk
AND        s_state = 'TN'
AND        ctr1.ctr_customer_sk = c_customer_sk
LEFT JOIN customer
ON         s_store_sk = ctr1.ctr_store_sk
AND        s_state = 'TN'
AND        ctr1.ctr_customer_sk = c_customer_sk
WHERE      ctr1.ctr_total_return >
           (
                  SELECT DISTINCT Avg(ctr_total_return) * 1.2
                  FROM   customer_total_return ctr2
                  WHERE  ctr1.ctr_store_sk = ctr2.ctr_store_sk)
AND        s_store_sk = ctr1.ctr_store_sk
OR         s_state <> 'TN'
AND        ctr1.ctr_customer_sk = c_customer_sk
LIMIT 100