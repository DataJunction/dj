WITH
default_DOT_hard_hat_2 AS (
SELECT  default_DOT_hard_hats.last_name,
	default_DOT_hard_hats.first_name,
	default_DOT_hard_hats.birth_date,
	default_DOT_hard_hats.country
 FROM roads.hard_hats AS default_DOT_hard_hats
 WHERE  DATE_FORMAT(default_DOT_hard_hats.birth_date, 'yyyyMMdd') = DATE_FORMAT(${dj_logical_timestamp}, 'yyyyMMdd')
)
SELECT  last_name,
	first_name,
	birth_date,
	country
 FROM default_DOT_hard_hat_2
 WHERE  birth_date BETWEEN CAST(DATE_FORMAT(CAST(${dj_logical_timestamp} AS TIMESTAMP) - INTERVAL 100 DAY , 'yyyyMMdd') AS TIMESTAMP) AND CAST(DATE_FORMAT(CAST(${dj_logical_timestamp} AS TIMESTAMP), 'yyyyMMdd') AS TIMESTAMP) AND country = CAST(${country} AS STRING)
