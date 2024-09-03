WITH
default_DOT_hard_hat_2 AS (
SELECT  default_DOT_hard_hats.hard_hat_id,
	default_DOT_hard_hats.last_name,
	default_DOT_hard_hats.first_name,
	default_DOT_hard_hats.title,
	default_DOT_hard_hats.birth_date,
	default_DOT_hard_hats.hire_date,
	default_DOT_hard_hats.address,
	default_DOT_hard_hats.city,
	default_DOT_hard_hats.state,
	default_DOT_hard_hats.postal_code,
	default_DOT_hard_hats.country,
	default_DOT_hard_hats.manager,
	default_DOT_hard_hats.contractor_id
 FROM roads.hard_hats AS default_DOT_hard_hats
)
SELECT  hard_hat_id,
	last_name,
	first_name,
	title,
	birth_date,
	hire_date,
	address,
	city,
	state,
	postal_code,
	country,
	manager,
	contractor_id
 FROM default_DOT_hard_hat_2
 WHERE  birth_date = CAST(DATE_FORMAT(CAST(${dj_logical_timestamp} AS TIMESTAMP), 'yyyyMMdd') AS TIMESTAMP)
