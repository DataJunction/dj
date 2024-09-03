WITH
default_DOT_hard_hat AS (
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
SELECT  default_DOT_hard_hat.hard_hat_id,
	default_DOT_hard_hat.last_name,
	default_DOT_hard_hat.first_name,
	default_DOT_hard_hat.title,
	default_DOT_hard_hat.birth_date,
	default_DOT_hard_hat.hire_date,
	default_DOT_hard_hat.address,
	default_DOT_hard_hat.city,
	default_DOT_hard_hat.state,
	default_DOT_hard_hat.postal_code,
	default_DOT_hard_hat.country,
	default_DOT_hard_hat.manager,
	default_DOT_hard_hat.contractor_id
 FROM default_DOT_hard_hat
