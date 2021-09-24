CREATE TABLE dim_users (
	id INTEGER PRIMARY KEY,
	full_name TEXT,
	age INTEGER,
	country TEXT,
	gender TEXT,
	preferred_language TEXT
);
INSERT INTO dim_users (
	id,
	full_name,
	age,
	country,
	gender,
	preferred_language
) VALUES
	(1, 'Alice One', 10, 'Argentina', 'female', 'Spanish'),
	(2, 'Bob Two', 15, 'Brazil', 'male', 'Portuguese'),
	(3, 'Charlie Three', 20, 'Chile', 'non-binary', 'Spanish'),
	(4, 'Denise Four', 25, 'Denmark', 'female', 'Danish'),
	(5, 'Ernie Five', 27, 'Equator', 'male', 'Spanish'),
	(6, 'Fabian Six', 29, 'France', 'non-binary', 'French')
;
