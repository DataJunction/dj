INSERT INTO users (username, password, oauth_provider, is_admin, kind)
VALUES
    ('dj', '$2b$12$K5oXl1Qs/UiNzvysOckn2uJjJmGHrhnk97hFRlMboP4NbvNbtoQ4a', 'BASIC', true, 'USER')
ON CONFLICT (username)
DO UPDATE SET is_admin = EXCLUDED.is_admin;
