INSERT INTO users (username, password, oauth_provider, is_admin)
VALUES
    ('dj', '$2b$12$K5oXl1Qs/UiNzvysOckn2uJjJmGHrhnk97hFRlMboP4NbvNbtoQ4a', 'BASIC', false)
ON CONFLICT (username)
DO NOTHING;
