CREATE DATABASE steam;
CREATE TABLE steam.playtime(user_id VARCHAR, game VARCHAR, behavior VARCHAR, amount FLOAT, zero INT);

IMPORT INTO steam.playtime (user_id, game, behavior, amount, zero)
CSV DATA (
    'nodelocal://self/steam-hours-played.csv'
);

CREATE TABLE steam.games(
    url VARCHAR,
    types VARCHAR,
    name VARCHAR,
    desc_snippet VARCHAR,
    recent_reviews VARCHAR,
    all_reviews VARCHAR,
    release_date VARCHAR,
    developer VARCHAR,
    publisher VARCHAR,
    popular_tags VARCHAR,
    game_details VARCHAR,
    languages VARCHAR,
    achievements VARCHAR,
    genre VARCHAR,
    game_description VARCHAR,
    mature_content VARCHAR,
    minimum_requirements VARCHAR,
    recommended_requirements VARCHAR,
    original_price VARCHAR,
    discount_price VARCHAR
);

IMPORT INTO steam.games (url,types,name,desc_snippet,recent_reviews,all_reviews,release_date,developer,publisher,popular_tags,game_details,languages,achievements,genre,game_description,mature_content,minimum_requirements,recommended_requirements,original_price,discount_price)
CSV DATA (
    'nodelocal://self/steam-games.csv'
);
