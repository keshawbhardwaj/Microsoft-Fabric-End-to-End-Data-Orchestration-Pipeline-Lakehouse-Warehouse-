CREATE TABLE wh_netflix_titles (
    show_id VARCHAR(50),
    type VARCHAR(50),
    title VARCHAR(500),
    director VARCHAR(500),
    cast VARCHAR(MAX),
    country VARCHAR(500),
    date_added DATE,
    release_year SMALLINT,
    rating VARCHAR(20),
    duration VARCHAR(50),
    listed_in VARCHAR(200),
    description VARCHAR(MAX),
    load_timestamp DATETIME2(6) 
);


select * from LH_Raw.dbo.netflix_series_description

MERGE INTO WH_Gold.dbo.wh_netflix_titles AS tgt
USING (
    SELECT
        show_id,
        LEFT(type, 50) AS type,
        LEFT(title, 500) AS title,
        LEFT(director, 500) AS director,
        cast,
        LEFT(country, 500) AS country,
        TRY_CAST(date_added AS DATE) AS date_added,
        TRY_CAST(release_year AS SMALLINT) AS release_year,
        LEFT(rating, 20) AS rating,
        LEFT(duration, 50) AS duration,
        LEFT(listed_in, 200) AS listed_in,
        description
    FROM LH_Raw.dbo.netflix_series_description
) AS src
ON tgt.show_id = src.show_id

WHEN MATCHED THEN 
    UPDATE SET
        tgt.type = src.type,
        tgt.title = src.title,
        tgt.director = src.director,
        tgt.cast = src.cast,
        tgt.country = src.country,
        tgt.date_added = src.date_added,
        tgt.release_year = src.release_year,
        tgt.rating = src.rating,
        tgt.duration = src.duration,
        tgt.listed_in = src.listed_in,
        tgt.description = src.description,
        tgt.load_timestamp = CURRENT_TIMESTAMP

WHEN NOT MATCHED THEN 
    INSERT (
        show_id,
        type,
        title,
        director,
        cast,
        country,
        date_added,
        release_year,
        rating,
        duration,
        listed_in,
        description,
        load_timestamp
    )
    VALUES (
        src.show_id,
        src.type,
        src.title,
        src.director,
        src.cast,
        src.country,
        src.date_added,
        src.release_year,
        src.rating,
        src.duration,
        src.listed_in,
        src.description,
        CURRENT_TIMESTAMP
    );
