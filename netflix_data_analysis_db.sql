USE netflixdataanalysisdb;
DROP TABLE IF EXISTS netflix_raw_bkp;
SHOW KEYS FROM netflix_raw_bkp WHERE Key_name = 'PRIMARY';
CREATE TABLE netflix_raw_bkp (
    show_id VARCHAR(10) NULL primary key,
    type VARCHAR(10) NULL,
    title VARCHAR(200) NULL,
    director VARCHAR(250) NULL,
    cast VARCHAR(1000) NULL,
    country VARCHAR(150) NULL,
    date_added VARCHAR(20) NULL,
    release_year INT NULL,
    rating VARCHAR(10) NULL,
    duration VARCHAR(10) NULL,
    listed_in VARCHAR(100) NULL,
    description VARCHAR(500) NULL
);
SELECT show_id, count(*) from netflix_raw_bkp;

SELECT show_id, count(*) from netflix_raw_bkp
group by show_id 
having count(*) > 1 ;

SELECT show_id, title, type, COUNT(*) 
FROM netflix_raw_bkp
GROUP BY show_id, title, type
HAVING COUNT(*) > 1;


SELECT * FROM netflix_raw_bkp 
	WHERE CONCAT(UPPER(title) , type)  IN 
	(
		SELECT CONCAT(UPPER(title) , type) 
		FROM netflix_raw_bkp
		GROUP BY UPPER(title) , type
		HAVING count(*) > 1 
	)
ORDER BY title;

-- WITH cte AS (
--     SELECT *,
--     ROW_NUMBER() OVER (PARTITION BY title, type ORDER BY show_id) AS rn
--     FROM netflix_raw_bkp
-- )
-- SELECT * FROM cte
-- WHERE rn = 1;
SET SQL_SAFE_UPDATES = 0;
DELETE FROM netflix_raw_bkp
WHERE show_id NOT IN (
    SELECT show_id FROM (
        SELECT MIN(show_id) as show_id
        FROM netflix_raw_bkp
        GROUP BY title, type
    ) AS temp
);
SELECT count(*) from netflix_raw_bkp;

-- SELECT show_id , value as genre
-- FROM netflix_raw_bkp 
-- cross apply string_split(director,',')

CREATE TABLE netflix_directors AS
WITH RECURSIVE split_directors AS (
    -- Anchor: Get the first director (up to the first comma)
    SELECT 
        show_id, 
        SUBSTRING_INDEX(director, ',', 1) AS director, 
        SUBSTRING(director, LOCATE(',', director) + 1) AS remaining_directors
    FROM netflix_raw_bkp
    WHERE director IS NOT NULL
    UNION ALL
    
    -- Recursive: Process the remaining string until no commas are left
    SELECT 
        show_id, 
        SUBSTRING_INDEX(remaining_directors, ',', 1), 
        IF(LOCATE(',', remaining_directors) > 0, 
           SUBSTRING(remaining_directors, LOCATE(',', remaining_directors) + 1), 
           NULL)
    FROM split_directors
    WHERE remaining_directors IS NOT NULL
)
SELECT show_id, TRIM(director) AS director
FROM split_directors;

CREATE TABLE netflix_country AS
WITH RECURSIVE split_country AS (
    -- Anchor: Get the first director (up to the first comma)
    SELECT 
        show_id, 
        SUBSTRING_INDEX(country, ',', 1) AS country, 
        SUBSTRING(country, LOCATE(',', country) + 1) AS remaining_country
    FROM netflix_raw_bkp
    WHERE country IS NOT NULL
    UNION ALL
    
    -- Recursive: Process the remaining string until no commas are left
    SELECT 
        show_id, 
        SUBSTRING_INDEX(remaining_country, ',', 1), 
        IF(LOCATE(',', remaining_country) > 0, 
           SUBSTRING(remaining_country, LOCATE(',', remaining_country) + 1), 
           NULL)
    FROM split_country
    WHERE remaining_country IS NOT NULL
)
SELECT show_id, TRIM(country) AS country
FROM split_country;



CREATE TABLE netflix_cast AS
WITH RECURSIVE split_cast AS (
    -- Anchor: Get the first director (up to the first comma)
    SELECT 
        show_id, 
        SUBSTRING_INDEX(cast, ',', 1) AS cast, 
        SUBSTRING(cast, LOCATE(',', cast) + 1) AS remaining_cast
    FROM netflix_raw_bkp
    WHERE cast IS NOT NULL
    UNION ALL
    
    -- Recursive: Process the remaining string until no commas are left
    SELECT 
        show_id, 
        SUBSTRING_INDEX(remaining_cast, ',', 1), 
        IF(LOCATE(',', remaining_cast) > 0, 
           SUBSTRING(remaining_cast, LOCATE(',', remaining_cast) + 1), 
           NULL)
    FROM split_cast
    WHERE remaining_cast IS NOT NULL
)
SELECT show_id, TRIM(cast) AS cast
FROM split_cast;

CREATE TABLE netflix_genre AS
WITH RECURSIVE split_genre AS (
    -- Anchor: Get the first genre (up to the first comma)
    SELECT 
        show_id, 
        SUBSTRING_INDEX(listed_in, ',', 1) AS genre, 
        SUBSTRING(listed_in, LOCATE(',', listed_in) + 1) AS remaining_genre
    FROM netflix_raw_bkp
    WHERE listed_in IS NOT NULL
    UNION ALL
    
    -- Recursive: Process the remaining string until no commas are left
    SELECT 
        show_id, 
        SUBSTRING_INDEX(remaining_genre, ',', 1), 
        IF(LOCATE(',', remaining_genre) > 0, 
           SUBSTRING(remaining_genre, LOCATE(',', remaining_genre) + 1), 
           NULL)
    FROM split_genre
    WHERE remaining_genre IS NOT NULL
)
SELECT show_id, TRIM(genre) AS genre
FROM split_genre;

SELECT * FROM netflix_cast;
SELECT * FROM netflix_genre;
SELECT * FROM netflixdataanalysisdb.netflix_raw_bkp;

SELECT * FROM netflix_country where show_id ='s1001';

INSERT into netflix_country
SELECT show_id,m.country 
FROM netflix_raw_bkp nrk
inner join (
select director,country
from netflix_country nc
inner join netflix_directors nd
on nc.show_id = nd.show_id
group by director,country
) m on nrk.director = m.director
WHERE nrk.country is null;
---------------------------------- 

select * from netflix_raw_bkp where duration is null;

CREATE TABLE netflix_stg AS
WITH cte AS (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY title, type ORDER BY show_id) AS rn
    FROM netflix_raw_bkp
)
SELECT 
    show_id,
    type,
    title,
    STR_TO_DATE(date_added, '%M %d, %Y') AS date_added,
    release_year,
    rating,
    CASE WHEN duration IS NULL THEN rating ELSE duration END AS duration,
    description
FROM cte
WHERE rn = 1;

-- Q1:For each director, count the number of movies and TV shows created by them in separate columns. 
-- For directors who have created TV shows and movies both.
#DIRECTORS DONE BOTH TV AND MOVIES
select  nd.director, -- COUNT(distinct n.type) as distinct_type
COUNT(distinct case when n.type = 'Movie' then n.show_id end) as no_of_movies,
COUNT(distinct case when n.type = 'Tv Show' then n.show_id end) as no_of_tv_shows
from netflix_stg n
inner join netflix_directors nd on
n.show_id = nd.show_id
GROUP BY nd.director
HAVING COUNT(distinct n.type)> 1
-- ORDER BY distinct_type desc

-- Q2:Which country has the highest number of comedy movies?
SELECT 
    nc.country, 
    COUNT(*) AS total_comedies
FROM 
    netflix_genre ng 
    INNER JOIN netflix_country nc ON nc.show_id = ng.show_id
    INNER JOIN netflix_stg ns ON ns.show_id = ng.show_id
WHERE 
    ng.genre = 'Comedies'
    AND ns.type = 'Movie'
GROUP BY 
    nc.country
ORDER BY 
    total_comedies DESC
LIMIT 1;


-- Q3:For each year (as per date added to Netflix), 
-- which director has the maximum number of movies released?
WITH cte AS (
    SELECT 
        nd.director,
        YEAR(date_added) AS date_year,
        COUNT(n.show_id) AS no_of_movies
    FROM netflix_stg n
    INNER JOIN netflix_directors nd ON n.show_id = nd.show_id
    WHERE type = 'Movie'
    GROUP BY nd.director, YEAR(date_added)
),
cte2 AS (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY date_year ORDER BY no_of_movies DESC, director) AS rn
    FROM cte
)
SELECT * FROM cte2 WHERE rn = 1;

-- Q4 : what is the avg duration movies in each genre
 
SELECT 
    ng.genre, 
    ROUND(AVG(CAST(REPLACE(duration, ' min', '') AS UNSIGNED)), 0) AS avg_duration_movies
FROM netflix_stg n
INNER JOIN netflix_genre ng ON n.show_id = ng.show_id
WHERE n.type = 'Movie'
GROUP BY ng.genre
ORDER BY avg_duration_movies DESC;


-- Q5 : Find the list of directors who have created **both horror and comedy movies**.
-- Display the **director names** along with the **number of comedy movies** and **number of horror movies** directed by them.
select
nd.director,
COUNT(distinct CASE WHEN ng.genre ='Comedies' then n.show_id end) as no_of_comedy,
COUNT(distinct CASE WHEN ng.genre ='Horror Movies' then n.show_id end) as no_of_horror

from netflix_stg n 
inner join netflix_genre ng on ng.show_id = n.show_id
inner join netflix_directors nd on nd.show_id = n.show_id
where type= 'Movie' and ng.genre in ('Comedies','Horror Movies')
group by nd.director 
having COUNT(distinct ng.genre) = 2 
	
    
























































