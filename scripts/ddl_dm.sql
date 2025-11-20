-- Витрина для анализа средней продолжительности песен для каждой страны
CREATE TABLE dm.avg_song_duration_by_country AS
SELECT date, country_name, AVG(duration_sec) AS avg_duration_sec
FROM dds.fact_daily_top_100 fdt
    JOIN dds.dim_song dm USING(song_id)
    JOIN dds.dim_country USING(country_id)
GROUP BY date, country_name;

-- Витрина для подсчета появлений артистов в чарте за каждую дату
CREATE TABLE dm.artist_appearances_by_date AS
SELECT date, artist_name, COUNT(*) AS cnt_appearance
FROM dds.fact_daily_top_100 fdt
    JOIN dds.dim_artist da USING(artist_id)
GROUP BY date, artist_name;

-- Витрина для подсчета ожидаемых роялти для каждого артиста за день
-- 0,003 - стоимость одного прослушивания (цифра взята для примера)
CREATE TABLE dm.expected_artist_royalties_by_date AS
SELECT date, artist_name, ROUND(SUM(listeners_count) * 0.003, 2) AS royalties 
FROM dds.fact_daily_top_100 fdt
    JOIN dds.dim_artist da USING(artist_id)
GROUP BY date, artist_name	
ORDER BY date, royalties DESC;