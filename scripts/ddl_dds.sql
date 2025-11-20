-- Таблица измерений для артистов
CREATE TABLE dds.dim_artist (
	artist_id serial PRIMARY KEY,
	artist_name varchar(50) NOT NULL UNIQUE
);

-- Таблица измерений для стран
CREATE TABLE dds.dim_country (
	country_id serial PRIMARY KEY,
	country_name char(50) NOT NULL UNIQUE
);

-- Таблица измерений для песен 
CREATE TABLE dds.dim_song (
	song_id serial PRIMARY KEY,
	song_name varchar(100) NOT NULL,
	duration_sec int,
	UNIQUE (song_name, duration_sec)
);

-- Таблица фактов для ежедненых топ-100 треков по странам
-- Одна строка - песня, ее позиция в чарте и количество прослушиваний за дату для конкретной страны
CREATE TABLE dds.fact_daily_top_100 (
	fact_id serial PRIMARY KEY,
	date DATE NOT NULL,
	country_id int NOT NULL REFERENCES dds.dim_country(country_id),
	song_id int NOT NULL REFERENCES dds.dim_song(song_id),
	artist_id int NOT NULL REFERENCES dds.dim_artist(artist_id),
	song_rank smallint NOT NULL,
	listeners_count int,
	UNIQUE (date, country_id, song_rank)
);