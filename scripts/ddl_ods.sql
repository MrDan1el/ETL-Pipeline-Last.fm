-- Временная таблица для получения данных из csv файла
CREATE TABLE ods.temp_daily_data(
    song_name varchar(100),
    artist_name varchar(50),
    duration_sec int,
    listeners_count int,
    song_rank smallint,
    source_date DATE,
    country varchar(50),
    UNIQUE(song_rank, source_date, country)
);

-- Основная таблица ODS слоя
CREATE TABLE ods.daily_data(
	id serial PRIMARY KEY,
	song_name varchar(100),
	artist_name varchar(50),
	duration_sec int,
	listeners_count int,
	song_rank smallint,
	source_date DATE,
	country varchar(50),
	UNIQUE(song_rank, source_date, country)
);