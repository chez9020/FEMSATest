-- Creamos esta extension para poder usar el archivo parquet.
CREATE EXTENSION parquet_fdw;

CREATE SERVER parquet_srv FOREIGN DATA WRAPPER parquet_fdw;

--- Creando una tabla foreign phl_parking_pq para poder cargar el archivo .parquet 
CREATE FOREIGN TABLE phl_parking_pq (
    author_id integer,
    tweet_text   text,
    source       text,
    created_at date
    )
  SERVER parquet_srv
  OPTIONS (filename './phl_parking.parquet');