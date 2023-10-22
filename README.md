Para realizar o teste de compressão do Timescale DB

Execute os seguintes comandos: 
```SQL

DROP TABLE IF EXISTS phasor;
CREATE TABLE IF NOT EXISTS phasor (
    ts TIMESTAMP NOT NULL,
    magnitude FLOAT,
    angle FLOAT, 
    frequency FLOAT,
    location INT
);
SELECT create_hypertable('phasor', 'ts');
```
Após abra o terminal e faça uma cópia de dados via psql

```bash
psql -h 127.0.0.1 -U postgres -d postgres -p 5432
postgres=# \COPY phasor FROM 'path/1Mlines/final_dataset.csv' WITH DELIMITER ',';
```
Execute os seguintes comandos 
```SQL
select * from phasor; --Garanta que foi carregado
SELECT 
    hypertable_name, 
    pg_size_pretty(hypertable_size(format('%I.%I', hypertable_schema, hypertable_name))) 
FROM 
    timescaledb_information.hypertables; -- Veja o espaço de armazenamento antes da compressão

ALTER TABLE phasor SET (
	timescaledb.compress,
	timescaledb.compress_segmentby = 'location'
);
SELECT add_compression_policy('phasor', INTERVAL '7 days'); -- Configura a compressão
-- Aguarde algum tempo, como os dados tem +7 dias isso deve iniciar o processo de compressão

SELECT 
    hypertable_name, 
    pg_size_pretty(hypertable_size(format('%I.%I', hypertable_schema, hypertable_name))) 
FROM 
    timescaledb_information.hypertables;
```
