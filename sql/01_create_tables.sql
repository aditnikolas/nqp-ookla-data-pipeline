CREATE TABLE IF NOT EXISTS mart_network_quality_province (
    prov_id varchar(10) not null,
    type varchar(20) not null check (type in ('mobile', 'fixed')),
    year int not null check (year >= 2019),
    quarter int not null check (quarter between 1 and 4),
    provinsi text not null,
    n_tiles bigint not null check (n_tiles > 0),
    tests bigint not null check (tests > 0),
    devices bigint not null check (devices > 0),
    dl_mbps double precision not null check (dl_mbps > 0),
    ul_mbps double precision not null check (ul_mbps > 0),
    latency_ms double precision not null check (latency_ms > 0),
    loaded_at timestamptz not null default current_timestamp,
    primary key (type, year, quarter, prov_id)
);

-- Double precision rather than numeric(8,2) is more faster and in gold layer value already in decimal 2.

CREATE TABLE IF NOT EXISTS ingestion_log (
    run_id int generated always as identity primary key,
    type text not null check (type in ('mobile', 'fixed')),
    year int not null check (year >= 2019),
    quarter int not null check (quarter between 1 and 4),
    status text not null check (status in ('running','success','failed')),
    rows_loaded bigint null check (rows_loaded >= 0),
    started_at timestamptz not null default current_timestamp,
    finished_at timestamptz null,
    message text null,
    constraint finish_rules check (finished_at >= started_at)
);