create table if not exists outbox
(
    id           bigint generated always as identity,
    grouping_key text not null,
    processed_at timestamptz
) partition by list (processed_at);

create table if not exists outbox_unpublished partition of outbox for values in (null) with
(
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_analyze_threshold = 10000,
    vacuum_truncate = false,
    toast.vacuum_truncate = false
);

create table if not exists outbox_published partition of outbox default with
(
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_vacuum_insert_scale_factor = 0,
    autovacuum_vacuum_insert_threshold = 1000000,
    autovacuum_analyze_threshold = 1000000,
    autovacuum_vacuum_threshold = 1000000
);

create index if not exists outbox_unpublished__id__idx on outbox_unpublished(id);
