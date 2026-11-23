-- Сохраняет экспорт одного нативного GP snapshot для повторяемого расчёта и сверок после закрытия GP-транзакции.
CREATE TABLE dlh.bronze.__source_snapshots (
    export_id varchar,
    object_symbol varchar,
    row_no bigint,
    row_json varchar
) WITH (format='PARQUET', format_version=2);
