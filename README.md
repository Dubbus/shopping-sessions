# shopping-sessions

Rust pipeline for the Databricks "Shopping Sessions" buildathon track: cleans click-stream CSVs,
links user IDs into people, builds sessions, attributes them to ad clicks, and writes two daily
Parquet tables.

    make build && make fixtures      # 36 edge-case fixtures
    make run-dev && make check-dev   # 1M-event dev set (generate it with the track's generator)
