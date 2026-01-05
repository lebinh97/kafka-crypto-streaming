import duckdb

TABLE_PATH = "/opt/consumer-output-iceberg/crypto/trades_stream"

if __name__ == "__main__":
    con = duckdb.connect()
    try:
        con.execute("INSTALL 'iceberg';")
        con.execute("LOAD 'iceberg';")
        bucket_seconds = 30
        lookback_hours = 6
        result = con.execute(
            f"""
            SELECT
                symbol,
                date_trunc('second', exchange_time_ts)
                  - ((date_diff('second', TIMESTAMP '1970-01-01', exchange_time_ts) % {bucket_seconds}) * INTERVAL 1 SECOND) AS bucket_start,
                max(exchange_time_ts) AS exchange_time_max,
                first(price ORDER BY exchange_time_ts) AS open,
                max(price) AS high,
                min(price) AS low,
                last(price ORDER BY exchange_time_ts) AS close,
                sum(size) AS volume,
                count(*) AS trades
            FROM iceberg_scan(?)
            WHERE exchange_time_ts >= now() - INTERVAL '{lookback_hours} hour'
            GROUP BY symbol, bucket_start
            ORDER BY bucket_start DESC
            """,
            [TABLE_PATH]
        ).fetchdf()
        print(result)
    except Exception as e:
        print(f"DuckDB test failed: {e}")
    finally:
        con.close()
