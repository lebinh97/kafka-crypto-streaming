import duckdb

TABLE_PATH = "/opt/consumer-output-iceberg/crypto/trades_stream"

if __name__ == "__main__":
    con = duckdb.connect()
    try:
        con.execute("INSTALL 'iceberg';")
        con.execute("LOAD 'iceberg';")
        result = con.execute(
            "SELECT * FROM iceberg_scan(?) LIMIT 5", [TABLE_PATH]
        ).fetchdf()
        print(result)
    except Exception as e:
        print(f"DuckDB test failed: {e}")
    finally:
        con.close()
