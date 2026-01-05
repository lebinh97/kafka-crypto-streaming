import time

import duckdb
import plotly.graph_objects as go
import streamlit as st


st.set_page_config(page_title="DuckDB Iceberg Candles", page_icon="📈", layout="wide")


ICEBERG_TABLE_PATH = "/opt/consumer-output-iceberg/crypto/trades_stream"
DEFAULT_LOOKBACK_HOURS = 6
BUCKET_OPTIONS = {
	"30s": 30,
	"1m": 60,
	"5m": 300,
	"30m": 1800,
}


@st.cache_resource
def get_con():
	con = duckdb.connect()
	con.execute("INSTALL 'iceberg';")
	con.execute("LOAD 'iceberg';")
	return con


def fetch_symbols(con, lookback_hours: int):
	# Return a static list to avoid scanning the table just to get symbols
	return ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD", "XRP-USD"]


def fetch_candles(con, bucket_seconds: int, lookback_hours: int, symbol: str):
	sql = f"""
    SELECT
        symbol,
        date_trunc('second', exchange_time_ts)
            - ((date_diff('second', TIMESTAMP '1970-01-01', exchange_time_ts) % {bucket_seconds}) * INTERVAL 1 SECOND) AS bucket_start,
        max(exchange_time_ts) as exchange_time_max,
        first(price ORDER BY exchange_time_ts)   AS open,
        max(price)                               AS high,
        min(price)                               AS low,
        last(price ORDER BY exchange_time_ts)    AS close,
        sum(size)                                AS volume,
        count(*)                                 AS trades
    FROM iceberg_scan('{ICEBERG_TABLE_PATH}')
    WHERE symbol = ?
        AND exchange_time_ts >= now() - INTERVAL '{lookback_hours} hours'
    GROUP BY symbol, bucket_start
    ORDER BY bucket_start DESC
	"""
	return con.execute(sql, [symbol]).fetch_df()

def render_candles(df, title: str):
	if df.empty:
		st.info("No data returned")
		return

	sub = df.sort_values("bucket_start")

	fig = go.Figure(data=[go.Candlestick(
		x=sub["bucket_start"],
		open=sub["open"],
		high=sub["high"],
		low=sub["low"],
		close=sub["close"],
		name=title
	)])
	fig.update_layout(title=title, xaxis_title="Time", yaxis_title="Price", height=500, xaxis_rangeslider_visible=False)
	st.plotly_chart(fig, use_container_width=True)

	st.dataframe(sub.sort_values("bucket_start", ascending=False).head(100), use_container_width=True, hide_index=True)


st.title("DuckDB + Iceberg Candles")

with st.sidebar:
	st.markdown("### Controls")
	lookback = st.slider("Lookback hours", 1, 48, DEFAULT_LOOKBACK_HOURS)
	candle_label = st.selectbox("Candle size", list(BUCKET_OPTIONS.keys()), index=1)
	refresh_seconds = st.slider("Refresh interval (s)", 3, 60, 10)
	auto_refresh = st.checkbox("Auto refresh", value=True)

con = get_con()

symbols = fetch_symbols(con, lookback)
if not symbols:
	st.warning("No symbols found in the selected lookback window")
else:
	selected_symbol = st.selectbox("Symbol", symbols)
	bucket_seconds = BUCKET_OPTIONS[candle_label]
	st.subheader(f"{selected_symbol} — {candle_label} candles")
	data = fetch_candles(con, bucket_seconds=bucket_seconds, lookback_hours=lookback, symbol=selected_symbol)
	render_candles(data, f"{selected_symbol} {candle_label}")

if auto_refresh:
	time.sleep(refresh_seconds)
	st.experimental_rerun()
