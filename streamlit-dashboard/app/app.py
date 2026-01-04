import streamlit as st
import duckdb
import polars as pl
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
import time

st.set_page_config(
    page_title="Crypto Trading Dashboard",
    page_icon="📊",
    layout="wide"
)

def format_timestamp_utc7(ts_val):
    """Convert timestamp to UTC+7 time string"""
    try:
        import pandas as pd
        dt = pd.to_datetime(ts_val)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt_utc7 = dt.astimezone(timezone(timedelta(hours=7)))
        return dt_utc7.strftime('%H:%M:%S')
    except:
        return 'N/A'

# Timezone helper
def get_utc7_time():
    return datetime.now(timezone(timedelta(hours=7)))

def format_timestamp(ts_str):
    """Convert timestamp string to UTC+7 display format"""
    try:
        if ts_str is None or (isinstance(ts_str, float) and pl.datatypes.is_null(ts_str)):
            return 'N/A'
        dt = pl.lit(ts_str).cast(pl.Datetime).to_physical()[0]
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        return str(ts_str)

# Initialize DuckDB connection
def get_duckdb_connection():
    con = duckdb.connect(database=':memory:')
    con.install_extension("iceberg")
    con.load_extension("iceberg")
    return con

if 'duckdb_con' not in st.session_state:
    st.session_state.duckdb_con = get_duckdb_connection()

con = st.session_state.duckdb_con

st.title("📊 Crypto Trading Dashboard")
st.markdown("Real-time streaming data from Kafka → Spark → Iceberg")

# Sidebar
st.sidebar.header("Settings")
auto_refresh = st.sidebar.checkbox("🔄 Auto Refresh", value=True)
refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 1, 30, 5)

# Load trades from Iceberg using DuckDB, convert to Polars
def load_trades_data():
    try:
        query = """
        SELECT * FROM iceberg_scan('/opt/consumer-output-iceberg/crypto/trades_stream')
        ORDER BY ingest_time DESC
        """
        # Get as Polars DataFrame directly from DuckDB
        df = con.execute(query).pl()
        return df
    except Exception as e:
        st.error(f"Failed to load trades: {e}")
        return pl.DataFrame()

# Compute candles from Polars DataFrame
def compute_candles_from_df(df, timeframe, symbol):
    """Compute OHLCV candles using Polars group_by_dynamic"""
    
    intervals = {
        "30s": "30s",
        "1m": "1m",
        "5m": "5m",
        "30m": "30m",
        "1h": "1h"
    }
    
    interval = intervals.get(timeframe, "5m")
    
    try:
        if df.is_empty():
            return pl.DataFrame()
        
        # Filter by symbol and compute candles
        candles = (
            df.filter(pl.col("symbol") == symbol)
            .sort("exchange_time_ts")
            .group_by_dynamic("exchange_time_ts", every=interval)
            .agg([
                pl.col("price").first().round(2).alias("open_price"),
                pl.col("price").max().round(2).alias("high_price"),
                pl.col("price").min().round(2).alias("low_price"),
                pl.col("price").last().round(2).alias("close_price"),
                pl.col("size").sum().round(2).alias("volume"),
                pl.col("trade_id").n_unique().alias("trade_count"),
            ])
            .rename({"exchange_time_ts": "candle_time"})
            .with_columns(pl.lit(symbol).alias("symbol"))
            .sort("candle_time", descending=True)
        )
        
        return candles
    except Exception as e:
        st.error(f"Failed to compute candles: {e}")
        return pl.DataFrame()

# Load data
df_trades = load_trades_data()

if not df_trades.is_empty():
    # Metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Trades", f"{len(df_trades):,}")
    
    with col2:
        avg_price = df_trades["price"].mean()
        st.metric("Avg Price", f"${avg_price:,.2f}")
    
    with col3:
        total_volume = df_trades["size"].sum()
        st.metric("Total Volume", f"{total_volume:,.2f}")
    
    with col4:
        symbols = df_trades["symbol"].n_unique()
        st.metric("Symbols", symbols)
    
    with col5:
        latest_ingest = 'N/A'
        if "ingest_time" in df_trades.columns and len(df_trades) > 0:
            latest_ingest = format_timestamp_utc7(df_trades["ingest_time"][0])
        st.metric("Latest Ingest", latest_ingest)
    
    # Tabs
    tab1, tab2, tab3 = st.tabs(["📈 Recent Trades", "🕯️ Candles", "📊 Analytics"])
    
    with tab1:
        st.subheader("Recent Trades")
        
        # Convert to pandas for display
        df_trades_pd = df_trades.to_pandas()
        df_trades_display = df_trades_pd.copy()
        
        if 'exchange_time' in df_trades_display.columns:
            df_trades_display['exchange_time'] = df_trades_display['exchange_time'].apply(lambda x: format_timestamp(x) if x else 'N/A')
        if 'ingest_time' in df_trades_display.columns:
            df_trades_display['ingest_time'] = df_trades_display['ingest_time'].apply(lambda x: format_timestamp(x) if x else 'N/A')
        
        st.dataframe(
            df_trades_display[['exchange_time', 'symbol', 'side', 'price', 'size', 'trade_id']].head(100),
            use_container_width=True,
            hide_index=True
        )
    
    with tab2:
        st.subheader("OHLCV Candles (Polars + DuckDB)")
        
        col1, col2 = st.columns([1, 3])
        with col1:
            timeframe = st.selectbox(
                "Select Timeframe",
                ["30s", "1m", "5m", "30m", "1h"],
                index=2
            )
        
        with col2:
            symbol_list = df_trades["symbol"].unique().to_list()
            selected_symbol = st.selectbox(
                "Select Symbol",
                symbol_list if symbol_list else []
            )
        
        # Compute candles from in-memory Polars DataFrame
        df_candles = compute_candles_from_df(df_trades, timeframe, selected_symbol)
        
        if not df_candles.is_empty():
            # Convert to pandas for Plotly
            df_candles_pd = df_candles.to_pandas()
            df_candles_display = df_candles_pd.copy()
            
            if 'candle_time' in df_candles_display.columns:
                df_candles_display['candle_time_utc7'] = df_candles_display['candle_time'].apply(lambda x: format_timestamp(x) if x else 'N/A')
            
            df_candles_sorted = df_candles_pd.sort_values('candle_time')
            
            fig = go.Figure(data=[go.Candlestick(
                x=df_candles_sorted['candle_time'],
                open=df_candles_sorted['open_price'],
                high=df_candles_sorted['high_price'],
                low=df_candles_sorted['low_price'],
                close=df_candles_sorted['close_price'],
                name=selected_symbol
            )])
            
            fig.update_layout(
                title=f"{selected_symbol} - {timeframe.upper()} (Polars)",
                xaxis_title="Time",
                yaxis_title="Price (USD)",
                height=500,
                xaxis_rangeslider_visible=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            st.dataframe(
                df_candles_display[['candle_time_utc7', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'trade_count']].head(50).rename(columns={'candle_time_utc7': 'candle_time'}),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info(f"No data available for {selected_symbol}")
    
    with tab3:
        st.subheader("Price Distribution")
        
        df_trades_pd = df_trades.to_pandas()
        
        fig = go.Figure()
        fig.add_trace(go.Histogram(x=df_trades_pd['price'], nbinsx=50, name='Price'))
        fig.update_layout(
            xaxis_title="Price (USD)",
            yaxis_title="Count",
            showlegend=False,
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Volume by Symbol")
        volume_by_symbol = df_trades_pd.groupby('symbol')['size'].sum().sort_values(ascending=False).head(10)
        
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(x=volume_by_symbol.index, y=volume_by_symbol.values))
        fig2.update_layout(
            xaxis_title="Symbol",
            yaxis_title="Total Volume",
            showlegend=False,
            height=400
        )
        st.plotly_chart(fig2, use_container_width=True)

else:
    st.warning("No data available. Check if Kafka consumer is running.")

# Footer
st.markdown("---")
col1, col2 = st.columns([3, 1])
with col1:
    now_utc7 = get_utc7_time().strftime('%Y-%m-%d %H:%M:%S')
    st.caption(f"Last updated: {now_utc7} (UTC+7)")
with col2:
    if auto_refresh:
        st.caption(f"⚡ Auto-refresh: {refresh_interval}s")

# Auto-refresh
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
