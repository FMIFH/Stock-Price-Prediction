import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker

from models.database import StockPrice
from models.settings import postgresql_settings

# Page config
st.set_page_config(page_title="Stock Price Dashboard", layout="wide")

# Database connection
@st.cache_resource
def get_db_engine():
    db_url = f"postgresql://{postgresql_settings.POSTGRES_USER}:{postgresql_settings.POSTGRES_PASSWORD}@{postgresql_settings.POSTGRES_HOST}:{postgresql_settings.POSTGRES_PORT}/{postgresql_settings.POSTGRES_DB}"
    return create_engine(db_url)

def load_data(symbol=None, limit=1000):
    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        query = session.query(StockPrice)
        if symbol:
            query = query.filter(StockPrice.symbol == symbol)
        query = query.order_by(desc(StockPrice.timestamp)).limit(limit)

        results = query.all()

        data = []
        for row in results:
            data.append({
                'symbol': row.symbol,
                'timestamp': row.timestamp,
                'open': row.open,
                'high': row.high,
                'low': row.low,
                'close': row.close,
                'volume': row.volume,
                'moving_avg_5': row.moving_avg_5,
                'moving_avg_30': row.moving_avg_30,
                'moving_avg_365': row.moving_avg_365,
            })

        df = pd.DataFrame(data)
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp')
        return df
    finally:
        session.close()

def get_available_symbols():
    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        symbols = session.query(StockPrice.symbol).distinct().all()
        return [s[0] for s in symbols]
    finally:
        session.close()

# Main app
st.title("📈 Stock Price Dashboard")

# Sidebar
st.sidebar.header("Filters")
symbols = get_available_symbols()

if not symbols:
    st.error("No data found in database!")
    st.stop()

selected_symbol = st.sidebar.selectbox("Select Stock Symbol", symbols)
data_limit = st.sidebar.slider("Number of records to load", 100, 5000, 1000, 100)

# Load data
with st.spinner("Loading data..."):
    df = load_data(selected_symbol, data_limit)

if df.empty:
    st.warning(f"No data found for {selected_symbol}")
    st.stop()

# Display metrics
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Latest Close", f"${df.iloc[-1]['close']:.2f}")
with col2:
    change = df.iloc[-1]['close'] - df.iloc[-2]['close'] if len(df) > 1 else 0
    pct_change = (change / df.iloc[-2]['close'] * 100) if len(df) > 1 and df.iloc[-2]['close'] != 0 else 0
    st.metric("Change", f"${change:.2f}", f"{pct_change:.2f}%")
with col3:
    st.metric("Volume", f"{df.iloc[-1]['volume']:,}")
with col4:
    st.metric("Records", f"{len(df):,}")

# Create candlestick chart with moving averages
fig = make_subplots(
    rows=2, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.03,
    row_heights=[0.7, 0.3],
    subplot_titles=(f'{selected_symbol} Price Chart', 'Volume')
)

# Candlestick chart
fig.add_trace(
    go.Candlestick(
        x=df['timestamp'],
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        name='Price'
    ),
    row=1, col=1
)

# Moving averages
if df['moving_avg_5'].notna().any():
    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['moving_avg_5'],
                  name='MA 5', line=dict(color='orange', width=1)),
        row=1, col=1
    )

if df['moving_avg_30'].notna().any():
    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['moving_avg_30'],
                  name='MA 30', line=dict(color='blue', width=1)),
        row=1, col=1
    )

if df['moving_avg_365'].notna().any():
    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['moving_avg_365'],
                  name='MA 365', line=dict(color='red', width=1)),
        row=1, col=1
    )

# Volume bars
fig.add_trace(
    go.Bar(x=df['timestamp'], y=df['volume'], name='Volume', marker_color='lightblue'),
    row=2, col=1
)

fig.update_layout(
    height=800,
    xaxis_rangeslider_visible=False,
    hovermode='x unified',
    showlegend=True
)

fig.update_xaxes(title_text="Date", row=2, col=1)
fig.update_yaxes(title_text="Price ($)", row=1, col=1)
fig.update_yaxes(title_text="Volume", row=2, col=1)

st.plotly_chart(fig, use_container_width=True)

# Data table
st.subheader("Recent Data")
st.dataframe(
    df.tail(20).sort_values('timestamp', ascending=False),
    use_container_width=True,
    hide_index=True
)

# Statistics
st.subheader("Statistics")
col1, col2 = st.columns(2)

with col1:
    st.write("**Price Statistics**")
    stats_df = df[['open', 'high', 'low', 'close']].describe()
    st.dataframe(stats_df, use_container_width=True)

with col2:
    st.write("**Volume Statistics**")
    vol_stats = df['volume'].describe().to_frame(name='Volume')
    st.dataframe(vol_stats, use_container_width=True)
