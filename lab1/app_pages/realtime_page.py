import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from time import sleep
from datetime import datetime, timedelta


def realtime_page():
    if "processor" in st.session_state:
        processor = st.session_state["processor"]
    st.header("Real-Time Stock Data Visualization")

    col1, col2, col3 = st.columns(3)

    with col1:
        symbol = st.text_input("Enter stock symbol (e.g., AAPL):", "AAPL")

    with col2:
        start_date = st.date_input(
            "Start date", value=datetime.now() - timedelta(days=30)
        ).strftime("%Y-%m-%d")

    with col3:
        end_date = st.date_input("End date (optional)", value=None)
        end_date = end_date.strftime("%Y-%m-%d") if end_date else None

    update_interval = st.slider(
        "Update interval (seconds)", min_value=1, max_value=60, value=5
    )

    if "historical_data" not in st.session_state:
        st.session_state["historical_data"] = []

    if "is_streaming" not in st.session_state:
        st.session_state["is_streaming"] = False

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Start Streaming"):
            st.session_state["is_streaming"] = True
            st.session_state["historical_data"] = []

    with col2:
        if st.button("Stop Streaming"):
            st.session_state["is_streaming"] = False

    if "metric_placeholder" not in st.session_state:
        st.session_state["metric_placeholder"] = st.empty()
    if "price_chart" not in st.session_state:
        st.session_state["price_chart"] = st.empty()

    if st.session_state["is_streaming"]:
        processor.data_producer.start_realtime_streaming(
            symbol, start_date=start_date, end_date=end_date, interval=update_interval
        )

        while st.session_state["is_streaming"]:

            new_data = processor.realtime_consumer.get_latest_data()

            if new_data and "data" in new_data:
                st.session_state["historical_data"].append(
                    {
                        "Date": pd.to_datetime(new_data["data"]["Date"]),
                        "Close": new_data["data"]["Close"],
                    }
                )

                df = pd.DataFrame(st.session_state["historical_data"])
                df["Returns"] = df["Close"].pct_change() * 100

                fig = go.Figure()
                fig.add_trace(
                    go.Scatter(
                        x=df["Date"],
                        y=df["Close"],
                        name="Price",
                        line=dict(color="blue"),
                    )
                )

                fig.update_layout(
                    title=f"{symbol} Stock Price",
                    xaxis_title="Date",
                    yaxis_title="Price",
                    height=250,
                    showlegend=True,
                    legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
                )

                st.session_state["price_chart"].plotly_chart(
                    fig, use_container_width=True
                )
                st.session_state["metric_placeholder"].metric(
                    "Current Returns (%)",
                    f"{df['Returns'].iloc[-1]:.2f}",
                    (
                        f"{df['Returns'].iloc[-1] - df['Returns'].iloc[-2]:.2f}"
                        if len(df) > 1
                        else 0.0
                    ),
                )
            else:
                st.error(new_data)
            sleep(1)
