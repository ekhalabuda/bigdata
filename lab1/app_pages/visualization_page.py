import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import time

def visualization_page():
    if "processor" in st.session_state:
        processor = st.session_state["processor"]
    st.header("Stock Data Visualization")

    symbol = st.text_input(
        "Enter stock symbol (e.g., AAPL, MSFT, GOOGL, AMZN, TSLA, META):"
    )
    date_option = st.radio(
        "Choose date range type:", ["Predefined Period", "Custom Range"]
    )

    if date_option == "Predefined Period":
        period = st.selectbox(
            "Select time period:",
            ["1mo", "3mo", "6mo", "1y"],
            format_func=lambda x: {
                "1mo": "1 Month",
                "3mo": "3 Months",
                "6mo": "6 Months",
                "1y": "1 Year",
            }[x],
        )
        date_params = {"period": period}
    else:
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date")
        with col2:
            end_date = st.date_input("End Date")
        date_params = {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        }

    if not st.button("Get Data"):
        return
    if not symbol:
        return

    with st.spinner("Getting data..."):
        processor.data_producer.get_stock_data(symbol, **date_params)
        time.sleep(10)
        data = processor.stock_consumer.get_latest_data()

    if data is None:
        st.error(f"None data received.")
        return
    if data["symbol"] != symbol:
        st.error(f"Wrong symbol received.")
        return

    with st.spinner("Computing stock metrics..."):
        df = pd.DataFrame(data["data"])
        processor.metrics_producer.process_stock_metrics(symbol, df)
        metrics = processor.metrics_consumer.get_latest_data()

    if metrics is None or "metrics" not in metrics:
        st.error(metrics)
        return

    with st.spinner("Visualizing metrics..."):
        metrics = metrics["metrics"]
        st.subheader("Price Analysis")
        col1, col2 = st.columns(2)

        with col1:
            fig_candlestick = go.Figure(
                data=[
                    go.Candlestick(
                        x=df.index,
                        open=df["Open"],
                        high=df["High"],
                        low=df["Low"],
                        close=df["Close"],
                        name="OHLC",
                    )
                ]
            )
            fig_candlestick.update_layout(
                title="Candlestick Chart", height=400
            )
            st.plotly_chart(fig_candlestick, use_container_width=True)

        with col2:
            fig_volume = go.Figure(
                data=[go.Bar(x=df.index, y=df["Volume"], name="Volume")]
            )
            fig_volume.update_layout(title="Trading Volume", height=400)
            st.plotly_chart(fig_volume, use_container_width=True)

        st.subheader("Key Statistics")
        col1, col2, col3, col4 = st.columns(4)

        price_metrics = metrics["price_metrics"]
        with col1:
            st.metric(
                "Current Price",
                f"${price_metrics['current_price']:.2f}",
            )
        with col2:
            st.metric(
                "Price Change",
                f"${price_metrics['price_change']:.2f}",
                f"{price_metrics['price_change_pct']:.1f}%",
            )
        with col3:
            st.metric(
                "Highest Price",
                f"${price_metrics['highest_price']:.2f}",
            )
        with col4:
            st.metric(
                "Lowest Price", f"${price_metrics['lowest_price']:.2f}"
            )

        st.subheader("Technical Indicators")
        col1, col2 = st.columns(2)

        with col1:
            fig_ma = go.Figure()
            fig_ma.add_trace(
                go.Scatter(x=df.index, y=df["Close"], name="Price")
            )
            fig_ma.add_trace(
                go.Scatter(
                    x=df.index,
                    y=metrics["technical_indicators"]["MA20"],
                    name="20-day MA",
                )
            )
            fig_ma.add_trace(
                go.Scatter(
                    x=df.index,
                    y=metrics["technical_indicators"]["MA50"],
                    name="50-day MA",
                )
            )
            fig_ma.update_layout(title="Moving Averages", height=400)
            st.plotly_chart(fig_ma, use_container_width=True)

        with col2:
            fig_returns = go.Figure(
                data=[
                    go.Histogram(
                        x=metrics["technical_indicators"][
                            "Daily_Return"
                        ],
                        nbinsx=30,
                        name="Daily Returns",
                    )
                ]
            )
            fig_returns.update_layout(
                title="Daily Returns Distribution", height=400
            )
            st.plotly_chart(fig_returns, use_container_width=True)

        st.subheader("Summary Statistics")
        st.dataframe(pd.Series(metrics["summary_stats"]))

        fin_metrics = metrics["financial_metrics"]
        if all(pd.notna(v) for v in fin_metrics.values()):
            st.subheader("Financial Metrics")
            fin_col1, fin_col2, fin_col3, fin_col4 = st.columns(4)

            with fin_col1:
                st.metric(
                    "EPS",
                    (
                        f"${fin_metrics['EPS']:.2f}"
                        if pd.notna(fin_metrics["EPS"])
                        else "N/A"
                    ),
                )
            with fin_col2:
                revenue = fin_metrics["Revenue"]
                st.metric(
                    "Revenue",
                    (
                        f"${revenue/1e9:.1f}B"
                        if pd.notna(revenue)
                        else "N/A"
                    ),
                )
            with fin_col3:
                roe = fin_metrics["ROE"]
                st.metric(
                    "ROE", f"{roe*100:.1f}%" if pd.notna(roe) else "N/A"
                )
            with fin_col4:
                pe = fin_metrics["P/E"]
                st.metric(
                    "P/E Ratio", f"{pe:.2f}" if pd.notna(pe) else "N/A"
                )
