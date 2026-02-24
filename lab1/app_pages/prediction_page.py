import streamlit as st
import plotly.graph_objects as go
import pandas as pd


def ml_prediction_page():
    if "processor" in st.session_state:
        processor = st.session_state["processor"]
    st.header("Stock Price Prediction")

    symbol = st.text_input(
        "Enter stock symbol (e.g., AAPL, MSFT, GOOGL, AMZN, TSLA, META):"
    )
    method = st.selectbox(
        "Select prediction method:", ["Linear Regression", "Exponential MA"]
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

    kwargs = {}
    if method == "Exponential MA":
        span = st.slider("Select EMA span:", min_value=5, max_value=50, value=20)
        kwargs["span"] = span

    if st.button("Predict"):
        if symbol:
            with st.spinner("Generating prediction..."):
                processor.data_producer.get_stock_data(symbol, **date_params)
                stock_data = processor.stock_consumer.get_latest_data()

                if stock_data and stock_data["symbol"] == symbol:
                    df = pd.DataFrame(stock_data["data"])

                    processor.prediction_producer.get_prediction(method, df, **kwargs)

                    prediction_data = processor.prediction_consumer.get_latest_data()
                    if prediction_data:
                        predictions = prediction_data["predictions"]
                        mse = prediction_data["mse"]
                        mape = prediction_data["mape"]

                        fig = go.Figure()

                        fig.add_trace(
                            go.Scatter(
                                x=pd.to_datetime(df["Date"]),
                                y=df["Close"],
                                name="Historical",
                            )
                        )

                        pred_dates = df["Date"][-len(predictions) :]
                        fig.add_trace(
                            go.Scatter(x=pred_dates, y=predictions, name="Prediction")
                        )

                        fig.update_layout(
                            title=f"Stock Price Prediction for {symbol}",
                            xaxis_title="Date",
                            yaxis_title="Price",
                            hovermode="x unified",
                        )

                        st.plotly_chart(fig)

                        metrics_col1, metrics_col2 = st.columns(2)
                        with metrics_col1:
                            if mse is not None:
                                st.metric("Mean Squared Error", f"{mse:.4f}")
                        with metrics_col2:
                            if mape is not None:
                                st.metric(
                                    "Mean Absolute Percentage Error", f"{mape:.2f}%"
                                )
                    else:
                        st.error("No prediction data received from Kafka")
                else:
                    st.error("No stock data received from Kafka")
