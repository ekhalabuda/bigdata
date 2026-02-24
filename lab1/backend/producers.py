import yfinance as yf
from kafka import KafkaProducer
import json
from datetime import datetime
import time
from threading import Thread
import pandas as pd
from typing import Optional
from models import METHODS


class BaseProducer:
    def __init__(self, kafka_host: str):
        self.producer = KafkaProducer(
            bootstrap_servers=[f"{kafka_host}:9092"],
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )


class StockDataProducer(BaseProducer):
    def __init__(self):
        super().__init__("kafka-raw")

    def get_stock_data(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        period: Optional[str] = None,
    ) -> None:
        try:
            stock = yf.Ticker(symbol)

            if start_date is not None:
                hist = stock.history(start=start_date, end=end_date)
            elif period is not None:
                hist = stock.history(period=period)
            else:
                hist = stock.history(period="1y")

            if hist.empty:
                raise ValueError(f"No data found for {symbol} in the given date range.")

            hist = hist.reset_index()
            hist["Date"] = hist["Date"].dt.strftime("%Y-%m-%d")
            hist = hist.drop(
                columns=[
                    col for col in ["Dividends", "Stock Splits"] if col in hist.columns
                ]
            )

            financial_metrics = {
                "EPS": stock.info.get("trailingEps"),
                "Revenue": stock.info.get("totalRevenue"),
                "ROE": stock.info.get("returnOnEquity"),
                "P/E": stock.info.get("trailingPE"),
            }

            for metric, value in financial_metrics.items():
                hist[metric] = value

            data = hist.to_dict("records")
            self.producer.send(
                "raw_stock_data",
                {
                    "symbol": symbol,
                    "data": data,
                    "timestamp": datetime.now().isoformat(),
                },
            )

        except Exception as e:
            self.producer.send(
                "raw_stock_data",
                {
                    "symbol": symbol,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                },
            )
        self.producer.flush()

    def get_real_time_data(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        interval: int = 60,
    ) -> None:
        try:
            stock = yf.Ticker(symbol)

            if start_date and end_date:
                hist = stock.history(start=start_date, end=end_date)
            elif start_date:
                hist = stock.history(start=start_date)
            else:
                hist = stock.history(period="1y")

            if hist.empty:
                raise ValueError(f"No data found for {symbol} in the given date range.")

            hist = hist.reset_index()
            hist["Date"] = hist["Date"].dt.strftime("%Y-%m-%d")
            data_points = hist.to_dict("records")

            for day_data in data_points:
                self.producer.send(
                    "realtime_stock_data",
                    {
                        "symbol": symbol,
                        "data": day_data,
                        "timestamp": datetime.now().isoformat(),
                    },
                )
                time.sleep(interval)

        except Exception as e:
            self.producer.send(
                "realtime_stock_data",
                {
                    "symbol": symbol,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                },
            )

    def start_realtime_streaming(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        interval: int = 60,
    ) -> None:
        Thread(
            target=self.get_real_time_data,
            args=(symbol,),
            kwargs={
                "start_date": start_date,
                "end_date": end_date,
                "interval": interval,
            },
            daemon=True,
        ).start()


class StockMetricsProducer(BaseProducer):
    def __init__(self):
        super().__init__("kafka-metrics")

    def calculate_metrics(self, df: pd.DataFrame) -> dict:

        current_price = df["Close"].iloc[-1]
        price_change = df["Close"].iloc[-1] - df["Close"].iloc[0]
        price_change_pct = (price_change / df["Close"].iloc[0]) * 100

        df["MA20"] = df["Close"].rolling(window=20).mean()
        df["MA50"] = df["Close"].rolling(window=50).mean()
        df["Daily_Return"] = df["Close"].pct_change() * 100

        return {
            "price_metrics": {
                "current_price": current_price,
                "price_change": price_change,
                "price_change_pct": price_change_pct,
                "highest_price": df["High"].max(),
                "lowest_price": df["Low"].min(),
            },
            "technical_indicators": {
                "MA20": df["MA20"].tolist(),
                "MA50": df["MA50"].tolist(),
                "Daily_Return": df["Daily_Return"].tolist(),
            },
            "summary_stats": df["Close"].describe().to_dict(),
        }

    def get_financial_metrics(self, stock: yf.Ticker) -> dict:
        info = stock.info
        return {
            "EPS": info.get("trailingEps"),
            "Revenue": info.get("totalRevenue"),
            "ROE": info.get("returnOnEquity"),
            "P/E": info.get("trailingPE"),
        }

    def process_stock_metrics(self, symbol: str, hist: pd.DataFrame) -> None:
        try:
            stock = yf.Ticker(symbol)

            metrics = self.calculate_metrics(hist)
            metrics["financial_metrics"] = self.get_financial_metrics(stock)

            self.producer.send(
                "stock_metrics",
                {
                    "symbol": symbol,
                    "metrics": metrics,
                    "timestamp": datetime.now().isoformat(),
                },
            )
        except Exception as e:
            self.producer.send(
                "stock_metrics",
                {
                    "symbol": symbol,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                },
            )
        self.producer.flush()


class StockPredictionProducer(BaseProducer):
    def __init__(self):
        super().__init__("kafka-predictions")

    def get_prediction(self, method_name: str, hist: pd.DataFrame, **kwargs) -> None:
        try:
            method = METHODS[method_name]()
            results = method.generate_prediction(hist, **kwargs)

            self.producer.send(
                "prediction_data",
                {
                    "predictions": results["future_predictions"].tolist(),
                    "mse": float(results["mse"]),
                    "mape": float(results["mape"]),
                    "timestamp": datetime.now().isoformat(),
                },
            )
        except Exception as e:
            self.producer.send(
                "prediction_data",
                {"error": str(e), "timestamp": datetime.now().isoformat()},
            )
        self.producer.flush()
