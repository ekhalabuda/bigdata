from models.base import BaseModel
from typing import Dict, Any
import pandas as pd
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error


class ExponentialMAMethod(BaseModel):
    def __init__(self, span: int = 20):
        self.span = span

    def generate_prediction(self, data: pd.DataFrame, **kwargs) -> Dict[str, Any]:
        span = kwargs.get("span", self.span)
        ema = data["Close"].ewm(span=span, adjust=False).mean()

        test_ratio = 0.2
        train_size = int((1 - test_ratio) * len(data))

        y_test = data["Close"][train_size:]
        y_pred = ema[train_size:]

        mse = mean_squared_error(y_test, y_pred)
        mape = mean_absolute_percentage_error(y_test, y_pred)

        return {"mse": mse, "mape": mape, "future_predictions": y_pred.values}
