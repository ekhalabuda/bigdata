from models.base import BaseModel
from typing import Dict, Any
from sklearn.linear_model import LinearRegression
import pandas as pd
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error


class RegressionMethod(BaseModel):
    def __init__(
        self,
    ):
        self.model = LinearRegression()

    def generate_prediction(self, data: pd.DataFrame, **kwargs) -> Dict[str, Any]:
        features = ["High", "Low", "Open", "Volume", "EPS", "Revenue", "ROE", "P/E"]
        X = data[features]
        y = data["Close"]

        test_ratio = 0.2
        training_ratio = 1 - test_ratio

        train_size = int(training_ratio * len(y))

        X_train, X_test, y_train, y_test = (
            X[:train_size],
            X[train_size:],
            y[:train_size],
            y[train_size:],
        )

        self.model.fit(X_train, y_train)
        y_pred = self.model.predict(X_test)

        mse = mean_squared_error(y_test, y_pred)
        mape = mean_absolute_percentage_error(y_test, y_pred)

        return {"mse": mse, "mape": mape, "future_predictions": y_pred}
