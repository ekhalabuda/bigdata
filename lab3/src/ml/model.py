"""Обучение регрессионных моделей (LinearRegression и RandomForestRegressor) с MLflow."""
import logging
import numpy as np
import pandas as pd
import polars as pl
from deltalake import DeltaTable
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
import mlflow
import mlflow.sklearn
import matplotlib.pyplot as plt
import tempfile

logger = logging.getLogger(__name__)

def run_regression_models(feature_table_path: str):
    logger.info("Loading feature table for regression models")
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("FlightDelayRegression")

    # Загрузка данных через Polars, затем в pandas
    data = pl.read_delta(feature_table_path).to_pandas()
    logger.info(f"Feature table shape: {data.shape}")

    # Кодирование категорий
    encoders = {
        "Airline": LabelEncoder(),
        "Origin": LabelEncoder(),
        "Dest": LabelEncoder()
    }
    for col, le in encoders.items():
        data[f"{col}_enc"] = le.fit_transform(data[col])

    features = ["DepartureDelay", "hour", "Distance", "Airline_enc", "Origin_enc", "Dest_enc"]
    target = data["ArrivalDelay"]

    X_train, X_test, y_train, y_test = train_test_split(
        data[features], target, test_size=0.2, random_state=42
    )

    # Версия золотой таблицы
    dt = DeltaTable(feature_table_path)
    gold_version = dt.version()

    # ------------------------------
    # Линейная регрессия
    # ------------------------------
    with mlflow.start_run(run_name="LinearRegression"):
        mlflow.log_param("gold_table_version", gold_version)
        mlflow.log_param("model_type", "LinearRegression")
        mlflow.log_param("features", features)

        model = LinearRegression()
        model.fit(X_train, y_train)
        predictions = model.predict(X_test)

        mse = mean_squared_error(y_test, predictions)
        r2 = r2_score(y_test, predictions)

        mlflow.log_metric("mse", mse)
        mlflow.log_metric("r2", r2)

        for f, coef in zip(features, model.coef_):
            mlflow.log_param(f"coef_{f}", coef)
        mlflow.log_param("intercept", model.intercept_)

        mlflow.sklearn.log_model(model, "model")
        logger.info(f"LinearRegression: MSE={mse:.3f}, R²={r2:.3f}")

    # ------------------------------
    # Random Forest Regressor
    # ------------------------------
    with mlflow.start_run(run_name="RandomForestRegressor"):
        mlflow.log_param("gold_table_version", gold_version)
        mlflow.log_param("model_type", "RandomForestRegressor")
        mlflow.log_param("n_estimators", 100)
        mlflow.log_param("features", features)

        rf = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
        rf.fit(X_train, y_train)
        predictions = rf.predict(X_test)

        mse = mean_squared_error(y_test, predictions)
        r2 = r2_score(y_test, predictions)

        mlflow.log_metric("mse", mse)
        mlflow.log_metric("r2", r2)

        # Важность признаков
        for f, imp in zip(features, rf.feature_importances_):
            mlflow.log_metric(f"importance_{f}", imp)

        # График предсказаний
        fig, ax = plt.subplots(figsize=(6, 5))
        ax.scatter(y_test, predictions, alpha=0.3)
        ax.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--')
        ax.set_xlabel("Actual Arrival Delay (min)")
        ax.set_ylabel("Predicted Arrival Delay (min)")
        ax.set_title(f"Random Forest (R²={r2:.3f})")
        with tempfile.NamedTemporaryFile(suffix=".png") as tmp:
            plt.savefig(tmp.name)
            mlflow.log_artifact(tmp.name, "plots")
        plt.close()

        mlflow.sklearn.log_model(rf, "model")
        logger.info(f"RandomForestRegressor: MSE={mse:.3f}, R²={r2:.3f}")

    logger.info("All models trained and logged")
