# app/services/forecast.py

"""
Path: app/services/forecast.py

Demand forecasting service for store replenishment.
Implements multiple forecasting methods (moving average, exponential smoothing, ARIMA with auto-parameter selection),
business adjustments, and forecast accuracy measurement.
"""

from typing import Optional, List, Dict
import pandas as pd
import numpy as np
from statsmodels.tsa.holtwinters import SimpleExpSmoothing
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import adfuller
from sklearn.metrics import mean_squared_error

# --- 1. Fetch sales history as a pandas Series ---

def get_sales_series(session, store_id: int, sku_id: int, days: int = 90) -> pd.Series:
    """
    Fetch sales history for a given store and SKU as a pandas Series.
    """
    from datetime import date, timedelta
    from app.models.sales_history import SalesHistory

    cutoff = date.today() - timedelta(days=days)
    records = (
        session.query(SalesHistory)
        .filter(
            SalesHistory.store_id == store_id,
            SalesHistory.sku_id == sku_id,
            SalesHistory.sale_date >= cutoff,
        )
        .order_by(SalesHistory.sale_date)
        .all()
    )
    if not records:
        return pd.Series(dtype=float)
    return pd.Series(
        [r.quantity_sold for r in records],
        index=[r.sale_date for r in records]
    )

# --- 2. Forecasting Models ---

def moving_average_forecast(sales_series: pd.Series, window: int = 7) -> float:
    """
    Forecast next period using a simple moving average.
    """
    if len(sales_series) == 0:
        return 0
    return sales_series.tail(window).mean()

def exponential_smoothing_forecast(sales_series: pd.Series, smoothing_level: float = 0.2) -> float:
    """
    Forecast using simple exponential smoothing.
    """
    if len(sales_series) < 2:
        return sales_series.mean() if len(sales_series) else 0
    model = SimpleExpSmoothing(sales_series).fit(smoothing_level=smoothing_level, optimized=False)
    return model.forecast(1)[0]

def check_stationarity(series: pd.Series, alpha: float = 0.05) -> bool:
    """
    Returns True if the series is stationary according to the Augmented Dickey-Fuller test.
    """
    if len(series.dropna()) < 3:
        return True  # Not enough data to test, assume stationary
    result = adfuller(series.dropna())
    return result[1] < alpha

def difference_until_stationary(series: pd.Series, max_d: int = 2):
    """
    Difference the series until it becomes stationary or max_d is reached.
    Returns differenced series and the order of differencing.
    """
    d = 0
    diffed = series.copy()
    while not check_stationarity(diffed) and d < max_d:
        diffed = diffed.diff().dropna()
        d += 1
    return diffed, d

def select_arima_order(series: pd.Series, p_values=range(0, 3), d_values=range(0, 2), q_values=range(0, 3)):
    """
    Grid search to select the best (p,d,q) order based on AIC.
    """
    best_aic = np.inf
    best_order = None
    best_model = None
    for p in p_values:
        for d in d_values:
            for q in q_values:
                try:
                    model = ARIMA(series, order=(p, d, q))
                    model_fit = model.fit()
                    if model_fit.aic < best_aic:
                        best_aic = model_fit.aic
                        best_order = (p, d, q)
                        best_model = model_fit
                except Exception:
                    continue
    return best_order, best_model

def arima_forecast(
    sales_series: pd.Series,
    auto_select: bool = True,
    forecast_steps: int = 1
) -> float:
    """
    Improved ARIMA forecast with parameter selection and diagnostics.
    - auto_select: if True, automatically selects (p,d,q) using grid search.
    - forecast_steps: number of periods to forecast ahead (default 1).
    Returns the forecast value.
    """
    # Handle short series
    if len(sales_series) < 10:
        return sales_series.mean() if len(sales_series) else 0

    # Step 1: Make series stationary if needed
    diffed, d = difference_until_stationary(sales_series)
    # If not enough data after differencing, fallback to mean
    if len(diffed) < 8:
        return sales_series.mean()

    # Step 2: Parameter selection
    if auto_select:
        order, model_fit = select_arima_order(sales_series, p_values=range(0, 3), d_values=range(d, d+1), q_values=range(0, 3))
        if model_fit is None:
            # Fallback to ARIMA(1,d,1)
            order = (1, d, 1)
            model_fit = ARIMA(sales_series, order=order).fit()
    else:
        order = (1, d, 1)
        model_fit = ARIMA(sales_series, order=order).fit()

    # Step 3: Forecast
    forecast = model_fit.forecast(steps=forecast_steps)
    forecast_value = float(forecast.iloc[-1])

    # Step 4: Diagnostics (optional, can be returned as needed)
    # residuals = model_fit.resid
    # rmse = np.sqrt(mean_squared_error(sales_series[-len(residuals):], model_fit.fittedvalues))
    # aic = model_fit.aic
    # bic = model_fit.bic
    # diagnostics = {
    #     'order': order,
    #     'aic': aic,
    #     'bic': bic,
    #     'rmse': rmse,
    #     'residuals': residuals
    # }

    return forecast_value

# --- 3. Business Adjustments (promotions, seasonality, manual override) ---

def apply_business_adjustments(
    forecast: float,
    promo_factor: float = 1.0,
    season_factor: float = 1.0,
    manual_override: Optional[float] = None
) -> float:
    """
    Apply business adjustments to the forecast.
    """
    if manual_override is not None:
        return manual_override
    return forecast * promo_factor * season_factor

# --- 4. Forecast Accuracy Measurement (MAPE) ---

def calculate_mape(actuals: List[float], forecasts: List[float]) -> Optional[float]:
    """
    Calculate Mean Absolute Percentage Error.
    """
    actuals, forecasts = np.array(actuals), np.array(forecasts)
    mask = actuals != 0
    return np.mean(np.abs((actuals[mask] - forecasts[mask]) / actuals[mask])) * 100 if np.any(mask) else None

# --- 5. Main Forecasting Service ---

def forecast_demand(
    session,
    store_id: int,
    sku_id: int,
    model: str = 'moving_average',
    window: int = 7,
    smoothing_level: float = 0.2,
    promo_factor: float = 1.0,
    season_factor: float = 1.0,
    manual_override: Optional[float] = None
) -> float:
    """
    Generate a demand forecast for a given store and SKU.
    """
    sales_series = get_sales_series(session, store_id, sku_id)
    if model == 'moving_average':
        forecast = moving_average_forecast(sales_series, window)
    elif model == 'exponential_smoothing':
        forecast = exponential_smoothing_forecast(sales_series, smoothing_level)
    elif model == 'arima':
        forecast = arima_forecast(sales_series)
    else:
        raise ValueError("Unknown model type")
    forecast = apply_business_adjustments(forecast, promo_factor, season_factor, manual_override)
    return max(0, round(forecast))

# --- 6. Batch Forecast for All SKUs/Stores ---

def batch_forecast_all(session, model: str = 'moving_average') -> List[Dict]:
    """
    Generate forecasts for all store/SKU pairs.
    """
    from app.models.store import Store
    from app.models.sku import SKU

    stores = session.query(Store).all()
    skus = session.query(SKU).all()
    results = []
    for store in stores:
        for sku in skus:
            forecast = forecast_demand(session, store.id, sku.id, model=model)
            results.append({
                'store_id': store.id,
                'sku_id': sku.id,
                'forecast': forecast
            })
    return results

# --- 7. Store Forecasts in Database ---

def save_forecast(
    session,
    store_id: int,
    sku_id: int,
    forecast_qty: float,
    model_used: str,
    promo_factor: float = 1.0,
    season_factor: float = 1.0,
    manual_override: Optional[float] = None
):
    """
    Save a forecast record to the database.
    """
    from datetime import date
    from app.models.forecast import Forecast

    forecast = Forecast(
        store_id=store_id,
        sku_id=sku_id,
        forecast_date=date.today(),
        forecast_qty=forecast_qty,
        model_used=model_used,
        promo_factor=promo_factor,
        season_factor=season_factor,
        manual_override=manual_override,
    )
    session.add(forecast)
    session.commit()
