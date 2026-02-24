from typing import Dict, Any
import pandas as pd


class BaseModel:
    def generate_prediction(self, data: pd.DataFrame, **kwargs) -> Dict[str, Any]:
        raise NotImplementedError("Subclasses must implement this method")
