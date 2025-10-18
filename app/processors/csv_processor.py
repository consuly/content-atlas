import pandas as pd
from typing import List, Dict, Any
import io


def process_csv(file_content: bytes) -> List[Dict[str, Any]]:
    """Process CSV file and return list of dictionaries."""
    df = pd.read_csv(io.BytesIO(file_content))
    return df.to_dict('records')


def process_excel(file_content: bytes) -> List[Dict[str, Any]]:
    """Process Excel file and return list of dictionaries."""
    df = pd.read_excel(io.BytesIO(file_content))
    return df.to_dict('records')
