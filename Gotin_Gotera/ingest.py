import os
from pathlib import Path
from typing import List
import pandas as pd
from Gotin_Gotera import minsearch

DATA_PATH = os.getenv("DATA_PATH", "DATASETS/faq_sacmex.csv")

def ingest_data(file_path: Path = Path(DATA_PATH), text_fields: List[str] = None):
    # Lee el archivo csv
    df = pd.read_csv(file_path)

    print(df.head())

    # Convierte los campos a string
    if text_fields:
        for field in text_fields:
            if field in df.columns:
                df[field] = df[field].astype(str)

    # Convierte el DataFrame a diccionario
    return df.to_dict(orient='records')


def load_index(
        documents,
        text_fields: List[str],
        keyword_fields: List[str],
        search_index=minsearch.Index
):
    for doc in documents:
        for key in doc:
            if isinstance(doc[key], float):
                doc[key] = str(doc[key])
            elif isinstance(doc[key], int):
                doc[key] = str(doc[key])
            elif not isinstance(doc[key], str):
                doc[key] = str(doc[key])
            else:
                doc[key] = doc[key]

    index = search_index(
        text_fields=text_fields,
        keyword_fields=keyword_fields
    )

    index.fit(documents)

    return index