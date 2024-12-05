import os
import pandas as pd
import json
from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch
from tqdm.auto import tqdm
from dotenv import load_dotenv
from pathlib import Path
from typing import List
from db import init_db
from ingest import ingest_data, load_index

load_dotenv()

ELASTIC_URL = os.getenv("ELASTIC_URL_LOCAL", "http://localhost:9200")
MODEL_NAME = os.getenv("MODEL_NAME", "multi-qa-MiniLM-L6-cos-v1")
INDEX_NAME = os.getenv("INDEX_NAME", "Gotin_Gotera_Documents")
FILE_DIR = os.getenv("DOCS_DIR", "./documents")

def load_model():
    print(f"Loading model: {MODEL_NAME}")
    return SentenceTransformer(MODEL_NAME)


def setup_elasticsearch():
    print("Setting up Elasticsearch...")
    es_client = Elasticsearch(ELASTIC_URL)
    
    index_settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "properties":
                {
                    'id': {"type": "keyword"},
                    'pregunta': {"type": "text"},
                    'respuesta': {"type": "text"},
                    'document': {"type": "text"},
                    'pregunta_vector': {"type": "dense_vector", "dims": 768, "index": True, "similarity": "cosine"},
                    'respuesta_vector': {"type": "dense_vector", "dims": 768, "index": True, "similarity": "cosine"},
                    'preg_resp_vector': {"type": "dense_vector", "dims": 768, "index": True, "similarity": "cosine"}
                }
        }
    }

    es_client.indices.delete(index=INDEX_NAME, ignore_unavailable=True)
    es_client.indices.create(index=INDEX_NAME, body=index_settings)
    print(f"Elasticsearch index '{INDEX_NAME}' created")
    return es_client


def load_documents(es_client, documents, model):
    print("Cargando documentos...")
    operations = []
    for doc in tqdm(documents):
        doc["pregunta_vector"] = model.encode(doc["pregunta"]).tolist()
        doc["respuesta_vector"] = model.encode(doc["respuesta"]).tolist()
        preg_resp = f"{doc["pregunta"]} {doc["respuesta"]}"
        doc["preg_resp_vector"] = model.encode(preg_resp).tolist()
        operations.append(doc)

    for doc in tqdm(operations):
        try:
            es_client.index(index=INDEX_NAME, document=doc)
        except Exception as e:
            print(e)

    print(f"Se cargarón {len(operations)} documentos")


def main():
    print("Iniciando proceso de indexado...")

    documents = ingest_data() # Carga los datos de DATASETS/faq_sacmex.csv
    ground_truth = ingest_data("../DATASETS/ground-truth-retrieval_llama3_2_3b.csv")
    model = load_model() # Carga el modelo del Transformer
    es_client = setup_elasticsearch() # Crea un cliente hacia ElasticSearch
    load_documents(es_client, documents, model)

    print("Inicializando la Base de datos...")
    init_db()

    print("Proceso de indexado de documentos realizado completamente!")


if __name__ == "__main__":
    main()