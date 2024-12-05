import os
import time
import json
from openai import OpenAI
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer


ELASTIC_URL = os.getenv("ELASTIC_URL", "http://localhost:9200")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/v1/")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "ollama")
MODEL_NAME = os.getenv("MODEL_NAME", "multi-qa-MiniLM-L6-cos-v1")
INDEX_NAME = os.getenv("INDEX_NAME", "Gotin_Gotera_Documents")

es_client = Elasticsearch(ELASTIC_URL)
ollama_client = OpenAI(base_url=OLLAMA_URL, api_key="ollama")
openai_client = OpenAI(api_key=OPENAI_API_KEY)
model = SentenceTransformer(MODEL_NAME)

def elastic_search_hybrid(query):
    vector = model.encode(query)
    knn_query = {
        "field": "preg_resp_vector",
        "query_vector": vector,
        "k": 5,
        "num_candidates": 10000,
        "boost": 0.5
    }

    keyword_query = {
        "bool": {
            "must": {
                "multi_match": {
                    "query": query,
                    "fields": ["pregunta", "respuesta"],
                    "type": "best_fields",
                    "boost": 0.5,
                }
            }
        }
    }

    search_query = {
        "knn": knn_query,
        "query": keyword_query,
        "size": 10,
        "_source": ["id", "pregunta", "respuesta"]
    }

    es_results = es_client.search(
        index=INDEX_NAME,
        body=search_query
    )

    result_docs = []

    for hit in es_results['hits']['hits']:
        result_docs.append(hit['_source'])

    return result_docs

def build_prompt_check_request_type(query, search_results):
    entry_template = """
    - {tipo}: {solicitud}\n
    """.strip()

    prompt_template = """
    Eres un agente de SACMEX.

    INSTRUCCIONES ESTRICTAS:
    - Analiza cuidadosamente la SOLICITUD
    - Genera ÚNICAMENTE un JSON con dos campos: "tipo_solicitud" y "solicitud"
    - NO incluyas información adicional ni estructuras anidadas
    - El "tipo_solicitud" debe ser UNO de estos: "pregunta", "nuevo_reporte", "status_reporte"
    - La "solicitud" es el texto original del usuario
    - Sin texto adicional, solo el JSON en una línea exacta

    FORMATO REQUERIDO:
    {{"tipo_solicitud": "categoria", "solicitud": "texto_completo"}}

    EJEMPLOS:
    {contexto}

    REGLA PRINCIPAL:
    - Elige ÚNICAMENTE UN tipo de solicitud que coincida EXACTAMENTE
    - Si la solicitud es sobre CÓMO, DÓNDE o INFORMACIÓN para realizar un reporte o cuales son los telefonos o redes sociales, usa "pregunta"
    - Si la solicitud contiene palabras clave como "levantar", "reportar", "fuga", usa "nuevo_reporte"
    - Si la solicitud menciona "estatus" o "reporte" específico, usa "status_reporte"
    - Si no hay coincidencia exacta, usa "otro" por defecto

    SOLICITUD: {solicitud}

    JSON DEFINITIVO:""".strip()

    context = ""
    for doc in search_results:
        context = context + entry_template.format(**doc) + "\n\n"

    return prompt_template.format(solicitud=query, contexto=context).strip()

def build_prompt_extract_location(query, search_results):
    entry_template = """
    - {solicitud}\n
    """.strip()

    prompt_template = """
    Eres un agente de SACMEX especializado en extracción de información de direcciones de nuevos reportes sobre el agua.

    INSTRUCCIONES PRECISAS:
    - Extrae ÚNICAMENTE la información de ubicación de la fuga
    - Genera un JSON con los siguientes campos:
        * "calle_principal": nombre de la calle principal
        * "entre_calles": calles cercanas (si se mencionan)
        * "colonia": nombre de la colonia
        * "delegacion": nombre de la delegación o alcaldía
        * "referencias_adicionales": cualquier detalle extra de ubicación
    - Sin texto adicional, solo el JSON en una línea exacta.

    FORMATO REQUERIDO:
    {{
        "calle_principal": "Nombre de la calle",
        "entre_calles": ["Calle 1", "Calle 2"],
        "colonia": "Nombre de la colonia",
        "delegacion": "Nombre de la delegación",
        "referencias_adicionales": "Detalles extra"
    }}

    EJEMPLOS:
    {contexto}

    SOLICITUD: {solicitud}

    JSON DEFINITIVO:""".strip()

    context = ""
    for doc in search_results:
        context = context + entry_template.format(**doc) + "\n\n"

    return prompt_template.format(solicitud=query, contexto=context).strip()


def llm(prompt, model="llama3.2:3b"):
    start_time = time.time()
    response = ollama_client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7
    )
    answer = response.choices[0].message.content
    tokens = {
        'prompt_tokens': response.usage.prompt_tokens,
        'completion_tokens': response.usage.completion_tokens,
        'total_tokens': response.usage.total_tokens
    }

    end_time = time.time()
    response_time = end_time - start_time

    return answer, tokens, response_time

def get_report_type(query, model_choice, search_type):
    if search_type == 'Vector':
        search_results = elastic_search_hybrid(query)
    else:
        search_results = elastic_search_hybrid(query)

    prompt = build_prompt_check_request_type(query, search_results)
    answer, tokens, response_time = llm(prompt, model_choice)

    return {
        'answer': answer,
        'response_time': response_time,
        'model_used': model_choice,
        'prompt_tokens': tokens['prompt_tokens'],
        'completion_tokens': tokens['completion_tokens'],
        'total_tokens': tokens['total_tokens']
    }

def get_address(query, model_choice, search_type):
    if search_type == 'Vector':
        search_results = elastic_search_hybrid(query)
    else:
        search_results = elastic_search_hybrid(query)

    prompt = build_prompt(query, search_results)
    answer, tokens, response_time = llm(prompt, model_choice)

    return {
        'answer': answer,
        'response_time': response_time,
        'model_used': model_choice,
        'prompt_tokens': tokens['prompt_tokens'],
        'completion_tokens': tokens['completion_tokens'],
        'total_tokens': tokens['total_tokens']
    }

def get_answer(query, model_choice, search_type):
    if search_type == 'Vector':
        search_results = elastic_search_hybrid(query)
    else:
        search_results = elastic_search_hybrid(query)

    prompt = build_prompt(query, search_results)
    answer, tokens, response_time = llm(prompt, model_choice)

    return {
        'answer': answer,
        'response_time': response_time,
        'model_used': model_choice,
        'prompt_tokens': tokens['prompt_tokens'],
        'completion_tokens': tokens['completion_tokens'],
        'total_tokens': tokens['total_tokens']
    }