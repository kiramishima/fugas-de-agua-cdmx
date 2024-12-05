from flask import Flask, request, jsonify, Response
import time
import uuid
import os
import requests
from assistant import get_report_type, get_address, get_answer
from db import save_conversation, save_feedback, get_recent_conversations, get_feedback_stats


TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")

app = Flask('charo-endpoint')

def print_log(message):
    print(message, flush=True)

@app.route('/conversation', methods=['POST'])
def conversation():
    user_input = request.get_json() # Recibimos los datos
    print_log(user_input)
    conversation_id = str(uuid.uuid4()) # Generamos un id unico de la platica
    model_choice = "llama3.2:3b" # En caso de usar otro modelo LLM aqui podemos indicarlo

    print_log(f"Nueva conversación iniciada con el ID: {conversation_id}")
    topic = user_input['SOLICITUD'] # Indica si la 
    query = user_input["MENSAJE"]
    search_type = "Vector"

    print_log(f"Obteniendo respuesta del asistente utilizando {model_choice} como modelo y tipo de busqueda {search_type}")
    try:
        if TELEGRAM_TOKEN != "":
            chat_id = user_input['message']['chat']['id'] # Id del Chatbot de Telegram
            query = user_input['message']['text'] # Texto enviado desde Telegram

        # Identificamos el tipo de Petición
        start_time = time.time() # Medir el tiempo de inicio
        topic_data = get_report_type(query, model_choice, search_type) # Enviamos los datos al modelo
        print_log(topic_data)
        topic = topic_data['tipo_solicitud'] == 'pregunta'
        end_time = time.time() # Tiempo en que tardo en procesar el agente
        print_log(f"Respuesta recibida en {end_time - start_time:.2f} segundos")

        # Invocamos por tipo de peticion
        output = dict()
        if topic_data['tipo_solicitud'] == 'pregunta':
            start_time = time.time() # Medir el tiempo de inicio
            answer_data = get_answer(query, model_choice, search_type) # Enviamos los datos al modelo
            print_log(topic_data)
            end_time = time.time() # Tiempo en que tardo en procesar el agente
            print_log(f"Respuesta recibida en {end_time - start_time:.2f} segundos")
        elif topic_data['tipo_solicitud'] == 'nuevo_reporte':
            start_time = time.time() # Medir el tiempo de inicio
            answer_data = get_answer(query, model_choice, search_type) # Enviamos los datos al modelo
            print_log(topic_data)
            end_time = time.time() # Tiempo en que tardo en procesar el agente
            print_log(f"Respuesta recibida en {end_time - start_time:.2f} segundos")
        elif topic_data['tipo_solicitud'] == 'nuevo_reporte':
            start_time = time.time() # Medir el tiempo de inicio
            answer_data = get_answer(query, model_choice, search_type) # Enviamos los datos al modelo
            print_log(topic_data)
            end_time = time.time() # Tiempo en que tardo en procesar el agente
            print_log(f"Respuesta recibida en {end_time - start_time:.2f} segundos")
        else:
            print("otro tipo")
            output = {
                'conversation_id': conversation_id,
                'answer': "Lo comunicaremos con usted lo más pronto posible para atender a su respuesta, gracias."
            }

        # Enviamos los datos de respuesta
        if TELEGRAM_TOKEN != '':
            # Invocamos el servicio de Telegram
            url = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'
            payload = {
                'chat_id': chat_id,
                'text': answer_data['answer']
            }
            r = requests.post(url, json=payload)

            if r.status_code == 200:
                return Response('ok', status=200)
            else:
                return Response('Failed to send message to Telegram', status=500)
        else:
            output = {
                'conversation_id': conversation_id,
                'answer': answer_data['answer']
            }

        # Almacenamos la conversación en la Base de datos
        print_log("Guardando la conversación en la base de datos")
        save_conversation(conversation_id, query, answer_data, topic)
        print_log("Conversación guardada de manera exitosa")

        return jsonify(output)  ## send back the data in json format to the user
    except:
        print("No hay mensaje")

    return Response("ok",status=200)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=9696)