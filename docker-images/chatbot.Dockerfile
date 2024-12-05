FROM python:3.13.0-bookworm

RUN pip install -U pip
RUN pip install pipenv

WORKDIR /app

# Copiamos hacia el directorio /app
COPY ["../Gotin_Gotera/Pipfile", "../Gotin_Gotera/Pipfile.lock", "./"]

# Instalamos las librerias
RUN pipenv install --system --deploy


COPY ["../Gotin_Gotera/app.py", "../Gotin_Gotera/assistant.py", "../Gotin_Gotera/db.py", "../Gotin_Gotera/.env", "../Gotin_Gotera/ingest.py", "../Gotin_Gotera/minsearch.py", "../Gotin_Gotera/prep_data.py", "./"]

EXPOSE 9696

# Ejecutamos el servicio de Flask con Gunicorn
ENTRYPOINT ["gunicorn", "--bind=0.0.0.0:9696", "app:app"]