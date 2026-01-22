FROM python:3.13-slim

WORKDIR /app

RUN apt update && apt install -y curl libpq-dev gcc g++ build-essential

RUN curl -sSL https://install.python-poetry.org | \
    POETRY_HOME=/opt/poetry POETRY_VERSION=1.8.5 python && \
    cd /usr/local/bin && \
    ln -s /opt/poetry/bin/poetry && \
    poetry config virtualenvs.create false

COPY ./pyproject.toml ./poetry.lock* /app/

RUN poetry install --no-root --only=main

COPY ./src /app/src