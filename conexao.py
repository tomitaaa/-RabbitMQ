"""Configuração compartilhada; os valores padrão reproduzem a aula."""

import os

import pika

FILA = os.environ.get("RABBITMQ_QUEUE", "fila_pedidos")


def conectar():
    parametros = pika.URLParameters(
        os.environ.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")
    )
    parametros.blocked_connection_timeout = 30
    return pika.BlockingConnection(parametros)
