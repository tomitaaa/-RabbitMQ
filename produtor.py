"""Publica um pedido persistente na fila apresentada no minicurso."""

import sys

import pika

from conexao import FILA, conectar


def main():
    mensagem = " ".join(sys.argv[1:]) or "Pedido Padrão"
    with conectar() as conexao:
        channel = conexao.channel()
        channel.queue_declare(queue=FILA, durable=True)
        # Confirma que o broker aceitou a publicação antes de encerrar.
        channel.confirm_delivery()
        channel.basic_publish(
            exchange="",
            routing_key=FILA,
            body=mensagem.encode("utf-8"),
            properties=pika.BasicProperties(delivery_mode=2),
            mandatory=True,
        )
        print(f"[x] Enviado para a fila: {mensagem}", flush=True)


if __name__ == "__main__":
    main()
