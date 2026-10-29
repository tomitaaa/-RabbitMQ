"""Consome pedidos: cada ponto na mensagem representa um segundo de trabalho."""

from conexao import FILA, conectar


def processar_pedido(ch, method, properties, body):
    mensagem = body.decode("utf-8")
    print(f"[x] Iniciando processamento: {mensagem}", flush=True)
    # Mantém os heartbeats ativos durante a espera, inclusive em pedidos longos.
    ch.connection.sleep(body.count(b"."))
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f"[x] Finalizado processamento: {mensagem}", flush=True)


def main():
    with conectar() as conexao:
        channel = conexao.channel()
        channel.queue_declare(queue=FILA, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=FILA,
            on_message_callback=processar_pedido,
            auto_ack=False,
        )
        print("[*] Aguardando pedidos. Para sair pressione CTRL+C", flush=True)
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            # A mensagem ainda não confirmada pode ser entregue a outro worker.
            print("\n[*] Encerrando consumidor.", flush=True)


if __name__ == "__main__":
    main()
