"""Testes de ponta a ponta. Exigem RabbitMQ real em localhost:5672."""

import os
from pathlib import Path
import queue
import subprocess
import sys
import threading
import time
import unittest
import uuid

import pika

ROOT = Path(__file__).resolve().parents[1]


class RabbitMQIntegracao(unittest.TestCase):
    def setUp(self):
        self.fila = "teste_atividade_" + uuid.uuid4().hex
        self.env = dict(os.environ, RABBITMQ_QUEUE=self.fila,
                        PYTHONIOENCODING="utf-8", PYTHONUNBUFFERED="1")
        self.connection = pika.BlockingConnection(pika.URLParameters(
            self.env.get("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/%2F")))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.fila, durable=True)
        self.workers = []

    def tearDown(self):
        for process, output, lines in self.workers:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=10)
        if self.connection.is_open:
            # Apenas a fila exclusiva criada por este teste é removida.
            self.channel.queue_delete(queue=self.fila)
            self.connection.close()

    def publicar(self, *partes):
        result = subprocess.run(
            [sys.executable, str(ROOT / "produtor.py"), *partes],
            cwd=ROOT, env=self.env, capture_output=True, text=True,
            encoding="utf-8", timeout=20,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Enviado para a fila:", result.stdout)

    def iniciar_worker(self):
        process = subprocess.Popen(
            [sys.executable, "-u", str(ROOT / "worker.py")],
            cwd=ROOT, env=self.env, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, encoding="utf-8",
        )
        output = queue.Queue()
        lines = []

        def ler():
            for line in process.stdout:
                lines.append(line.rstrip())
                output.put(line.rstrip())

        threading.Thread(target=ler, daemon=True).start()
        worker = (process, output, lines)
        self.workers.append(worker)
        self.esperar_saida(worker, "Aguardando pedidos")
        return worker

    def esperar_saida(self, worker, trecho, timeout=20):
        process, output, lines = worker
        limite = time.monotonic() + timeout
        while time.monotonic() < limite:
            try:
                line = output.get(timeout=0.1)
                if trecho in line:
                    return line
            except queue.Empty:
                if process.poll() is not None:
                    self.fail("Worker encerrou: " + "\n".join(lines))
        self.fail(f"Não apareceu {trecho!r}: " + "\n".join(lines))

    def test_01_produtor_armazena_pedido_sem_consumidor(self):
        self.publicar("Pedido", "com", "acentuação")
        method, properties, body = self.channel.basic_get(self.fila, auto_ack=False)
        self.assertIsNotNone(method)
        self.assertEqual(body.decode(), "Pedido com acentuação")
        self.assertEqual(properties.delivery_mode, 2)
        self.channel.basic_ack(method.delivery_tag)
        self.publicar()
        method, _, body = self.channel.basic_get(self.fila, auto_ack=True)
        self.assertIsNotNone(method)
        self.assertEqual(body.decode(), "Pedido Padrão")
        print("EVIDENCIA: pedidos ficam na fila sem worker; publicação persistente e UTF-8 OK.")

    def test_02_worker_processa_e_confirma(self):
        worker = self.iniciar_worker()
        self.publicar("Pedido confirmado.")
        self.esperar_saida(worker, "Finalizado processamento: Pedido confirmado.")
        worker[0].kill()
        worker[0].wait(timeout=10)
        # Após fechar a conexão, um pedido sem ACK voltaria à fila.
        time.sleep(1)
        result = self.channel.queue_declare(queue=self.fila, passive=True)
        self.assertEqual(result.method.message_count, 0)
        print("EVIDENCIA: worker executou o pedido e enviou ACK; fila vazia após sua saída.")

    def test_03_dois_workers_dividem_pedidos(self):
        a, b = self.iniciar_worker(), self.iniciar_worker()
        pedidos = [f"Pedido {n}.." for n in range(1, 7)]
        for pedido in pedidos:
            self.publicar(pedido)
        limite = time.monotonic() + 30
        prefixo = "[x] Finalizado processamento: "
        while time.monotonic() < limite:
            finalizados = [line[len(prefixo):] for w in (a, b)
                           for line in list(w[2]) if line.startswith(prefixo)]
            if len(finalizados) == len(pedidos):
                break
            time.sleep(0.1)
        self.assertCountEqual(finalizados, pedidos)
        for nome, worker in (("A", a), ("B", b)):
            recebidos = [line for line in worker[2] if line.startswith(prefixo)]
            self.assertTrue(recebidos, f"Worker {nome} não recebeu pedidos")
            print(f"EVIDENCIA: worker {nome}: {recebidos}")

    def test_04_queda_antes_do_ack_devolve_pedido(self):
        worker = self.iniciar_worker()
        pedido = "Pedido recuperável........"
        self.publicar(pedido)
        self.esperar_saida(worker, "Iniciando processamento: " + pedido)
        # Prefetch 1: o pedido seguinte deve continuar disponível na fila.
        self.publicar("Pedido aguardando")
        method, _, body = self.channel.basic_get(self.fila, auto_ack=True)
        self.assertIsNotNone(method)
        self.assertEqual(body.decode(), "Pedido aguardando")
        worker[0].kill()
        worker[0].wait(timeout=10)
        limite = time.monotonic() + 10
        method = None
        while time.monotonic() < limite:
            method, _, body = self.channel.basic_get(self.fila, auto_ack=False)
            if method is not None:
                break
            time.sleep(0.1)
        self.assertIsNotNone(method, "Pedido não retornou após a queda")
        self.assertTrue(method.redelivered)
        self.assertEqual(body.decode(), pedido)
        self.channel.basic_nack(method.delivery_tag, requeue=True)
        recuperacao = self.iniciar_worker()
        self.esperar_saida(recuperacao, "Finalizado processamento: " + pedido)
        print("EVIDENCIA: prefetch 1 respeitado; pedido sem ACK foi reentregue e concluído por outro worker.")


if __name__ == "__main__":
    unittest.main(verbosity=2)
