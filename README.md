# Mensageria com RabbitMQ

[![Validação com RabbitMQ](https://github.com/tomitaaa/-RabbitMQ/actions/workflows/validacao.yml/badge.svg)](https://github.com/tomitaaa/-RabbitMQ/actions/workflows/validacao.yml)

Projeto da atividade assíncrona baseado no **Minicurso — Mensageria com RabbitMQ**, de Igor, Andrey e Laryssa. A parte prática utiliza Python e Pika para enviar pedidos a uma fila e processá-los em workers.

## O que o projeto demonstra

```text
produtor.py → exchange padrão ("") → fila_pedidos → worker.py
                                                 → worker.py
                                                 → worker.py
```

- Comunicação assíncrona: o produtor pode enviar pedidos antes de existir um consumidor.
- Fila durável (`durable=True`) e mensagens persistentes (`delivery_mode=2`).
- Vários consumidores dividindo os pedidos de uma mesma fila.
- Confirmação manual (`basic_ack`) somente após o processamento.
- `prefetch_count=1`: cada worker recebe no máximo um pedido sem confirmação por vez.
- Cada ponto `.` na mensagem representa um segundo de processamento, como no vídeo.

## 1. Preparar o ambiente

Requisitos: Python 3.10 ou superior e Docker com suporte a contêineres Linux.

```sh
git clone https://github.com/tomitaaa/-RabbitMQ.git
cd ./-RabbitMQ
python -m venv .venv
```

No Windows, é possível usar o Python do ambiente diretamente, sem alterar a política do PowerShell:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

No Linux/macOS:

```sh
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Nos comandos seguintes, use `python` com o ambiente ativado ou substitua por `.\.venv\Scripts\python.exe` no Windows.

## 2. Iniciar o RabbitMQ

Comando fornecido na atividade e utilizado no vídeo:

```sh
docker run -d --name rabbitmq-server -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

Alternativamente, este repositório inclui um Compose que limita as portas ao próprio computador e mantém os dados em um volume:

```sh
docker compose up -d --wait
```

Escolha uma das opções; ambas usam o nome `rabbitmq-server` e as mesmas portas. Se o contêiner já foi criado, use `docker start rabbitmq-server`.

Interface de gerenciamento: [http://localhost:15672](http://localhost:15672). Usuário `guest`, senha `guest`. A porta `5672` é usada pelos programas; a `15672`, pela interface web.

## 3. Reproduzir a demonstração

Abra um terminal para o consumidor:

```sh
python worker.py
```

Em outro terminal, envie um pedido:

```sh
python produtor.py "Pedido 1..."
```

O produtor informa o envio. O worker informa o início e, depois de três segundos, a conclusão. Sem argumentos, o produtor envia `Pedido Padrão`.

Em **Queues and Streams → fila_pedidos**, observe:

| Indicador | Significado |
| --- | --- |
| Ready | Pedidos aguardando entrega a um consumidor |
| Unacked | Pedidos entregues, mas ainda sem confirmação |
| Total | Soma de Ready e Unacked |
| Consumers | Número de consumidores inscritos na fila |

As métricas da interface podem levar alguns segundos para atualizar.

## 4. Situações práticas

### A. Pedido enviado sem consumidor

Encerre os workers com `Ctrl+C` e execute `python produtor.py "Pedido pendente..."`. O pedido permanece em **Ready**. Inicie `python worker.py`: o pedido será entregue e confirmado após o processamento.

### B. Divisão entre três workers, como no final do vídeo

Execute `python worker.py` em três terminais. Em um quarto, envie seis pedidos em sequência:

```sh
python produtor.py "Pedido 1..."
python produtor.py "Pedido 2..."
python produtor.py "Pedido 3..."
python produtor.py "Pedido 4..."
python produtor.py "Pedido 5..."
python produtor.py "Pedido 6..."
```

Os consumidores dividem os pedidos; cada mensagem é entregue a um consumidor por vez. No vídeo, cada worker recebe dois dos seis pedidos. Os testes reproduzem os três workers e seis pedidos; a distribuição exata pode variar conforme o escalonamento dos processos. Com prefetch 1 e tempos diferentes, o worker que termina antes pode receber mais pedidos. Não há garantia de alternância estrita nem de ordem global de conclusão.

### C. Queda durante o processamento

Com apenas um worker ativo, envie `python produtor.py "Pedido demorado.........."`. Interrompa esse worker antes de dez segundos. Como ainda não enviou o ACK, o RabbitMQ recoloca o pedido na fila quando detecta o fechamento da conexão. Inicie outro worker para concluir o pedido.

Isso permite reentrega: uma aplicação real deve tratar pedidos de forma idempotente, pois efeitos executados antes de uma falha podem se repetir.

### D. Reinicialização do broker

Sem workers ativos, publique um pedido e execute `docker restart rabbitmq-server`. Aguarde a inicialização e inicie novamente o worker. A fila durável e a mensagem persistente permitem recuperar o pedido após o reinício. Não remova os dados/volume do contêiner. Os clientes deste exemplo precisam ser reiniciados quando sua conexão com o broker é interrompida.

## Arquivos

| Arquivo | Função |
| --- | --- |
| `produtor.py` | Publica pedidos persistentes pela exchange padrão |
| `worker.py` | Processa pedidos e confirma manualmente |
| `conexao.py` | Centraliza a conexão e o nome da fila |
| `compose.yaml` | Alternativa ao comando Docker da aula |
| `tests/test_integracao.py` | Executa os programas contra RabbitMQ real |
| `.github/workflows/validacao.yml` | Executa os testes com RabbitMQ em contêiner |

O fluxo, o nome `fila_pedidos`, o pedido padrão e a simulação por pontos seguem o vídeo. Foram acrescentados organização em funções, fechamento das conexões, variáveis de ambiente, confirmação de publicação e testes. A espera do worker usa `connection.sleep` para manter os heartbeats ativos durante pedidos longos.

## Validação

Com RabbitMQ em execução:

```sh
python -m unittest discover -s tests -v
```

Os testes criam filas exclusivas e executam `produtor.py` e `worker.py` em processos separados. Verificam envio sem consumidor, conteúdo UTF-8, persistência marcada na mensagem, ACK, divisão entre workers, prefetch e reentrega após queda. As filas temporárias são removidas ao final. O teste não reinicia o broker; a situação D é um roteiro manual adicional.

O GitHub Actions usa a mesma imagem `rabbitmq:3-management` solicitada na atividade. Consulte os [resultados e logs de execução](https://github.com/tomitaaa/-RabbitMQ/actions).

Para usar outro ambiente, configure `RABBITMQ_URL`. Para alterar a fila, configure `RABBITMQ_QUEUE`. Os padrões são `amqp://guest:guest@localhost:5672/%2F` e `fila_pedidos`.

## Conceitos da aula

| Conceito | Descrição |
| --- | --- |
| Mensageria | Favorece desacoplamento e comunicação assíncrona entre componentes |
| Produtor | Envia a mensagem para o broker |
| Exchange | Recebe publicações e as roteia para filas |
| Binding | Regra que conecta uma exchange a uma fila |
| Fila | Armazena as mensagens até a entrega ao consumidor |
| Consumidor | Recebe e processa mensagens |

Na exchange padrão (`""`), o RabbitMQ cria automaticamente um binding com a routing key igual ao nome da fila. Por isso o exemplo não precisa declarar uma exchange ou binding manualmente. A aula também apresenta direct, topic, fanout e headers conceitualmente; a implementação demonstrada utiliza a exchange padrão.

## Referências

- [Vídeo disponibilizado na atividade](https://drive.google.com/file/d/1jDwwekobsYigG9gBBf40rWjDheXMz_X-/view)
- [RabbitMQ: Work Queues em Python](https://www.rabbitmq.com/tutorials/tutorial-two-python)
- [Pika: BlockingConnection](https://pika.readthedocs.io/en/stable/modules/adapters/blocking.html)
