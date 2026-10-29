# Evidências de execução

Em **29/10/2026**, os programas foram executados contra um RabbitMQ real em contêiner no GitHub Actions.

- [Execução de referência: 40972259053](https://github.com/tomitaaa/-RabbitMQ/actions/runs/40972259053)
- Commit avaliado: `d0cd80dfda5e8279ccf3111a4d1e68d03d6ed207`.
- Ambiente: Ubuntu, Python 3.12, Pika 1.4.1 e imagem `rabbitmq:3-management`.
- Resultado: **4 testes aprovados**, em **16,841 segundos**.

| Situação executada | Resultado observado |
| --- | --- |
| Envio sem consumidor | Mensagem preservada na fila, conteúdo UTF-8 correto e `delivery_mode=2`; pedido padrão também conferido |
| Consumo com ACK | Pedido concluído; após encerrar o worker, a fila permaneceu vazia |
| Dois workers | Worker A concluiu os pedidos 1, 3 e 5; worker B concluiu 2, 4 e 6 |
| Queda antes do ACK | Pedido voltou com `redelivered=True` e foi concluído por um novo worker; o pedido seguinte permaneceu disponível enquanto o primeiro estava em processamento |

A distribuição exata entre workers pode variar em outras execuções; o teste verifica que ambos trabalham e que cada pedido é concluído uma vez no cenário sem falhas.

## O que foi executado e o que é roteiro

O computador usado para preparar esta atividade não tinha Docker disponível. Por isso, a execução do broker e dos cenários acima ocorreu no GitHub Actions, que inicializou a mesma imagem Docker indicada no enunciado. Os programas Python do repositório foram executados de fato; os testes não simulam o broker.

A reinicialização do broker está documentada no README como situação manual adicional, mas **não faz parte dos quatro testes executados**. A propriedade de persistência da publicação foi conferida; isso, isoladamente, não equivale a testar recuperação após reinício do servidor.

Consulte a [lista de execuções](https://github.com/tomitaaa/-RabbitMQ/actions) para a validação dos commits posteriores. A execução de referência acima também apontou avisos de fechamento dos arquivos de saída dos processos de teste; esse fechamento foi corrigido posteriormente.
