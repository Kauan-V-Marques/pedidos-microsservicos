"""RabbitMQ: topologia, consumidor e relay do outbox (aio-pika)."""

import asyncio
import json
import logging

import aio_pika
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from estoque.repositorio import ler_mensagem, processar

log = logging.getLogger("estoque.mensageria")

EXCHANGE = "loja.eventos"
DLX = "loja.dlx"
FILA = "estoque.eventos"
CHAVES = ("pedido.criado", "pagamento.aprovado", "pagamento.recusado")


async def declarar(
    canal: aio_pika.abc.AbstractChannel,
) -> tuple[aio_pika.abc.AbstractExchange, aio_pika.abc.AbstractQueue]:
    exchange = await canal.declare_exchange(EXCHANGE, aio_pika.ExchangeType.TOPIC, durable=True)
    dlx = await canal.declare_exchange(DLX, aio_pika.ExchangeType.DIRECT, durable=True)
    dlq = await canal.declare_queue(f"{FILA}.dlq", durable=True, arguments={"x-queue-type": "quorum"})
    await dlq.bind(dlx, routing_key=FILA)
    fila = await canal.declare_queue(
        FILA,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-delivery-limit": 5,
            "x-dead-letter-exchange": DLX,
            "x-dead-letter-routing-key": FILA,
        },
    )
    for chave in CHAVES:
        await fila.bind(exchange, routing_key=chave)
    return exchange, fila


async def consumir(pool: AsyncConnectionPool, fila: aio_pika.abc.AbstractQueue) -> None:
    async with fila.iterator() as mensagens:
        async for mensagem in mensagens:
            try:
                evento = ler_mensagem(mensagem.body)
            except (ValueError, KeyError) as erro:
                log.warning("mensagem inválida enviada para a DLQ: %s", erro)
                await mensagem.reject(requeue=False)
                continue
            try:
                async with pool.connection() as conn:
                    resultado = await processar(conn, evento)
                log.info("[%s] %s: %s", evento["pedido_id"], evento["tipo"], resultado)
                await mensagem.ack()
            except Exception as erro:  # noqa: BLE001 - erro temporário: tenta de novo (limite de 5 entregas)
                log.warning("[%s] falha em %s: %s", evento["pedido_id"], evento["tipo"], erro)
                await mensagem.nack(requeue=True)


async def relay_outbox(pool: AsyncConnectionPool, exchange: aio_pika.abc.AbstractExchange) -> None:
    """Publica os eventos do outbox; só marca como publicado depois da confirmação do broker."""
    while True:
        try:
            async with pool.connection() as conn, conn.transaction():
                cursor = conn.cursor(row_factory=dict_row)
                await cursor.execute(
                    """SELECT id, mensagem FROM outbox WHERE publicado_em IS NULL
                        ORDER BY criado_em LIMIT 50 FOR UPDATE SKIP LOCKED"""
                )
                pendentes = await cursor.fetchall()
                for linha in pendentes:
                    evento = linha["mensagem"]
                    await exchange.publish(  # canal com publisher confirms: aguarda o "ok" do broker
                        aio_pika.Message(
                            json.dumps(evento).encode(),
                            content_type="application/json",
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            message_id=evento["id"],
                            correlation_id=evento["pedido_id"],
                            type=evento["tipo"],
                        ),
                        routing_key=evento["tipo"],
                    )
                if pendentes:
                    await conn.execute(
                        "UPDATE outbox SET publicado_em = now() WHERE id = ANY(%s)",
                        ([p["id"] for p in pendentes],),
                    )
                    log.info("outbox: %d evento(s) publicados", len(pendentes))
            if not pendentes:
                await asyncio.sleep(0.25)
        except Exception as erro:  # noqa: BLE001
            log.warning("outbox: falha ao publicar (%s); nova tentativa em 1 s", erro)
            await asyncio.sleep(1)
