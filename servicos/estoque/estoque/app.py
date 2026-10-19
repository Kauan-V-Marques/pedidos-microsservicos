"""Serviço de estoque: API de consulta + consumidor de eventos + relay do outbox."""

import asyncio
import logging
import os
from contextlib import asynccontextmanager

import aio_pika
from fastapi import FastAPI
from psycopg_pool import AsyncConnectionPool

from estoque import mensageria, repositorio

logging.basicConfig(level=logging.INFO, format="%(asctime)s estoque: %(message)s")
log = logging.getLogger("estoque")


async def conectar_rabbitmq(url: str) -> aio_pika.abc.AbstractRobustConnection:
    for tentativa in range(1, 31):
        try:
            # Conexão "robusta": reconecta e redeclara a topologia sozinha se o broker cair
            return await aio_pika.connect_robust(url)
        except (OSError, aio_pika.exceptions.AMQPConnectionError):
            log.info("RabbitMQ indisponível (%d/30), tentando de novo...", tentativa)
            await asyncio.sleep(2)
    raise RuntimeError("RabbitMQ não respondeu")


def criar_app(
    database_url: str | None = None, rabbitmq_url: str | None = None, mensageria_ativa: bool = True
):
    database_url = database_url or os.environ.get(
        "DATABASE_URL", "postgresql://loja:loja@localhost:5432/estoque"
    )
    rabbitmq_url = rabbitmq_url or os.environ.get("RABBITMQ_URL", "amqp://loja:loja@localhost:5672/")
    pool = AsyncConnectionPool(database_url, min_size=1, max_size=10, open=False)

    @asynccontextmanager
    async def ciclo(app: FastAPI):
        await pool.open(wait=True)
        async with pool.connection() as conn:
            await repositorio.migrar(conn)
        tarefas, conexao = [], None
        if mensageria_ativa:
            conexao = await conectar_rabbitmq(rabbitmq_url)
            consumidor = await conexao.channel()
            await consumidor.set_qos(prefetch_count=1)
            _, fila = await mensageria.declarar(consumidor)
            publicador = await conexao.channel(publisher_confirms=True)
            exchange = await publicador.get_exchange(mensageria.EXCHANGE)
            tarefas = [
                asyncio.create_task(mensageria.consumir(pool, fila)),
                asyncio.create_task(mensageria.relay_outbox(pool, exchange)),
            ]
        yield
        for tarefa in tarefas:
            tarefa.cancel()
        if conexao:
            await conexao.close()
        await pool.close()

    app = FastAPI(title="Serviço de estoque", lifespan=ciclo)
    app.state.pool = pool

    @app.get("/saude")
    async def saude():
        async with pool.connection() as conn:
            await conn.execute("SELECT 1")
        return {"status": "ok"}

    @app.get("/produtos")
    async def produtos():
        async with pool.connection() as conn:
            return await repositorio.produtos(conn)

    return app
