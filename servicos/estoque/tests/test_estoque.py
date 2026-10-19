import asyncio
import os
import sys
import uuid

import psycopg
import pytest
from psycopg_pool import AsyncConnectionPool

from estoque import repositorio
from estoque.repositorio import envelope, ler_mensagem, processar

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

SERVIDOR = os.environ.get("TEST_DATABASE_URL", "postgresql://loja:loja@localhost:5432/estoque")


@pytest.fixture(scope="session")
def database_url():
    nome = f"estoque_teste_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(SERVIDOR, autocommit=True) as conn:
        # UTF-8 explícito: não depende da codificação padrão do servidor
        conn.execute(f"CREATE DATABASE {nome} ENCODING 'UTF8' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0")
    base, _, _ = SERVIDOR.rpartition("/")
    yield f"{base}/{nome}"
    with psycopg.connect(SERVIDOR, autocommit=True) as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {nome} WITH (FORCE)")


@pytest.fixture()
async def pool(database_url):
    pool = AsyncConnectionPool(database_url, min_size=1, max_size=10, open=False)
    await pool.open(wait=True)
    async with pool.connection() as conn:
        await conn.execute("DROP TABLE IF EXISTS reservas, outbox, mensagens_processadas, produtos")
        await repositorio.migrar(conn)
    yield pool
    await pool.close()


def pedido_criado(itens, cartao="4242"):
    return envelope("pedido.criado", str(uuid.uuid4()), {"itens": itens, "cartao_final": cartao})


async def disponivel(pool, sku):
    async with pool.connection() as conn:
        return (
            await (await conn.execute("SELECT disponivel FROM produtos WHERE sku = %s", (sku,))).fetchone()
        )[0]


async def eventos_no_outbox(pool, pedido_id):
    async with pool.connection() as conn:
        cursor = await conn.execute("SELECT tipo, mensagem FROM outbox WHERE pedido_id = %s", (pedido_id,))
        return await cursor.fetchall()


async def tratar(pool, evento):
    async with pool.connection() as conn:
        return await processar(conn, evento)


async def test_reserva_baixa_o_estoque_e_avisa_o_valor_total(pool):
    evento = pedido_criado([{"sku": "SEN-QTR8A", "quantidade": 2}, {"sku": "MOT-N20-1K", "quantidade": 4}])

    assert await tratar(pool, evento) == "estoque.reservado"

    assert await disponivel(pool, "SEN-QTR8A") == 18
    assert await disponivel(pool, "MOT-N20-1K") == 36
    [(tipo, mensagem)] = await eventos_no_outbox(pool, evento["pedido_id"])
    assert tipo == "estoque.reservado"
    assert mensagem["dados"]["valor_total"] == pytest.approx(2 * 189.90 + 4 * 39.90)


async def test_reserva_e_tudo_ou_nada(pool):
    evento = pedido_criado([{"sku": "SEN-QTR8A", "quantidade": 2}, {"sku": "KIT-SUMO", "quantidade": 5}])

    assert await tratar(pool, evento) == "estoque.recusado"

    assert await disponivel(pool, "SEN-QTR8A") == 20  # nem o item que tinha estoque foi reservado
    [(tipo, mensagem)] = await eventos_no_outbox(pool, evento["pedido_id"])
    assert tipo == "estoque.recusado"
    assert "KIT-SUMO (pedido 5, disponível 3)" in mensagem["dados"]["motivo"]


async def test_produto_inexistente_e_recusado(pool):
    evento = pedido_criado([{"sku": "NAO-EXISTE", "quantidade": 1}])

    assert await tratar(pool, evento) == "estoque.recusado"


async def test_pagamento_recusado_devolve_as_unidades_compensacao(pool):
    criado = pedido_criado([{"sku": "MCU-ESP32", "quantidade": 3}])
    await tratar(pool, criado)
    assert await disponivel(pool, "MCU-ESP32") == 22

    resultado = await tratar(
        pool, envelope("pagamento.recusado", criado["pedido_id"], {"motivo": "cartão recusado"})
    )

    assert resultado == "liberadas 1"
    assert await disponivel(pool, "MCU-ESP32") == 25


async def test_pagamento_aprovado_confirma_a_reserva(pool):
    criado = pedido_criado([{"sku": "MCU-ESP32", "quantidade": 1}])
    await tratar(pool, criado)

    assert await tratar(pool, envelope("pagamento.aprovado", criado["pedido_id"], {})) == "confirmadas 1"
    # Um "recusado" atrasado não devolve o que já foi confirmado
    assert await tratar(pool, envelope("pagamento.recusado", criado["pedido_id"], {})) == "liberadas 0"
    assert await disponivel(pool, "MCU-ESP32") == 24


async def test_mensagem_repetida_nao_reserva_duas_vezes(pool):
    evento = pedido_criado([{"sku": "BAT-2S-850", "quantidade": 2}])

    assert await tratar(pool, evento) == "estoque.reservado"
    assert await tratar(pool, evento) == "repetido"

    assert await disponivel(pool, "BAT-2S-850") == 13
    assert len(await eventos_no_outbox(pool, evento["pedido_id"])) == 1


async def test_pedidos_simultaneos_nunca_vendem_mais_do_que_existe(pool):
    # 3 kits em estoque, 5 pedidos de 2 kits ao mesmo tempo: só um pode ser atendido
    eventos = [pedido_criado([{"sku": "KIT-SUMO", "quantidade": 2}]) for _ in range(5)]

    resultados = await asyncio.gather(*(tratar(pool, e) for e in eventos))

    assert sorted(resultados) == ["estoque.recusado"] * 4 + ["estoque.reservado"]
    assert await disponivel(pool, "KIT-SUMO") == 1


def test_mensagem_malformada_e_rejeitada():
    with pytest.raises(ValueError):
        ler_mensagem(b"nao e json")
    with pytest.raises(ValueError):
        ler_mensagem(b'{"id": "123", "tipo": "pedido.criado", "pedido_id": "x", "dados": {}}')
    with pytest.raises(ValueError, match="sem itens"):
        evento = envelope("pedido.criado", str(uuid.uuid4()), {"itens": []})
        ler_mensagem(__import__("json").dumps(evento).encode())
