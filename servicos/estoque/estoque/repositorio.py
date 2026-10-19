"""Regras do estoque. Cada evento é tratado numa transação que também grava a mensagem
como processada (inbox) e o evento de resposta (outbox): ou acontece tudo, ou nada."""

import json
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from importlib import resources

from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


def envelope(tipo: str, pedido_id: str, dados: dict) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "tipo": tipo,
        "ocorrido_em": datetime.now(UTC).isoformat(),
        "pedido_id": pedido_id,
        "dados": dados,
    }


async def migrar(conn: AsyncConnection) -> None:
    sql = resources.files("estoque").joinpath("banco.sql").read_text(encoding="utf-8")
    await conn.execute(sql)
    await conn.commit()


async def _publicar_depois(conn: AsyncConnection, evento: dict) -> None:
    await conn.execute(
        "INSERT INTO outbox (id, tipo, pedido_id, mensagem) VALUES (%s, %s, %s, %s)",
        (evento["id"], evento["tipo"], evento["pedido_id"], Jsonb(evento)),
    )


async def reservar(conn: AsyncConnection, pedido_id: str, itens: list[dict], cartao_final: str) -> dict:
    """Reserva tudo ou nada. As linhas são travadas em ordem de SKU para evitar deadlock
    entre pedidos simultâneos que disputam os mesmos produtos."""
    pedidos = {item["sku"]: int(item["quantidade"]) for item in itens}
    cursor = conn.cursor(row_factory=dict_row)
    await cursor.execute(
        "SELECT sku, nome, preco, disponivel FROM produtos WHERE sku = ANY(%s) ORDER BY sku FOR UPDATE",
        (sorted(pedidos),),
    )
    produtos = {p["sku"]: p for p in await cursor.fetchall()}

    faltantes = [
        f"{sku} (pedido {qtd}, disponível {produtos[sku]['disponivel'] if sku in produtos else 0})"
        for sku, qtd in sorted(pedidos.items())
        if sku not in produtos or produtos[sku]["disponivel"] < qtd
    ]
    if faltantes:
        evento = envelope("estoque.recusado", pedido_id, {"motivo": ", ".join(faltantes)})
        await _publicar_depois(conn, evento)
        return evento

    total = Decimal("0")
    for sku, qtd in pedidos.items():
        await conn.execute("UPDATE produtos SET disponivel = disponivel - %s WHERE sku = %s", (qtd, sku))
        await conn.execute(
            "INSERT INTO reservas (pedido_id, sku, quantidade, status) VALUES (%s, %s, %s, 'reservada')",
            (pedido_id, sku, qtd),
        )
        total += produtos[sku]["preco"] * qtd

    evento = envelope(
        "estoque.reservado",
        pedido_id,
        {"itens": itens, "valor_total": float(total), "cartao_final": cartao_final},
    )
    await _publicar_depois(conn, evento)
    return evento


async def confirmar(conn: AsyncConnection, pedido_id: str) -> int:
    cursor = await conn.execute(
        """UPDATE reservas SET status = 'confirmada', atualizada_em = now()
            WHERE pedido_id = %s AND status = 'reservada'""",
        (pedido_id,),
    )
    return cursor.rowcount


async def liberar(conn: AsyncConnection, pedido_id: str) -> int:
    """Compensação da saga: o pagamento falhou, então as unidades voltam para o estoque."""
    cursor = await conn.execute(
        """
        WITH liberadas AS (
            UPDATE reservas SET status = 'liberada', atualizada_em = now()
             WHERE pedido_id = %s AND status = 'reservada'
            RETURNING sku, quantidade
        )
        UPDATE produtos p SET disponivel = p.disponivel + l.quantidade
          FROM liberadas l WHERE p.sku = l.sku
        """,
        (pedido_id,),
    )
    return cursor.rowcount


async def processar(conn: AsyncConnection, evento: dict) -> str:
    """Trata um evento recebido. Idempotente: devolve 'repetido' se a mensagem já foi processada."""
    async with conn.transaction():
        nova = await conn.execute(
            "INSERT INTO mensagens_processadas (id) VALUES (%s) ON CONFLICT DO NOTHING", (evento["id"],)
        )
        if nova.rowcount == 0:
            return "repetido"

        pedido_id, dados = evento["pedido_id"], evento["dados"]
        if evento["tipo"] == "pedido.criado":
            resposta = await reservar(conn, pedido_id, dados["itens"], dados["cartao_final"])
            return resposta["tipo"]
        if evento["tipo"] == "pagamento.aprovado":
            return f"confirmadas {await confirmar(conn, pedido_id)}"
        if evento["tipo"] == "pagamento.recusado":
            return f"liberadas {await liberar(conn, pedido_id)}"
        return "ignorado"


async def produtos(conn: AsyncConnection) -> list[dict]:
    cursor = conn.cursor(row_factory=dict_row)
    await cursor.execute(
        """
        SELECT p.sku, p.nome, p.preco::float AS preco, p.disponivel,
               coalesce(sum(r.quantidade) FILTER (WHERE r.status = 'reservada'), 0)::int AS reservado
          FROM produtos p LEFT JOIN reservas r ON r.sku = p.sku
         GROUP BY p.sku ORDER BY p.sku
        """
    )
    return await cursor.fetchall()


def ler_mensagem(corpo: bytes) -> dict:
    """Valida o envelope; erro aqui significa mensagem que nunca vai dar certo (vai para a DLQ)."""
    evento = json.loads(corpo)
    for campo in ("id", "tipo", "pedido_id", "dados"):
        if campo not in evento:
            raise ValueError(f"campo obrigatório ausente: {campo}")
    uuid.UUID(evento["id"])
    uuid.UUID(evento["pedido_id"])
    if evento["tipo"] == "pedido.criado" and not evento["dados"].get("itens"):
        raise ValueError("pedido.criado sem itens")
    return evento
