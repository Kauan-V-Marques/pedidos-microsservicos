"""Testes de ponta a ponta: sobem os 3 serviços, o RabbitMQ e o PostgreSQL (docker compose)
e conferem a saga inteira de fora, só pelas APIs públicas."""

import concurrent.futures
import time

import httpx
import pytest

PEDIDOS = "http://localhost:3000"
ESTOQUE = "http://localhost:8000"
RABBIT = "http://localhost:15672/api"
AUTH = ("loja", "loja")


def disponivel(sku: str) -> int:
    produtos = httpx.get(f"{ESTOQUE}/produtos").json()
    return next(p["disponivel"] for p in produtos if p["sku"] == sku)


def criar_pedido(itens: list[tuple[str, int]], cartao: str = "4242") -> str:
    resposta = httpx.post(
        f"{PEDIDOS}/pedidos",
        json={"cliente": "Teste E2E", "cartao_final": cartao, "itens": [{"sku": s, "quantidade": q} for s, q in itens]},
    )
    assert resposta.status_code == 202, resposta.text
    return resposta.json()["id"]


def aguardar_final(pedido_id: str, segundos: float = 30) -> dict:
    limite = time.monotonic() + segundos
    while time.monotonic() < limite:
        pedido = httpx.get(f"{PEDIDOS}/pedidos/{pedido_id}").json()
        if pedido["status"] in ("CONFIRMADO", "CANCELADO"):
            return pedido
        time.sleep(0.2)
    pytest.fail(f"pedido {pedido_id} não terminou: {pedido}")


def test_caminho_feliz_reserva_cobra_e_confirma():
    antes = disponivel("SEN-QTR8A")

    pedido = aguardar_final(criar_pedido([("SEN-QTR8A", 2)]))

    assert pedido["status"] == "CONFIRMADO"
    assert pedido["valor_total"] == pytest.approx(379.80)
    assert [h["status"] for h in pedido["historico"]] == ["AGUARDANDO_ESTOQUE", "AGUARDANDO_PAGAMENTO", "CONFIRMADO"]
    assert disponivel("SEN-QTR8A") == antes - 2


def test_sem_estoque_cancela_sem_cobrar():
    pedido = aguardar_final(criar_pedido([("KIT-SUMO", 10)]))

    assert pedido["status"] == "CANCELADO"
    assert "Sem estoque" in pedido["motivo"]


def test_pagamento_recusado_devolve_o_estoque_compensacao():
    antes = disponivel("MCU-ESP32")

    pedido = aguardar_final(criar_pedido([("MCU-ESP32", 2)], cartao="0000"))

    assert pedido["status"] == "CANCELADO"
    assert "Pagamento recusado" in pedido["motivo"]
    # A compensação é assíncrona: o estoque volta logo depois
    limite = time.monotonic() + 10
    while disponivel("MCU-ESP32") != antes and time.monotonic() < limite:
        time.sleep(0.2)
    assert disponivel("MCU-ESP32") == antes


def test_valor_acima_do_limite_e_recusado():
    pedido = aguardar_final(criar_pedido([("KIT-SUMO", 3)]))

    assert pedido["status"] == "CANCELADO"
    assert "acima do limite" in pedido["motivo"]


def test_pedidos_simultaneos_nao_vendem_alem_do_estoque():
    # 15 baterias, 6 pedidos de 3 ao mesmo tempo: no máximo 5 podem ser confirmados
    antes = disponivel("BAT-2S-850")
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        ids = list(executor.map(lambda _: criar_pedido([("BAT-2S-850", 3)]), range(6)))

    finais = [aguardar_final(i)["status"] for i in ids]

    assert finais.count("CONFIRMADO") == antes // 3
    assert disponivel("BAT-2S-850") == antes - 3 * finais.count("CONFIRMADO")


def test_mensagem_malformada_vai_para_a_fila_de_mortas():
    def mensagens_na_dlq() -> int:
        return httpx.get(f"{RABBIT}/queues/%2F/estoque.eventos.dlq", auth=AUTH).json().get("messages", 0)

    antes = mensagens_na_dlq()
    publicada = httpx.post(
        f"{RABBIT}/exchanges/%2F/loja.eventos/publish",
        auth=AUTH,
        json={"properties": {}, "routing_key": "pedido.criado", "payload": "isto não é JSON", "payload_encoding": "string"},
    )
    assert publicada.json()["routed"] is True

    limite = time.monotonic() + 10
    while mensagens_na_dlq() == antes and time.monotonic() < limite:
        time.sleep(0.3)
    assert mensagens_na_dlq() == antes + 1
