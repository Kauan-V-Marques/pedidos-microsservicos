import { randomUUID } from 'node:crypto';

import type pg from 'pg';

import { type Envelope, type NovoPedido, proximoStatus, type Status, type TipoRecebido } from './dominio.js';

export const MIGRACAO = `
CREATE TABLE IF NOT EXISTS pedidos (
    id           uuid PRIMARY KEY,
    cliente      text        NOT NULL,
    cartao_final char(4)     NOT NULL,
    status       text        NOT NULL,
    motivo       text,
    valor_total  numeric(12, 2),
    criado_em    timestamptz NOT NULL DEFAULT now(),
    atualizado_em timestamptz NOT NULL DEFAULT now(),
    historico    jsonb       NOT NULL DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS itens_pedido (
    pedido_id  uuid    NOT NULL REFERENCES pedidos (id),
    sku        text    NOT NULL,
    quantidade integer NOT NULL CHECK (quantidade > 0),
    PRIMARY KEY (pedido_id, sku)
);
-- Outbox: o evento é gravado na mesma transação do pedido e publicado depois por um relay.
-- Assim nunca existe pedido salvo sem evento (nem evento de um pedido que não foi salvo).
CREATE TABLE IF NOT EXISTS outbox (
    id            uuid PRIMARY KEY,
    tipo          text        NOT NULL,
    pedido_id     uuid        NOT NULL,
    mensagem      jsonb       NOT NULL,
    criado_em     timestamptz NOT NULL DEFAULT clock_timestamp(),
    publicado_em  timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_pendentes ON outbox (criado_em) WHERE publicado_em IS NULL;
-- Inbox: ids das mensagens já processadas (consumidor idempotente)
CREATE TABLE IF NOT EXISTS mensagens_processadas (
    id             uuid PRIMARY KEY,
    processada_em  timestamptz NOT NULL DEFAULT now()
);
`;

export function envelope(tipo: string, pedidoId: string, dados: Record<string, unknown>): Envelope {
  return { id: randomUUID(), tipo, ocorrido_em: new Date().toISOString(), pedido_id: pedidoId, dados };
}

export async function migrar(pool: pg.Pool) {
  await pool.query(MIGRACAO);
}

export async function criarPedido(pool: pg.Pool, pedido: NovoPedido): Promise<{ id: string; status: Status }> {
  const id = randomUUID();
  const cliente = await pool.connect();
  try {
    await cliente.query('BEGIN');
    await cliente.query(
      `INSERT INTO pedidos (id, cliente, cartao_final, status, historico)
       VALUES ($1, $2, $3, 'AGUARDANDO_ESTOQUE', jsonb_build_array(jsonb_build_object('status', 'AGUARDANDO_ESTOQUE', 'em', now())))`,
      [id, pedido.cliente, pedido.cartao_final],
    );
    for (const item of pedido.itens) {
      await cliente.query('INSERT INTO itens_pedido (pedido_id, sku, quantidade) VALUES ($1, $2, $3)', [
        id,
        item.sku,
        item.quantidade,
      ]);
    }
    const evento = envelope('pedido.criado', id, { itens: pedido.itens, cartao_final: pedido.cartao_final });
    await cliente.query('INSERT INTO outbox (id, tipo, pedido_id, mensagem) VALUES ($1, $2, $3, $4)', [
      evento.id,
      evento.tipo,
      id,
      evento,
    ]);
    await cliente.query('COMMIT');
    return { id, status: 'AGUARDANDO_ESTOQUE' };
  } catch (erro) {
    await cliente.query('ROLLBACK');
    throw erro;
  } finally {
    cliente.release();
  }
}

/**
 * Aplica um evento recebido. Idempotente: a mesma mensagem entregue duas vezes só tem efeito uma vez.
 * Devolve o que aconteceu, para log e testes.
 */
export async function aplicarEvento(pool: pg.Pool, evento: Envelope): Promise<'aplicado' | 'repetido' | 'ignorado'> {
  const cliente = await pool.connect();
  try {
    await cliente.query('BEGIN');
    const nova = await cliente.query('INSERT INTO mensagens_processadas (id) VALUES ($1) ON CONFLICT DO NOTHING', [evento.id]);
    if (nova.rowCount === 0) {
      await cliente.query('ROLLBACK');
      return 'repetido';
    }

    const { rows } = await cliente.query<{ status: Status }>('SELECT status FROM pedidos WHERE id = $1 FOR UPDATE', [
      evento.pedido_id,
    ]);
    const atual = rows[0];
    const transicao = atual ? proximoStatus(atual.status, evento.tipo as TipoRecebido, evento.dados) : null;
    if (!transicao) {
      await cliente.query('COMMIT'); // registra como processada mesmo sem efeito
      return 'ignorado';
    }

    const valor = evento.tipo === 'estoque.reservado' ? Number(evento.dados.valor_total) : null;
    await cliente.query(
      `UPDATE pedidos
          SET status = $2, motivo = $3, valor_total = coalesce($4, valor_total), atualizado_em = now(),
              historico = historico || jsonb_build_array(jsonb_build_object('status', $2::text, 'evento', $5::text, 'em', now()))
        WHERE id = $1`,
      [evento.pedido_id, transicao.novo, transicao.motivo ?? null, valor, evento.tipo],
    );
    await cliente.query('COMMIT');
    return 'aplicado';
  } catch (erro) {
    await cliente.query('ROLLBACK');
    throw erro;
  } finally {
    cliente.release();
  }
}

export async function buscarPedido(pool: pg.Pool, id: string) {
  const { rows } = await pool.query(
    `SELECT p.id, p.cliente, p.status, p.motivo, p.valor_total::float AS valor_total, p.criado_em, p.atualizado_em,
            p.historico,
            (SELECT jsonb_agg(jsonb_build_object('sku', i.sku, 'quantidade', i.quantidade) ORDER BY i.sku)
               FROM itens_pedido i WHERE i.pedido_id = p.id) AS itens
       FROM pedidos p WHERE p.id = $1`,
    [id],
  );
  return rows[0] ?? null;
}

export async function listarPedidos(pool: pg.Pool, status?: Status) {
  const { rows } = await pool.query(
    `SELECT id, cliente, status, motivo, valor_total::float AS valor_total, criado_em
       FROM pedidos WHERE ($1::text IS NULL OR status = $1) ORDER BY criado_em DESC LIMIT 100`,
    [status ?? null],
  );
  return rows;
}
