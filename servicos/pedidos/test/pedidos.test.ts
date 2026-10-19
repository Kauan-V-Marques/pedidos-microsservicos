import { randomBytes } from 'node:crypto';

import pg from 'pg';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import { criarApp } from '../src/app.js';
import { proximoStatus } from '../src/dominio.js';
import { aplicarEvento, criarPedido, envelope, migrar } from '../src/repositorio.js';

describe('máquina de estados do pedido', () => {
  it('segue o caminho feliz: estoque reservado e pagamento aprovado', () => {
    expect(proximoStatus('AGUARDANDO_ESTOQUE', 'estoque.reservado')).toEqual({ novo: 'AGUARDANDO_PAGAMENTO' });
    expect(proximoStatus('AGUARDANDO_PAGAMENTO', 'pagamento.aprovado')).toEqual({ novo: 'CONFIRMADO' });
  });

  it('cancela com o motivo quando falta estoque ou o pagamento é recusado', () => {
    expect(proximoStatus('AGUARDANDO_ESTOQUE', 'estoque.recusado', { motivo: 'SEN-01 indisponível' })).toEqual({
      novo: 'CANCELADO',
      motivo: 'Sem estoque: SEN-01 indisponível',
    });
    expect(proximoStatus('AGUARDANDO_PAGAMENTO', 'pagamento.recusado', { motivo: 'cartão bloqueado' })?.novo).toBe(
      'CANCELADO',
    );
  });

  it('pedido finalizado não muda mais, e evento fora de ordem é ignorado', () => {
    expect(proximoStatus('CONFIRMADO', 'pagamento.recusado')).toBeNull();
    expect(proximoStatus('CANCELADO', 'pagamento.aprovado')).toBeNull();
    expect(proximoStatus('AGUARDANDO_PAGAMENTO', 'estoque.reservado')).toBeNull();
    expect(proximoStatus('AGUARDANDO_PAGAMENTO', 'estoque.recusado')).toBeNull();
  });
});

// ---------- com banco de verdade ----------

const SERVIDOR = process.env.TEST_DATABASE_URL ?? 'postgres://loja:loja@localhost:5432/pedidos';
const NOME = `pedidos_teste_${randomBytes(4).toString('hex')}`;
let pool: pg.Pool;

beforeAll(async () => {
  const admin = new pg.Client({ connectionString: SERVIDOR });
  await admin.connect();
  await admin.query(`CREATE DATABASE ${NOME}`);
  await admin.end();
  const url = new URL(SERVIDOR);
  url.pathname = `/${NOME}`;
  pool = new pg.Pool({ connectionString: url.toString() });
  await migrar(pool);
});

beforeEach(async () => {
  await pool.query('TRUNCATE itens_pedido, pedidos, outbox, mensagens_processadas');
});

afterAll(async () => {
  await pool.end();
  const admin = new pg.Client({ connectionString: SERVIDOR });
  await admin.connect();
  await admin.query(`DROP DATABASE IF EXISTS ${NOME} WITH (FORCE)`);
  await admin.end();
});

const novo = { cliente: 'Ana', cartao_final: '4242', itens: [{ sku: 'sen-qtr8a', quantidade: 2 }] };

describe('pedidos com banco', () => {
  it('criar o pedido grava o evento no outbox na mesma transação', async () => {
    const { id } = await criarPedido(pool, { ...novo, itens: [{ sku: 'SEN-QTR8A', quantidade: 2 }] });

    const { rows } = await pool.query('SELECT tipo, mensagem FROM outbox WHERE pedido_id = $1', [id]);
    expect(rows).toHaveLength(1);
    expect(rows[0].tipo).toBe('pedido.criado');
    expect(rows[0].mensagem.dados.itens).toEqual([{ sku: 'SEN-QTR8A', quantidade: 2 }]);
  });

  it('a mesma mensagem entregue duas vezes só tem efeito uma vez', async () => {
    const { id } = await criarPedido(pool, { ...novo, itens: [{ sku: 'SEN-QTR8A', quantidade: 2 }] });
    const reservado = envelope('estoque.reservado', id, { valor_total: 379.8 });

    expect(await aplicarEvento(pool, reservado)).toBe('aplicado');
    expect(await aplicarEvento(pool, reservado)).toBe('repetido');

    const { rows } = await pool.query('SELECT status, valor_total::float AS valor, historico FROM pedidos WHERE id = $1', [id]);
    expect(rows[0]).toMatchObject({ status: 'AGUARDANDO_PAGAMENTO', valor: 379.8 });
    expect(rows[0].historico).toHaveLength(2);
  });

  it('API: valida o pedido, aceita com 202 e mostra o andamento', async () => {
    const app = criarApp(pool);

    const invalido = await app.inject({ method: 'POST', url: '/pedidos', payload: { cliente: 'A', itens: [] } });
    const aceito = await app.inject({ method: 'POST', url: '/pedidos', payload: novo });
    const { id } = aceito.json();
    await aplicarEvento(pool, envelope('estoque.recusado', id, { motivo: 'SEN-QTR8A indisponível' }));
    const consulta = await app.inject({ method: 'GET', url: `/pedidos/${id}` });

    expect(invalido.statusCode).toBe(400);
    expect(aceito.statusCode).toBe(202);
    expect(aceito.headers.location).toBe(`/pedidos/${id}`);
    expect(consulta.json()).toMatchObject({
      status: 'CANCELADO',
      motivo: 'Sem estoque: SEN-QTR8A indisponível',
      itens: [{ sku: 'SEN-QTR8A', quantidade: 2 }],
    });
    await app.close();
  });
});
