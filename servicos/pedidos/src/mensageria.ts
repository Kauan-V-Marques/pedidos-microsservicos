import amqp, { type Channel, type ConfirmChannel, type ConsumeMessage } from 'amqplib';
import type pg from 'pg';
import { ZodError } from 'zod';

import { type Envelope, envelopeSchema, TIPOS_RECEBIDOS } from './dominio.js';
import { aplicarEvento } from './repositorio.js';

export const EXCHANGE = 'loja.eventos';
export const DLX = 'loja.dlx';
const FILA = 'pedidos.eventos';

/**
 * Declara a topologia (idempotente: todos os serviços podem declarar a mesma coisa).
 * Fila quorum com limite de entregas: depois de 5 falhas a mensagem vai para a DLQ.
 */
export async function declararFila(canal: Channel, fila: string, chaves: readonly string[]) {
  await canal.assertExchange(EXCHANGE, 'topic', { durable: true });
  await canal.assertExchange(DLX, 'direct', { durable: true });
  await canal.assertQueue(`${fila}.dlq`, { durable: true, arguments: { 'x-queue-type': 'quorum' } });
  await canal.bindQueue(`${fila}.dlq`, DLX, fila);
  await canal.assertQueue(fila, {
    durable: true,
    arguments: {
      'x-queue-type': 'quorum',
      'x-delivery-limit': 5,
      'x-dead-letter-exchange': DLX,
      'x-dead-letter-routing-key': fila,
    },
  });
  for (const chave of chaves) await canal.bindQueue(fila, EXCHANGE, chave);
}

export interface Mensageria {
  publicador: ConfirmChannel;
  fechar: () => Promise<void>;
}

export async function conectar(url: string): Promise<{ conexao: amqp.ChannelModel; consumidor: Channel; publicador: ConfirmChannel }> {
  const conexao = await amqp.connect(url);
  const consumidor = await conexao.createChannel();
  const publicador = await conexao.createConfirmChannel();
  await declararFila(consumidor, FILA, TIPOS_RECEBIDOS);
  return { conexao, consumidor, publicador };
}

export function publicar(canal: ConfirmChannel, evento: Envelope) {
  canal.publish(EXCHANGE, evento.tipo, Buffer.from(JSON.stringify(evento)), {
    persistent: true,
    contentType: 'application/json',
    messageId: evento.id,
    correlationId: evento.pedido_id,
    type: evento.tipo,
  });
}

/**
 * Relay do outbox: publica os eventos pendentes e só marca como publicados depois da
 * confirmação do broker. SKIP LOCKED permite rodar várias réplicas sem publicar em dobro.
 */
export async function publicarPendentes(pool: pg.Pool, canal: ConfirmChannel, limite = 50): Promise<number> {
  const cliente = await pool.connect();
  try {
    await cliente.query('BEGIN');
    const { rows } = await cliente.query<{ id: string; mensagem: Envelope }>(
      `SELECT id, mensagem FROM outbox WHERE publicado_em IS NULL
        ORDER BY criado_em LIMIT $1 FOR UPDATE SKIP LOCKED`,
      [limite],
    );
    if (rows.length === 0) {
      await cliente.query('COMMIT');
      return 0;
    }
    for (const { mensagem } of rows) publicar(canal, mensagem);
    await canal.waitForConfirms();
    await cliente.query('UPDATE outbox SET publicado_em = now() WHERE id = ANY($1::uuid[])', [rows.map((r) => r.id)]);
    await cliente.query('COMMIT');
    return rows.length;
  } catch (erro) {
    await cliente.query('ROLLBACK');
    throw erro;
  } finally {
    cliente.release();
  }
}

export function consumir(pool: pg.Pool, canal: Channel, log: (msg: string) => void) {
  void canal.prefetch(1); // uma por vez: mantém a ordem dos eventos de cada pedido
  return canal.consume(FILA, async (mensagem: ConsumeMessage | null) => {
    if (!mensagem) return;
    let evento: Envelope;
    try {
      evento = envelopeSchema.parse(JSON.parse(mensagem.content.toString()));
    } catch (erro) {
      // Mensagem malformada nunca vai dar certo: direto para a DLQ, sem tentar de novo
      log(`mensagem inválida descartada para a DLQ: ${erro instanceof ZodError ? erro.issues[0]?.message : erro}`);
      canal.nack(mensagem, false, false);
      return;
    }
    try {
      const resultado = await aplicarEvento(pool, evento);
      log(`[${evento.pedido_id}] ${evento.tipo}: ${resultado}`);
      canal.ack(mensagem);
    } catch (erro) {
      // Erro temporário (ex.: banco fora): devolve para a fila; o limite de entregas evita loop infinito
      log(`[${evento.pedido_id}] falha ao processar ${evento.tipo}: ${erro}`);
      canal.nack(mensagem, false, true);
    }
  });
}
