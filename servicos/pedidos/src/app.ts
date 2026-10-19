import Fastify from 'fastify';
import type pg from 'pg';
import { z, ZodError } from 'zod';

import { novoPedidoSchema, STATUS } from './dominio.js';
import { buscarPedido, criarPedido, listarPedidos } from './repositorio.js';

export function criarApp(pool: pg.Pool, logger = false) {
  const app = Fastify({ logger });

  app.setErrorHandler((erro, _request, reply) => {
    if (erro instanceof ZodError) {
      return reply.status(400).send({ erro: 'Pedido inválido.', detalhes: erro.issues.map((i) => i.message) });
    }
    app.log.error(erro);
    return reply.status(500).send({ erro: 'Erro interno.' });
  });

  app.get('/saude', async () => {
    await pool.query('SELECT 1');
    return { status: 'ok' };
  });

  // 202: o pedido foi aceito, mas a confirmação acontece de forma assíncrona (saga)
  app.post('/pedidos', async (request, reply) => {
    const pedido = await criarPedido(pool, novoPedidoSchema.parse(request.body));
    return reply.status(202).header('Location', `/pedidos/${pedido.id}`).send(pedido);
  });

  app.get('/pedidos/:id', async (request, reply) => {
    const { id } = z.object({ id: z.uuid() }).parse(request.params);
    const pedido = await buscarPedido(pool, id);
    return pedido ? pedido : reply.status(404).send({ erro: 'Pedido não encontrado.' });
  });

  app.get('/pedidos', async (request) => {
    const { status } = z.object({ status: z.enum(STATUS).optional() }).parse(request.query);
    return listarPedidos(pool, status);
  });

  return app;
}
