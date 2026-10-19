import { setTimeout as esperar } from 'node:timers/promises';

import pg from 'pg';

import { criarApp } from './app.js';
import { conectar, consumir, publicarPendentes } from './mensageria.js';
import { migrar } from './repositorio.js';

const DATABASE_URL = process.env.DATABASE_URL ?? 'postgres://loja:loja@localhost:5432/pedidos';
const RABBITMQ_URL = process.env.RABBITMQ_URL ?? 'amqp://loja:loja@localhost:5672';
const PORTA = Number(process.env.PORT ?? 3000);

const log = (mensagem: string) => console.log(`${new Date().toISOString()} pedidos: ${mensagem}`);

async function conectarComRetentativa() {
  for (let tentativa = 1; ; tentativa++) {
    try {
      return await conectar(RABBITMQ_URL);
    } catch (erro) {
      if (tentativa >= 30) throw erro;
      log(`RabbitMQ indisponível (${tentativa}/30), tentando de novo...`);
      await esperar(2000);
    }
  }
}

async function main() {
  const pool = new pg.Pool({ connectionString: DATABASE_URL, max: 10 });
  await migrar(pool);
  const { conexao, consumidor, publicador } = await conectarComRetentativa();
  conexao.on('close', () => {
    log('conexão com o RabbitMQ caiu; encerrando para o orquestrador reiniciar');
    process.exit(1);
  });

  await consumir(pool, consumidor, log);

  let ativo = true;
  const relay = (async () => {
    while (ativo) {
      try {
        const publicadas = await publicarPendentes(pool, publicador);
        if (publicadas > 0) log(`outbox: ${publicadas} evento(s) publicados`);
        else await esperar(250);
      } catch (erro) {
        log(`outbox: falha ao publicar (${erro}); nova tentativa em 1 s`);
        await esperar(1000);
      }
    }
  })();

  const app = criarApp(pool);
  await app.listen({ port: PORTA, host: '0.0.0.0' });
  log(`API em http://0.0.0.0:${PORTA}`);

  const encerrar = async () => {
    ativo = false;
    await relay;
    await app.close();
    await conexao.close();
    await pool.end();
    process.exit(0);
  };
  process.on('SIGTERM', encerrar);
  process.on('SIGINT', encerrar);
}

main().catch((erro) => {
  console.error(erro);
  process.exit(1);
});
