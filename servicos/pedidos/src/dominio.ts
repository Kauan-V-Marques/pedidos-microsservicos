import { z } from 'zod';

export const STATUS = ['AGUARDANDO_ESTOQUE', 'AGUARDANDO_PAGAMENTO', 'CONFIRMADO', 'CANCELADO'] as const;
export type Status = (typeof STATUS)[number];
const FINAIS: ReadonlySet<Status> = new Set(['CONFIRMADO', 'CANCELADO']);

/** Eventos que o serviço de pedidos escuta (publicados por estoque e pagamentos). */
export const TIPOS_RECEBIDOS = ['estoque.reservado', 'estoque.recusado', 'pagamento.aprovado', 'pagamento.recusado'] as const;
export type TipoRecebido = (typeof TIPOS_RECEBIDOS)[number];

export const novoPedidoSchema = z.object({
  cliente: z.string().trim().min(2).max(80),
  cartao_final: z.string().regex(/^\d{4}$/, 'Informe os 4 últimos dígitos do cartão'),
  itens: z
    .array(z.object({ sku: z.string().trim().toUpperCase().min(3).max(20), quantidade: z.number().int().min(1).max(100) }))
    .min(1, 'O pedido precisa de pelo menos um item')
    .max(20)
    .refine((itens) => new Set(itens.map((i) => i.sku)).size === itens.length, 'SKU repetido no pedido'),
});
export type NovoPedido = z.infer<typeof novoPedidoSchema>;

/** Envelope de todas as mensagens trocadas entre os serviços. */
export const envelopeSchema = z.object({
  id: z.uuid(),
  tipo: z.string(),
  ocorrido_em: z.string(),
  pedido_id: z.uuid(),
  dados: z.record(z.string(), z.unknown()),
});
export type Envelope = z.infer<typeof envelopeSchema>;

export interface Transicao {
  novo: Status;
  motivo?: string;
}

/**
 * Máquina de estados do pedido. Devolve null quando o evento não muda nada:
 * pedido já finalizado, evento repetido ou fora de ordem.
 */
export function proximoStatus(atual: Status, tipo: TipoRecebido, dados: Record<string, unknown> = {}): Transicao | null {
  if (FINAIS.has(atual)) return null;
  switch (tipo) {
    case 'estoque.reservado':
      return atual === 'AGUARDANDO_ESTOQUE' ? { novo: 'AGUARDANDO_PAGAMENTO' } : null;
    case 'estoque.recusado':
      return atual === 'AGUARDANDO_ESTOQUE'
        ? { novo: 'CANCELADO', motivo: `Sem estoque: ${String(dados.motivo ?? 'itens indisponíveis')}` }
        : null;
    case 'pagamento.aprovado':
      return { novo: 'CONFIRMADO' };
    case 'pagamento.recusado':
      return { novo: 'CANCELADO', motivo: `Pagamento recusado: ${String(dados.motivo ?? 'sem motivo')}` };
  }
}
