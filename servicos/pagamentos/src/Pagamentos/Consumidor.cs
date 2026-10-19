using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Pagamentos;

/// <summary>Consome estoque.reservado, decide o pagamento e publica a resposta.</summary>
public sealed class Consumidor(IConfiguration config, ILogger<Consumidor> log) : BackgroundService
{
    private const string Exchange = "loja.eventos";
    private const string Dlx = "loja.dlx";
    private const string Fila = "pagamentos.eventos";

    protected override async Task ExecuteAsync(CancellationToken parar)
    {
        var url = config["RABBITMQ_URL"] ?? "amqp://loja:loja@localhost:5672/";
        await using var conexao = await Conectar(url, parar);

        // Canal separado para publicar, com confirmação do broker a cada mensagem
        await using var publicador = await conexao.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
            parar);
        await using var canal = await conexao.CreateChannelAsync(cancellationToken: parar);
        await DeclararTopologia(canal, parar);
        await canal.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false, parar);

        var consumidor = new AsyncEventingBasicConsumer(canal);
        consumidor.ReceivedAsync += async (_, entrega) =>
        {
            Envelope entrada;
            try
            {
                entrada = Envelope.Ler(entrega.Body.Span);
            }
            catch (JsonException erro)
            {
                log.LogWarning("Mensagem inválida enviada para a DLQ: {Erro}", erro.Message);
                await canal.BasicRejectAsync(entrega.DeliveryTag, requeue: false);
                return;
            }

            try
            {
                var resposta = Regras.Processar(entrada);
                await publicador.BasicPublishAsync(Exchange, resposta.Tipo, mandatory: false,
                    new BasicProperties
                    {
                        ContentType = "application/json",
                        DeliveryMode = DeliveryModes.Persistent,
                        MessageId = resposta.Id.ToString(),
                        CorrelationId = resposta.PedidoId.ToString(),
                        Type = resposta.Tipo,
                    },
                    resposta.Serializar());
                // Só confirma o recebimento depois que o broker confirmou a resposta
                await canal.BasicAckAsync(entrega.DeliveryTag, multiple: false);
                log.LogInformation("[{Pedido}] {Tipo} {Motivo}", entrada.PedidoId, resposta.Tipo,
                    resposta.Dados["motivo"]?.ToString() ?? "");
            }
            catch (ArgumentException erro)
            {
                log.LogWarning("[{Pedido}] evento sem os dados necessários, para a DLQ: {Erro}", entrada.PedidoId, erro.Message);
                await canal.BasicRejectAsync(entrega.DeliveryTag, requeue: false);
            }
            catch (Exception erro)
            {
                log.LogWarning("[{Pedido}] falha temporária, nova tentativa: {Erro}", entrada.PedidoId, erro.Message);
                await canal.BasicNackAsync(entrega.DeliveryTag, multiple: false, requeue: true);
            }
        };

        await canal.BasicConsumeAsync(Fila, autoAck: false, consumidor, parar);
        log.LogInformation("Aguardando eventos em {Fila}", Fila);
        await Task.Delay(Timeout.Infinite, parar).ContinueWith(_ => { }, CancellationToken.None);
    }

    private async Task<IConnection> Conectar(string url, CancellationToken parar)
    {
        var fabrica = new ConnectionFactory { Uri = new Uri(url), AutomaticRecoveryEnabled = true };
        for (var tentativa = 1; ; tentativa++)
        {
            try
            {
                return await fabrica.CreateConnectionAsync("pagamentos", parar);
            }
            catch (Exception) when (tentativa < 30)
            {
                log.LogInformation("RabbitMQ indisponível ({Tentativa}/30), tentando de novo...", tentativa);
                await Task.Delay(TimeSpan.FromSeconds(2), parar);
            }
        }
    }

    /// <summary>Mesma topologia declarada pelos outros serviços (a declaração é idempotente).</summary>
    private static async Task DeclararTopologia(IChannel canal, CancellationToken parar)
    {
        await canal.ExchangeDeclareAsync(Exchange, ExchangeType.Topic, durable: true, cancellationToken: parar);
        await canal.ExchangeDeclareAsync(Dlx, ExchangeType.Direct, durable: true, cancellationToken: parar);
        await canal.QueueDeclareAsync($"{Fila}.dlq", durable: true, exclusive: false, autoDelete: false,
            new Dictionary<string, object?> { ["x-queue-type"] = "quorum" }, cancellationToken: parar);
        await canal.QueueBindAsync($"{Fila}.dlq", Dlx, Fila, cancellationToken: parar);
        await canal.QueueDeclareAsync(Fila, durable: true, exclusive: false, autoDelete: false,
            new Dictionary<string, object?>
            {
                ["x-queue-type"] = "quorum",
                ["x-delivery-limit"] = 5,
                ["x-dead-letter-exchange"] = Dlx,
                ["x-dead-letter-routing-key"] = Fila,
            }, cancellationToken: parar);
        await canal.QueueBindAsync(Fila, Exchange, "estoque.reservado", cancellationToken: parar);
    }
}
