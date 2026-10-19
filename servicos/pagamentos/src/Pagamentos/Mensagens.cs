using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Pagamentos;

/// <summary>Envelope comum a todos os eventos trocados entre os serviços.</summary>
public sealed record Envelope(Guid Id, string Tipo, DateTimeOffset OcorridoEm, Guid PedidoId, JsonObject Dados)
{
    public static readonly JsonSerializerOptions Json = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };

    public static Envelope Ler(ReadOnlySpan<byte> corpo) =>
        JsonSerializer.Deserialize<Envelope>(corpo, Json) ?? throw new JsonException("Mensagem vazia.");

    public byte[] Serializar() => JsonSerializer.SerializeToUtf8Bytes(this, Json);
}

public static class Ids
{
    /// <summary>
    /// Id da resposta derivado do id da mensagem recebida. Se a mesma mensagem for entregue
    /// duas vezes, a resposta sai com o mesmo id e o inbox dos outros serviços descarta a repetida.
    /// </summary>
    public static Guid Derivado(Guid origem, string tipo)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes($"{origem:N}:{tipo}"));
        var bytes = hash[..16];
        bytes[6] = (byte)((bytes[6] & 0x0F) | 0x50); // versão 5 (baseado em nome)
        bytes[8] = (byte)((bytes[8] & 0x3F) | 0x80); // variante RFC 4122
        return new Guid(bytes, bigEndian: true);
    }
}
