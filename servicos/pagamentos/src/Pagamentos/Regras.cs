using System.Globalization;
using System.Text.Json.Nodes;

namespace Pagamentos;

public sealed record Decisao(bool Aprovado, string? Motivo);

/// <summary>Regras da operadora simulada. Determinísticas: a mesma entrada sempre dá a mesma resposta.</summary>
public static class Regras
{
    public const decimal LimitePorPedido = 10_000m;

    public static Decisao Decidir(decimal valor, string cartaoFinal)
    {
        if (cartaoFinal == "0000")
            return new Decisao(false, "cartão recusado pela operadora");
        if (valor > LimitePorPedido)
            return new Decisao(false, $"valor de {Reais(valor)} acima do limite de {Reais(LimitePorPedido)}");
        return new Decisao(true, null);
    }

    /// <summary>R$ 14.670,00 (sem depender dos dados de cultura do sistema, ausentes na imagem mínima)</summary>
    public static string Reais(decimal valor) =>
        "R$ " + valor.ToString("N2", CultureInfo.InvariantCulture).Replace(',', '_').Replace('.', ',').Replace('_', '.');

    /// <summary>Transforma um estoque.reservado no evento de resposta (aprovado ou recusado).</summary>
    public static Envelope Processar(Envelope reservado)
    {
        if (reservado.Tipo != "estoque.reservado")
            throw new ArgumentException($"Evento inesperado: {reservado.Tipo}");

        var valor = reservado.Dados["valor_total"]?.GetValue<decimal>() ?? throw new ArgumentException("Sem valor_total.");
        var cartao = reservado.Dados["cartao_final"]?.GetValue<string>() ?? throw new ArgumentException("Sem cartao_final.");
        var decisao = Decidir(valor, cartao);

        var tipo = decisao.Aprovado ? "pagamento.aprovado" : "pagamento.recusado";
        var dados = new JsonObject { ["valor"] = valor };
        if (decisao.Motivo is not null) dados["motivo"] = decisao.Motivo;
        return new Envelope(Ids.Derivado(reservado.Id, tipo), tipo, DateTimeOffset.UtcNow, reservado.PedidoId, dados);
    }
}
