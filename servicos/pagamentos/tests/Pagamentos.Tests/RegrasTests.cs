using System.Text;
using System.Text.Json.Nodes;
using Pagamentos;

namespace Pagamentos.Tests;

public class RegrasTests
{
    private static Envelope Reservado(decimal valor, string cartao, Guid? id = null) =>
        new(id ?? Guid.NewGuid(), "estoque.reservado", DateTimeOffset.UtcNow, Guid.NewGuid(),
            new JsonObject { ["valor_total"] = valor, ["cartao_final"] = cartao, ["itens"] = new JsonArray() });

    [Fact]
    public void Aprova_pedido_normal()
    {
        Assert.Equal(new Decisao(true, null), Regras.Decidir(379.80m, "4242"));
    }

    [Fact]
    public void Recusa_cartao_bloqueado()
    {
        var decisao = Regras.Decidir(100m, "0000");
        Assert.False(decisao.Aprovado);
        Assert.Contains("operadora", decisao.Motivo);
    }

    [Theory]
    [InlineData(10_000, true)]
    [InlineData(10_000.01, false)]
    public void Respeita_o_limite_por_pedido(decimal valor, bool aprovado)
    {
        Assert.Equal(aprovado, Regras.Decidir(valor, "4242").Aprovado);
    }

    [Fact]
    public void Processa_estoque_reservado_e_gera_a_resposta_do_mesmo_pedido()
    {
        var entrada = Reservado(14_670m, "4242");

        var saida = Regras.Processar(entrada);

        Assert.Equal("pagamento.recusado", saida.Tipo);
        Assert.Equal(entrada.PedidoId, saida.PedidoId);
        Assert.Equal(14_670m, saida.Dados["valor"]!.GetValue<decimal>());
        Assert.Equal("valor de R$ 14.670,00 acima do limite de R$ 10.000,00", saida.Dados["motivo"]!.GetValue<string>());
    }

    [Fact]
    public void Mensagem_repetida_gera_resposta_com_o_mesmo_id()
    {
        var id = Guid.NewGuid();

        var primeira = Regras.Processar(Reservado(100m, "4242", id));
        var segunda = Regras.Processar(Reservado(100m, "4242", id));

        Assert.Equal(primeira.Id, segunda.Id);
        Assert.NotEqual(id, primeira.Id);
        Assert.Equal(5, primeira.Id.Version);
    }

    [Fact]
    public void Le_e_escreve_o_envelope_em_snake_case()
    {
        var json = """
            {"id":"7d3f1a2e-1111-4c4c-9a9a-123456789abc","tipo":"estoque.reservado","ocorrido_em":"2026-10-19T10:00:00Z",
             "pedido_id":"0b1c2d3e-2222-4d4d-8b8b-abcdefabcdef","dados":{"valor_total":99.9,"cartao_final":"1234"}}
            """;

        var envelope = Envelope.Ler(Encoding.UTF8.GetBytes(json));
        var texto = Encoding.UTF8.GetString(Regras.Processar(envelope).Serializar());

        Assert.Equal(Guid.Parse("0b1c2d3e-2222-4d4d-8b8b-abcdefabcdef"), envelope.PedidoId);
        Assert.Contains("\"pedido_id\":\"0b1c2d3e-2222-4d4d-8b8b-abcdefabcdef\"", texto);
        Assert.Contains("\"tipo\":\"pagamento.aprovado\"", texto);
    }

    [Fact]
    public void Evento_sem_dados_obrigatorios_e_rejeitado()
    {
        var sem_valor = new Envelope(Guid.NewGuid(), "estoque.reservado", DateTimeOffset.UtcNow, Guid.NewGuid(),
            new JsonObject { ["cartao_final"] = "4242" });

        Assert.Throws<ArgumentException>(() => Regras.Processar(sem_valor));
    }
}
