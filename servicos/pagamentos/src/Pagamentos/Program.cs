using Pagamentos;

var builder = Host.CreateApplicationBuilder(args);
builder.Logging.AddSimpleConsole(o =>
{
    o.SingleLine = true;
    o.TimestampFormat = "yyyy-MM-ddTHH:mm:ss ";
});
builder.Services.AddHostedService<Consumidor>();

await builder.Build().RunAsync();
