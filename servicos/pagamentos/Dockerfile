FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY nuget.config ./
COPY src/Pagamentos/Pagamentos.csproj src/Pagamentos/
RUN dotnet restore src/Pagamentos/Pagamentos.csproj
COPY src/ src/
RUN dotnet publish src/Pagamentos -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app .
USER $APP_UID
ENTRYPOINT ["dotnet", "Pagamentos.dll"]
