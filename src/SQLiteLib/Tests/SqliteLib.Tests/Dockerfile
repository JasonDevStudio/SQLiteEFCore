#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/runtime:7.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
WORKDIR /src
COPY ["Tests/SqliteLib.Tests/SqliteLib.Tests.csproj", "Tests/SqliteLib.Tests/"]
COPY ["Lib/SQLiteLib/SQLiteLib.csproj", "Lib/SQLiteLib/"]
RUN dotnet restore "Tests/SqliteLib.Tests/SqliteLib.Tests.csproj"
COPY . .
WORKDIR "/src/Tests/SqliteLib.Tests"
RUN dotnet build "SqliteLib.Tests.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "SqliteLib.Tests.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "SqliteLib.Tests.dll"]