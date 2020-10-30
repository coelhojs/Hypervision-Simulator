#PARA BUILD:
#docker build . -t Hypervision-Simulator
#PARA EXECUTAR [-p PORTA_HOST:PORTA_CONTAINER]:
#docker run -p 5090:80 Hypervision-Simulator --restart=on-failure:10 --ip 192.168.2.93 --port 8082 --option 1 --count 10 --seconds 5

#See https://aka.ms/containerfastmode to understand how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/core/runtime:3.1-buster-slim AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/core/sdk:3.1-buster AS build
WORKDIR /src
COPY ["Hypervision-Simulator/Simulator.csproj", "Hypervision-Simulator/"]
COPY ["commons-core/Commons-Core.csproj", "commons-core/"]
RUN dotnet restore "Hypervision-Simulator/Simulator.csproj"
COPY . .
WORKDIR "/src/Hypervision-Simulator"
RUN dotnet build "Simulator.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Simulator.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
#ENV ip=${""}
#ENV port=${""}
#ENV option=${""}
#ENV count=${""}
#ENV seconds=${""}
ENTRYPOINT ["dotnet", "Simulator.dll"]
#ENTRYPOINT ["dotnet", "Simulator.dll", "--ip ${ip}", "--port ${port}", "--option ${option}", "--count ${count}", "--seconds ${seconds}"]