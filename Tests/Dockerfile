FROM mcr.microsoft.com/dotnet/sdk:3.1

# install debugger
WORKDIR /vsdbg
RUN apt-get update \
    && apt-get install -y --no-install-recommends unzip \
    && rm -rf /var/lib/apt/lists/* \
    && curl -sSL https://aka.ms/getvsdbgsh | bash /dev/stdin -v latest -l /vsdbg
    
WORKDIR /app
ENTRYPOINT ["tail", "-f", "/dev/null"]
