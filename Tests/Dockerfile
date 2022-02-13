FROM mcr.microsoft.com/dotnet/sdk:6.0-alpine

# install debugger
WORKDIR /vsdbg
RUN curl -sSL https://aka.ms/getvsdbgsh | sh /dev/stdin -v latest -l /vsdbg
    
WORKDIR /app
ENTRYPOINT ["tail", "-f", "/dev/null"]
