# -------- BUILD --------
FROM maiconfr/go-oracle-basic:1.0 AS builder

WORKDIR /app

COPY go.mod go.sum ./
# RUN go mod download

COPY . .

RUN go build -o go-email
#RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o app

# -------- RUNTIME --------
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    tzdata \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Dependências Oracle
RUN apt-get update && apt-get install -y libaio1 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/lib/oracle /usr/lib/oracle
COPY --from=builder /etc/ld.so.conf.d/oracle-instantclient.conf /etc/ld.so.conf.d/oracle-instantclient.conf
ENV LD_LIBRARY_PATH=/usr/lib/oracle/19.3/client64/lib:$LD_LIBRARY_PATH


ENV TZ=America/Sao_Paulo

WORKDIR /app

COPY --from=builder /app/go-email /app/go-email
COPY crontab /etc/cron.d/root
COPY .env /app/.env

RUN chmod +x /app/go-email

CMD ["cron", "-f"]
