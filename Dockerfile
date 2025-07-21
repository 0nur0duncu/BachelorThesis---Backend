# Go uygulamasını derlemek için ilk aşama
# FROM golang:1.22-alpine AS builder
FROM golang:1.22 AS builder

# Çalışma dizini
WORKDIR /app

# Go mod dosyalarını kopyala ve bağımlılıkları indir
COPY go.* *.go ./
# RUN apk add --no-cache libffi
# RUN apt-get update && apt-get install -y libffi-dev libmupdf-dev
# RUN go get "github.com/gen2brain/go-fitz"
# RUN go mod download
RUN go mod tidy

# Uygulama dosyasını derle
# RUN CGO_ENABLED=0 GOOS=linux go build -o main main.go
RUN CGO_ENABLED=1 GOOS=linux go build -o main main.go

# Nginx ve Go uygulaması için ikinci aşama
FROM nginx:latest

# Çalışma dizini
WORKDIR /app
RUN apt-get update && apt-get install -y libffi-dev libmupdf-dev

# Nginx yapılandırmasını kopyala
# COPY default.conf /etc/nginx/conf.d/default.conf

# Derlenen Go uygulamasını kopyala
COPY --from=builder /app/main /app/main

EXPOSE 5000

# Go uygulamasını başlat ve aynı anda Nginx'i çalıştır
CMD ["sh", "-c", "/app/main & nginx -g 'daemon off;'"]
