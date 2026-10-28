# ---- Tahap 1: builder ----
FROM golang:1.26-alpine AS builder
WORKDIR /app

# Cache dependency: layer ini dipakai ulang selama go.mod/go.sum tidak berubah.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/api ./cmd

# ---- Tahap 2: runtime ----
# distroless/static = image minimal tanpa shell; :nonroot = jalan sebagai user biasa.
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=builder /app/bin/api .
EXPOSE 8084
USER nonroot:nonroot
ENTRYPOINT ["/app/api"]
