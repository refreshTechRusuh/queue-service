# queue-service

Layanan **Waiting Room (antrian virtual)** untuk **Rusuh Booking Tiket**. Menahan flash
crowd di depan booking: semua orang antre FCFS, dan hanya sejumlah terbatas yang
di-*admit* ke booking room sehingga yang sudah masuk **dijamin dapat tiket** ("jangan PHP").

- Porsi: **BE-1 (Identity & Flow)**
- Arsitektur: Hexagonal (Ports & Adapters), `net/http` + Redis (+ Kafka consumer)
- Port default: **8084**
- State antrian sepenuhnya di **Redis** (sorted set, counter occupancy, cache stok)

## Prasyarat

- Go 1.26+
- Redis (infra bersama di repo `tools` via docker-compose)

## Menjalankan

```bash
# 1. Infra bersama (dari repo tools)
cd tools && docker compose up -d redis

# 2. Jalankan service (dari repo ini)
make run        # = go run ./cmd
```

Cek: `curl localhost:8084/sys/ping` → `{"status":"00000","message":"queue-service ok",...}`.

## Endpoint

| Method | Path        | Keterangan                      | Status        |
|--------|-------------|---------------------------------|---------------|
| GET    | `/sys/ping` | Health check (ikut cek Redis)   | ✅ skeleton    |

Endpoint antrian (join, cek posisi, SSE stream) + consumer Kafka dikerjakan di branch
fitur dan akan masuk lewat PR.

## Cara kerja (ringkas)

```
join ──▶ occupancy < bookingRoomCap ? ──ya──▶ ADMITTED  (Access Token JWT, TTL 5 menit)
                     │
                     └──tidak──▶ QUEUED   (ZADD queue:{eventId}, score = waktu masuk)

bookingRoomCap = stok ≤ 3 ? 1 : floor(stok / 3)
```

Kenapa `floor(stok/3)`: satu booking maksimal 3 tiket. Bila jumlah orang di booking room
≤ stok/3, total permintaan maksimum ≤ stok, jadi hold saat submit pasti berhasil →
tidak ada yang "di-PHP". Detail lengkap: `docs/HLD.md §11`, `design.md §2.1`.

## Konfigurasi (environment variable)

| Env             | Default          | Keterangan                          |
|-----------------|------------------|-------------------------------------|
| `APP_PORT`      | `8084`           | Port HTTP                           |
| `APP_ENV`       | `local`          | `local` / `preprod` / `production`  |
| `LOG_LEVEL`     | `info`           | `debug` / `info` / `warn` / `error` |
| `LOG_FORMAT`    | `json`           | `json` / `text`                     |
| `REDIS_ADDR`    | `localhost:6379` | Redis (state antrian)               |
| `REDIS_PASSWORD`| (kosong)         | Password Redis                      |
| `REDIS_DB`      | `0`              | Nomor DB Redis                      |

## Perintah Makefile

| Target                    | Keterangan                          |
|---------------------------|-------------------------------------|
| `make run`                | Jalankan via `go run ./cmd`         |
| `make build`              | Compile binary ke `bin/api`         |
| `make test`               | Jalankan test                       |
| `make test-race`          | Test dengan race detector           |
| `make unit-test-coverage` | Test + coverage                     |
| `make golint`             | Lint                                |
| `make docker-build`       | Build image `queue-service:latest`  |

## Struktur

```
cmd/main.go                  # entry point: startup + graceful shutdown
internal/
  config/                    # env → Config
  delivery/server.go         # composition root (wiring + route + middleware)
  queue/                     # domain antrian (hexagonal) — ditambahkan di branch fitur
    domain/entity + domain/dto
    port/inbound + port/outbound
    application/usecase/
    infrastructure/{cache,http,event,token}
pkg/
  apinizer/ apperror/        # envelope response + error domain
  redis/ middleware/         # Redis client, TransactionID + Chain
```
