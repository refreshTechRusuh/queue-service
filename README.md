# queue-service

Layanan **Waiting Room (antrian virtual)** untuk **Rusuh Booking Tiket**. Menahan flash
crowd di depan booking: semua orang antre FCFS, dan hanya sejumlah terbatas yang
di-*admit* ke booking room sehingga yang sudah masuk **dijamin dapat tiket** ("jangan PHP").

- Porsi: **BE-1 (Identity & Flow)**
- Arsitektur: Hexagonal (Ports & Adapters), `net/http` + Redis + Kafka consumer
- Port default: **8084**
- State antrian sepenuhnya di **Redis**; operasi kritikal atomik lewat **Lua script**

## Prasyarat

- Go 1.26+
- Redis, PostgreSQL, Kafka (infra bersama di repo `tools` via docker-compose)

## Menjalankan

```bash
# 1. Infra bersama (dari repo tools)
cd tools && docker compose up -d postgres redis zookeeper kafka kafka-init

# 2. Jalankan service (dari repo ini)
make run        # = go run ./cmd
```

Tanpa Kafka (dev saja): `KAFKA_ENABLED=false make run` — event dari BE-2 tidak dikonsumsi.

## Endpoint

| Method | Path                                                  | Keterangan                                   |
|--------|-------------------------------------------------------|----------------------------------------------|
| POST   | `/api/queue/v1/event/{eventId}/join`                  | Masuk antrian. Tanpa body, tanpa login, tanpa NIK |
| GET    | `/api/queue/v1/event/{eventId}/{queueToken}`          | Cek status/posisi (dipakai saat refresh)     |
| GET    | `/api/queue/v1/event/{eventId}/{queueToken}/stream`   | **SSE** posisi real-time                     |
| GET    | `/sys/ping`                                           | Health check (ikut cek Redis)                |

Login tidak diperlukan (guest boleh antre). Identitas antrian = `queueToken` yang
persisten di Redis (bukan session browser), jadi refresh tidak kehilangan posisi.

### Response `data` (join & cek status)

```jsonc
// langsung masuk
{ "status":"ADMITTED", "queueToken":"q_...", "accessToken":"<jwt>", "bookingExpiresAt":"2026-11-01T10:05:00Z" }
// antre
{ "status":"QUEUED", "queueToken":"q_...", "position":12, "etaSeconds":40 }
// status lain: "EXPIRED" (kesempatan habis), "SOLD_OUT" (antrian ditutup)
```

`etaSeconds` disembunyikan bila laju admit belum terukur.

### Event SSE

| Event      | Data                                   | Setelahnya            |
|------------|----------------------------------------|-----------------------|
| `position` | `{position, etaSeconds?}` (bila berubah) | lanjut              |
| `admitted` | `{accessToken, bookingExpiresAt}`      | stream ditutup → redirect ke Detail Ticket |
| `sold_out` | `{message}`                            | stream ditutup        |
| `expired`  | `{message}`                            | stream ditutup        |

Selain event, server mengirim heartbeat `: ping` tiap `QUEUE_SSE_TICK_SECONDS`.

### Error

| HTTP | Kode              | Kapan                                         |
|------|-------------------|-----------------------------------------------|
| 400  | `VALIDATION_ERROR`| eventId bukan UUID / queueToken salah format  |
| 404  | `NOT_FOUND`       | event tidak punya kategori, queueToken tak dikenal |
| 410  | `SOLD_OUT`        | tiket habis, antrian ditutup                  |
| 429  | `RATE_LIMITED`    | join per IP melebihi batas per menit          |
| 503  | `QUEUE_FULL`      | antrian penuh — "ramai, coba lagi" (≠ sold out) |

## Cara kerja

```
join ──▶ masuk antrian (nomor urut) ──▶ promosi dari DEPAN selama occupancy < bookingRoomCap
            │                                  │
            │                                  ├─ ter-admit → ADMITTED (Access Token JWT, TTL 5 menit)
            │                                  └─ belum     → QUEUED (posisi)
            └─ ditolak bila: sold out (410) · antrian penuh (503)

bookingRoomCap = min( stok ≤ 3 ? 1 : floor(stok / 3) , GLOBAL_BOOKING_CAP )
```

**Kenapa `floor(stok/3)`:** satu booking maksimal 3 tiket. Bila jumlah orang di booking
room ≤ stok/3, total permintaan maksimum ≤ stok, jadi hold saat submit pasti berhasil →
tidak ada yang di-PHP. Stok ≤ 3 → hanya 1 orang sekaligus.

**Kenapa atomik:** satu Lua script menjalankan "cek kursi kosong → admit → INCR
occupancy" tanpa diselingi request lain. Tanpa ini, dua orang bisa sama-sama melihat
"ada 1 kursi" dan sama-sama masuk.

**Kenapa nomor urut, bukan timestamp:** banyak orang bisa datang di milidetik yang sama;
score seri membuat Redis mengurutkan berdasarkan teks token, sehingga pendatang
belakangan bisa terselip. Score = `INCR queue:seq:{e}` menjamin FCFS ketat.

### Kapan occupancy (kursi terisi) turun

1. **Access Token kedaluwarsa** (sweeper tiap `QUEUE_SWEEP_INTERVAL_MS`). Ini jalur utama:
   orang yang di-admit tapi tidak pernah submit tidak punya booking, jadi tidak akan
   pernah ada `slot.released` untuknya — tanpa sweeper kursinya bocor dan antrian macet.
   Aman dari PHP: setelah token kedaluwarsa ia tak bisa submit lagi; bila sempat submit,
   tiketnya sudah di-hold (sudah keluar dari stok) sehingga kursi cadangannya tak dibutuhkan.
2. **`slot.released` dari BE-2 hanya memicu promosi, tidak melepas kursi.** Payload-nya tidak
   membawa identitas orang di booking room; bila ikut menurunkan occupancy, satu orang bisa
   terhitung keluar dua kali (sekali oleh sweeper) → orang ekstra masuk → risiko PHP.
   BE-2 tidak perlu mengubah payload. (Opsional: bila kelak event membawa `queueToken`,
   kursi orang itu dilepas lebih awal, idempoten per token.)

### Event Kafka yang dikonsumsi (consumer group `queue-service`)

| Topic            | Efek                                                              |
|------------------|-------------------------------------------------------------------|
| `stock.changed`  | perbarui cache stok kategori (last-write-wins) → coba promosi     |
| `slot.released`  | lepas kursi bila ada `queueToken` → promosi gelombang             |
| `event.sold_out` | tutup antrian (idempoten), keluarkan yang menunggu, SSE `sold_out` |

Pesan rusak dilewati; error sementara di-retry 3x lalu dilewati (handler idempoten,
sweeper & event berikutnya memulihkan state).

### Stok awal (cold start)

`stock.changed` hanya dikirim saat stok **berubah**. Event yang baru dibuat belum pernah
mengirimnya, sehingga saat cache stok kosong queue membaca `ticket_category` **sekali**
(read-only) lalu selanjutnya hanya mengikuti `stock.changed`. Queue tidak pernah menulis DB.

Pembacaan terjadi saat **orang pertama join** (saat penjualan benar-benar dibuka), bukan saat
event/kategori dibuat — jadi tidak ada state yang harus bertahan berbulan-bulan di Redis, dan
bila Redis kehilangan cache, queue cukup membaca ulang. Saat gerbang dibuka ribuan join datang
bersamaan; `singleflight` memastikan per instance hanya **satu** yang membaca DB, sisanya
menunggu hasil yang sama.

### Multi-instance (Cloud Run)

Promosi bisa terjadi di instance yang membaca Kafka, sementara koneksi SSE user ada di
instance lain. Sinyal disebar lewat Redis Pub/Sub `queue:notify:<eventId>`; tiap instance
membangunkan koneksi SSE lokal. Semua key satu event memakai hash tag `{eventId}` sehingga
berada di slot Redis yang sama (siap Redis Cluster).

## Struktur key Redis

| Key                          | Tipe   | Isi                                          |
|------------------------------|--------|----------------------------------------------|
| `queue:{e}`                  | ZSET   | antrian (member token, score nomor urut)     |
| `queue:seq:{e}`              | STRING | counter nomor urut                           |
| `queue:token:{e}:<token>`    | HASH   | status, expiresAt                            |
| `admitted:{e}`               | ZSET   | token di booking room (score = expiresAt ms) |
| `admission:{e}`              | STRING | **occupancy** (jumlah orang di booking room) |
| `stock:{e}`                  | HASH   | available per kategori                       |
| `admission:log:{e}`          | ZSET   | jejak admit 5 menit (laju → ETA)             |
| `queue:closed:{e}`           | STRING | flag sold out                                |
| `queue:events`               | SET    | event dengan antrian aktif (untuk sweeper)   |
| `ratelimit:join:<ip>:<menit>`| STRING | counter rate limit                           |

## Access Token

JWT HS256 dengan claim yang cocok dengan validasi booking/event/payment-service:
`eventId`, `queueToken`, `bookingTtlExpiresAt`, `exp`, `iat`, `sub`. Claim `nik` (opsional di
booking-service) tidak diisi, karena join antrian memang tanpa identitas.
Self-verifiable: service lain memvalidasi lokal dengan `ACCESS_TOKEN_SECRET` yang sama,
tanpa memanggil queue. Token dibangun ulang secara deterministik dari state, jadi refresh
mengembalikan token yang identik.

## Konfigurasi (environment variable)

| Env                          | Default                           | Keterangan                              |
|------------------------------|-----------------------------------|-----------------------------------------|
| `APP_PORT`                   | `8084`                            | Port HTTP                               |
| `APP_ENV`                    | `local`                           | `production` mewajibkan secret non-default |
| `LOG_LEVEL` / `LOG_FORMAT`   | `info` / `json`                   | Logging slog                            |
| `REDIS_ADDR`                 | `localhost:6379`                  | State antrian                           |
| `REDIS_PASSWORD` / `REDIS_DB`| (kosong) / `0`                    |                                         |
| `DB_HOST` ... `DB_NAME`      | `localhost:5432` `rusuh_booking`  | Hanya untuk warm-up stok (read-only)    |
| `ACCESS_TOKEN_SECRET`        | `dev-secret-change-in-production` | **Harus sama** dengan booking/event/payment |
| `BOOKING_TTL_SECONDS`        | `300`                             | Umur kursi / Access Token (BR-03)       |
| `GLOBAL_BOOKING_CAP`         | `1000`                            | Plafon orang di booking room per event  |
| `QUEUE_CAP`                  | `100000`                          | Panjang antrian maksimum (Req 22)       |
| `QUEUE_JOIN_RATE_PER_MIN`    | `30`                              | Batas join per IP per menit (0 = mati)  |
| `QUEUE_SWEEP_INTERVAL_MS`    | `1000`                            | Interval sweeper kursi kedaluwarsa      |
| `QUEUE_SSE_TICK_SECONDS`     | `2`                               | Cek ulang + heartbeat SSE               |
| `QUEUE_STATE_TTL_SECONDS`    | `21600`                           | Umur data token di Redis                |
| `KAFKA_ENABLED`              | `true`                            | Konsumsi event BE-2                     |
| `KAFKA_BROKERS`              | `localhost:29092`                 | Lokal: port host docker-compose         |
| `KAFKA_USERNAME` / `KAFKA_PASSWORD` / `KAFKA_TLS` | (kosong) / `false` | SASL/TLS untuk Confluent      |
| `KAFKA_GROUP_ID`             | `queue-service`                   | Consumer group                          |

## Test

```bash
make test-race    # butuh Redis lokal; test DB & Kafka di-skip otomatis bila tidak ada
```

Test kritikal (bukti anti-PHP) berjalan terhadap Redis **sungguhan**, karena atomisitas
Lua tidak bisa dibuktikan dengan mock:

- stok 1 + 2 orang join bersamaan → tepat 1 ADMITTED (diulang 50x)
- 200 orang join bersamaan, cap 10 → occupancy tepat 10, posisi unik
- rumus cap di Lua identik dengan rumus Go (`entity.CapByStock`)
- lepas M kursi → tepat M orang terdepan dipromosikan sekaligus
- sweeper melepas kursi kedaluwarsa & mempromosikan berikutnya
- 503 antrian penuh vs 410 sold out
- Access Token tervalidasi dengan claim milik booking-service
- SSE menerima `admitted` lewat Pub/Sub; consumer membaca dari broker Kafka asli

Test memakai Redis DB 13/14/15 (bukan DB dev). Override: `TEST_REDIS_ADDR`,
`TEST_DB_DSN`, `TEST_KAFKA_BROKER`.

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
cmd/main.go                        # startup + graceful shutdown (SSE ikut diputus rapi)
internal/
  config/                          # env → Config
  delivery/server.go               # composition root + proses latar (hub, sweeper, consumer)
  queue/
    domain/entity/                 # status, rumus CapByStock, claim Access Token
    domain/dto/                    # request/response + payload SSE
    port/inbound/                  # IQueueUsecase
    port/outbound/                 # IQueueStore, IStockSource, INotifier, IAccessTokenSigner
    application/usecase/           # logika antrian + test kritikal
    infrastructure/
      cache/                       # Redis: Lua script, store, Pub/Sub hub
      db/                          # warm-up stok (read-only)
      event/                       # Kafka consumer
      http/                        # handler join/status/SSE + rate limit
      token/                       # penanda tangan Access Token (JWT)
pkg/
  apinizer/ apperror/              # envelope response + error domain
  redis/ db/ kafka/ middleware/    # client & middleware
```
