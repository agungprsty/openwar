# Manual Testing Guide — OpenWar

Dokumen ini berisi panduan langkah-demi-langkah pengujian manual untuk **OpenWar** (Token Bucket, Virtual Waiting Room, Sharded Inventory, dan Idempotency Gate).

---

## Preparasi Environment

### 1. Jalankan Service dengan Docker Compose

```bash
make up
# Atau: docker compose -f deploy/docker-compose.yml up --build -d
```

Service yang berjalan:
- `gateway` (:8080)
- `backend-demo` (:9001 internal)
- `worker` (admission + durable order worker)
- `redis` (:6379)
- `nats` (:4222 / :8222)
- `postgres` (:5432)

### 2. Seed Catalog & Partitioned Inventory

Jalankan seeder untuk mengisi database PostgreSQL dan mem-prewarm 10 tiket ke 4 shard Redis:

```bash
docker compose -f deploy/docker-compose.yml exec gateway /app/openwar seed-events --event flash-sale-001 --qty 10 --shards 4
```

---

## Skenario 1: Testing Alur Utama (Queue → Heartbeat → Purchase)

### Step 1.1: Generate JWT Token Valid untuk `user-1`

Buat token JWT bertanda tangan HMAC-SHA256 dengan secret `docker-demo-secret`:

```bash
HEADER=$(echo -n '{"alg":"HS256","typ":"JWT"}' | base64 | tr -d '=' | tr '/+' '_-')
PAYLOAD=$(echo -n '{"uid":"user-1","exp":1999999999}' | base64 | tr -d '=' | tr '/+' '_-')
SIG=$(echo -n "${HEADER}.${PAYLOAD}" | openssl dgst -sha256 -hmac "docker-demo-secret" -binary | base64 | tr -d '=' | tr '/+' '_-')
TOKEN="${HEADER}.${PAYLOAD}.${SIG}"
```

### Step 1.2: Join Waiting Room Queue

```bash
curl -i -X POST http://localhost:8080/event/flash-sale-001/queue \
  -H "Authorization: Bearer $TOKEN"
```

**Ekspektasi:** `HTTP 202 Accepted` dan header `Set-Cookie: openwar_session=<SESSION_ID>`.  
*Ambil nilai `<SESSION_ID>` dari cookie.*

### Step 1.3: Kirim Heartbeat Liveness

```bash
curl -i -X POST http://localhost:8080/event/flash-sale-001/queue/heartbeat \
  -H "Cookie: openwar_session=<SESSION_ID>"
```

**Ekspektasi:** `HTTP 200 OK` dengan JSON `{"admitted": true, "position": 0, ...}`.

### Step 1.4: Lakukan Purchase Pertama

```bash
IDEM_KEY="11111111-2222-4333-8444-555555555555"

curl -i -X POST http://localhost:8080/event/flash-sale-001/purchase \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: $IDEM_KEY" \
  -H "Content-Type: application/json" \
  -H "Cookie: openwar_session=<SESSION_ID>" \
  -d '{"sku":"flash-sale-001:ticket","qty":1}'
```

**Ekspektasi:** `HTTP 201 Created` dengan JSON `{"orderId":"ord-...", "status":"PENDING_PAYMENT"}`.

---

## Skenario 2: Testing Verification & Edge Cases

### Test 2.1: Single-Use Admission Token Contract

Jalankan kembali perintah Purchase dengan cookie session yang sama (`openwar_session=<SESSION_ID>`):

```bash
curl -i -X POST http://localhost:8080/event/flash-sale-001/purchase \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: 22222222-3333-4444-8555-666666666666" \
  -H "Content-Type: application/json" \
  -H "Cookie: openwar_session=<SESSION_ID>" \
  -d '{"sku":"flash-sale-001:ticket","qty":1}'
```

**Ekspektasi:** `HTTP 403 Forbidden` (`{"error": "not admitted to checkout"}`).

### Test 2.2: Idempotency Replay Guard

Kirim ulang request purchase menggunakan `Idempotency-Key` yang sama (`11111111-2222-4333-8444-555555555555`):

**Ekspektasi:**
- Jika request sebelumnya sukses (`201`), gateway mengembalikan response ter-cache dengan header `X-Idempotent-Replay: true`.
- Jika request sebelumnya gagal (`5xx`), gateway mengizinkan permohonan di-retry dari awal tanpa tertahan `409 Conflict`.

### Test 2.3: Restock & Sold-Out Latch Clearing

1. Pesan sisa tiket hingga habis. Request ke-11 akan mengembalikan `HTTP 410 Gone / sold out`.
2. Ketika terjadi pembatalan / kompensasi stok, key `soldout:<sku>` akan terhapus otomatis di Redis sehingga tiket yang di-refund bisa dipesan kembali.

---

## Skenario 3: Automated Load Testing (k6)

Jalankan pengujian beban dengan script k6 yang mensimulasikan pengguna dengan token JWT bertanda tangan valid:

```bash
docker compose -f deploy/docker-compose.yml --profile tools run --rm k6 run /scripts/flashsale.js
```

**Ekspektasi:** Seluruh request dari k6 terautentikasi dan tingkat error HTTP `< 5%`.
