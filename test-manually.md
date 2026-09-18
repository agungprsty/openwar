# Testing Guide — OpenWar (Manual & Otomatis)

Dokumen ini berisi panduan lengkap untuk **pengujian otomatis** (unit test, build, k6 load test) dan **pengujian manual** (curl, SQL, Redis CLI, payment timeout v0.2) pada **OpenWar**.

---

## 1. Pengujian Otomatis (Automated Tests)

### A. Unit Test & Code Quality
Jalankan linting dan unit test Go di seluruh package:

```bash
# Verify sintaks & tipe data
make test

# Atau jalankan go test langsung dengan output verbose
go test -v ./...
```

### B. Automated Load Testing (k6)
Gunakan Grafana k6 via Docker Compose untuk mensimulasikan beban ribuan pengguna bersamaan dengan autentikasi JWT dinamis:

```bash
docker compose -f deploy/docker-compose.yml --profile tools run --rm k6 run /scripts/flashsale.js
```

**Ekspektasi Hasil:**
- `join accepted` & `purchase accepted` berhasil dipenuhi.
- `http_req_failed` < 5%.

---

## 2. Preparasi Environment Pengujian Manual

### Jalankan Seluruh Container Docker
```bash
make up
# Atau: docker compose -f deploy/docker-compose.yml up --build -d
```

Service yang berjalan:
- `gateway` (:8080)
- `backend-demo` (:9001 internal)
- `worker` (admission + durable order worker + payment timeout + reconciler + DLQ)
- `redis` (:6379)
- `nats` (:4222 / :8222)
- `postgres` (:5432)

### Seed Data Katalog & Partitioned Inventory
Mengisi data pengguna demo di PostgreSQL dan pre-warm 10 tiket ke 4 shard Redis:

```bash
docker compose -f deploy/docker-compose.yml exec gateway /app/openwar seed-events --event flash-sale-001 --qty 10 --shards 4
```

---

## 3. Pengujian Manual Alur Utama (v0.1 & v0.2)

### Step 3.1: Generate JWT Token Valid untuk `user-1`

```bash
HEADER=$(echo -n '{"alg":"HS256","typ":"JWT"}' | base64 | tr -d '=' | tr '/+' '_-')
PAYLOAD=$(echo -n '{"uid":"user-1","exp":1999999999}' | base64 | tr -d '=' | tr '/+' '_-')
SIG=$(echo -n "${HEADER}.${PAYLOAD}" | openssl dgst -sha256 -hmac "docker-demo-secret" -binary | base64 | tr -d '=' | tr '/+' '_-')
TOKEN="${HEADER}.${PAYLOAD}.${SIG}"
```

### Step 3.2: Join Waiting Room Queue
```bash
curl -i -X POST http://localhost:8080/event/flash-sale-001/queue \
  -H "Authorization: Bearer $TOKEN"
```
* **Ekspektasi:** `HTTP 202 Accepted` dan header `Set-Cookie: openwar_session=<SESSION_ID>`.  
*Ambil nilai `<SESSION_ID>` dari header response.*

### Step 3.3: Heartbeat Liveness & Admission
```bash
curl -i -X POST http://localhost:8080/event/flash-sale-001/queue/heartbeat \
  -H "Cookie: openwar_session=<SESSION_ID>"
```
* **Ekspektasi:** `HTTP 200 OK` dengan `"admitted": true`.

### Step 3.4: Purchase / Checkout
```bash
IDEM_KEY="11111111-2222-4333-8444-555555555555"

curl -i -X POST http://localhost:8080/event/flash-sale-001/purchase \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: $IDEM_KEY" \
  -H "Content-Type: application/json" \
  -H "Cookie: openwar_session=<SESSION_ID>" \
  -d '{"sku":"flash-sale-001:ticket","qty":1}'
```
* **Ekspektasi:** `HTTP 201 Created` dengan JSON `{"orderId":"ord-...", "status":"PENDING_PAYMENT"}`.

---

## 4. Pengujian Fitur v0.2 (Payment Timeout, CAS Cancel & Restock)

### Test 4.1: Otomatis Timeout Payment (15 Menit) & Stock Restock
1. Setelah membuat order di **Step 3.4**, periksa data di PostgreSQL:
   ```bash
   docker compose exec postgres psql -U openwar -d openwar -c "SELECT order_id, user_id, status, expires_at FROM openwar.orders;"
   ```
   * **Status Awal:** `PENDING_PAYMENT`.
2. Tunggu hingga waktu `expires_at` terlewati (atau dalam pengujian unit/integrasi waktu expired dipercepat):
   - NATS JetStream Message Scheduler akan menembakkan event `orders.timeout`.
   - Worker memproses timeout, mengupdate status SQL via atomic CAS ke `CANCELLED_TIMEOUT`, dan mengembalikan stok ke shard Redis.
3. Periksa kembali PostgreSQL setelah timeout:
   ```bash
   docker compose exec postgres psql -U openwar -d openwar -c "SELECT order_id, status, cancelled_at FROM openwar.orders;"
   ```
   * **Status Akhir:** `CANCELLED_TIMEOUT` dan `cancelled_at` terisi timestamptz.

### Test 4.2: Inspeksi Redis State & Sold-Out Latch
Gunakan Redis CLI untuk melihat sisa stok per-shard dan memverifikasi bahwa `soldout:<sku>` latch terhapus saat restock:

```bash
docker compose exec redis redis-cli MGET inventory:flash-sale-001:ticket:shard:0 inventory:flash-sale-001:ticket:shard:1 inventory:flash-sale-001:ticket:shard:2 inventory:flash-sale-001:ticket:shard:3
docker compose exec redis redis-cli EXISTS soldout:flash-sale-001:ticket
```

---

## 5. Pengujian Single-Use Admission & Idempotency Edge Cases

### Test 5.1: Single-Use Admission Token Contract
Coba panggil endpoint purchase sekali lagi dengan cookie session yang sama:
```bash
curl -i -X POST http://localhost:8080/event/flash-sale-001/purchase \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: 22222222-3333-4444-8555-666666666666" \
  -H "Content-Type: application/json" \
  -H "Cookie: openwar_session=<SESSION_ID>" \
  -d '{"sku":"flash-sale-001:ticket","qty":1}'
```
* **Ekspektasi:** `HTTP 403 Forbidden` (`{"error": "not admitted to checkout"}`).

### Test 5.2: Idempotency Replay
Kirim request purchase menggunakan `Idempotency-Key` yang sama (`11111111-2222-4333-8444-555555555555`):
* **Ekspektasi:** Gateway mengembalikan response cache sebelumnya dengan header `X-Idempotent-Replay: true`.
