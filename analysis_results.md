# Kritik Tajam & Analisis Production-Readiness — OpenWar

Dokumen ini berisi kritik objektif terhadap kesiapan proyek OpenWar untuk production, termasuk analisis tajam terhadap hasil load test k6.

---

## Bagian 1: Kritik Hasil Load Test k6

### Angka yang Terlihat Bagus — Tapi Menipu

| Metrik | Nilai | Komentar |
|---|---|---|
| `http_req_failed` | **0%** | Terlihat sempurna, tapi ini bukan karena sistem kebal error — ini karena **skenario test terlalu dangkal** |
| `checks` passes | **329.392** | Yang dicek hanya `"join accepted"` — **satu check saja** |
| purchase checks | **0** | Tidak ada satu pun purchase yang terjadi atau diverifikasi |
| `http_reqs` | **658.784** | Artinya **2 request per iterasi** (join + heartbeat). Purchase **tidak pernah tercapai** |

> [!CAUTION]
> ### Load Test Ini Belum Menguji Apa-Apa yang Penting
> Dari 329.392 iterasi, **tidak ada satu pun purchase yang ter-execute**. Artinya:
> - Token Bucket **belum pernah diuji** di bawah beban
> - Sharded Inventory DECR **belum pernah diuji** di bawah beban
> - Idempotency middleware **belum pernah diuji** di bawah beban
> - NATS publish + compensation path **belum pernah diuji** di bawah beban
>
> Yang diuji hanyalah: "bisakah Gateway menerima POST /queue dan POST /heartbeat 16K kali per detik?" — itu hanya menguji Redis ZADD + ZRANK + SET, bukan sistem secara keseluruhan.

### Mengapa Purchase Tidak Pernah Terjadi?

Alur k6 script saat ini:
1. Join queue → ✅ (329K kali)
2. Heartbeat sekali → cek `admitted` → **hampir selalu `false`**
3. Jika `admitted == true` → purchase — **tidak pernah tercapai**

**Penyebab:** Admission worker di container `worker` mengadmit 100 user/detik. Tapi k6 hanya mengirim **1 heartbeat** lalu langsung cek `admitted`. Karena antara join dan heartbeat pertama hanya sepersekian detik, admission worker belum sempat menjalankan batch admit. Skenario test ini pada dasarnya **menguji antrean masuk saja, bukan alur pembelian**.

### Apa yang Seharusnya Diuji

Skenario load test yang bermakna untuk production harus mencakup:

| Skenario | Yang Diuji | Status Saat Ini |
|---|---|---|
| Join → Poll heartbeat berulang sampai admitted → Purchase | End-to-end flow | ❌ Tidak ada |
| 1000 user bersaing untuk 100 tiket → verifikasi tidak oversell | Inventory integrity | ❌ Tidak ada |
| Duplicate purchase dengan idempotency key yang sama | Idempotency guard | ❌ Tidak ada |
| Purchase saat Redis down (fail-open vs fail-closed) | Fault tolerance | ❌ Tidak ada |
| Purchase setelah tiket habis → verify 410 Gone | Sold-out path | ❌ Tidak ada |

---

## Bagian 2: Kritik Kesiapan Production — Kode & Arsitektur

### 🔴 CRITICAL — Harus Diperbaiki Sebelum Production

#### 1. Test Coverage Sangat Rendah (13.8%)
- Total kode: **3.059 baris Go**
- Total test: **421 baris** (termasuk integration test yang butuh Redis/Postgres)
- Unit test murni (tanpa infra): **hanya 23 baris** ([timeout_test.go](file:///home/farghani/Project/openwar/internal/order/timeout_test.go))
- **Zero unit test** untuk: middleware chain, rate limiter, idempotency, proxy, router, config, queue, waiting room
- Standar industri minimum: **≥ 60% line coverage** untuk critical path

#### 2. Graceful Shutdown Tidak Lengkap
- [cmd/worker/main.go](file:///home/farghani/Project/openwar/cmd/worker/main.go#L164): Saat `ctx.Done()`, goroutine NATS subscriber **tidak di-drain**. Message yang sedang diproses bisa terpotong.
- Tidak ada `sub.Drain()` atau timeout untuk in-flight messages.
- Produksi: Order yang sedang di-handle saat shutdown bisa hilang antara DECR dan publish.

#### 3. Secret Management Tidak Aman
- [config.go](file:///home/farghani/Project/openwar/internal/config/config.go#L59): `JWT_SECRET` default `"dev-secret-change-me"` — jika env var tidak di-set, **semua token bisa di-forge**.
- [docker-compose.yml](file:///home/farghani/Project/openwar/deploy/docker-compose.yml#L80): `JWT_SECRET: "docker-demo-secret"` di-hardcode plain text.
- Produksi: Harus menggunakan secret manager (Vault, AWS Secrets Manager, K8s Secrets) dan **refuse to start** jika JWT_SECRET tidak di-set secara eksplisit.

#### 4. Database Credentials Hardcoded
- [docker-compose.yml](file:///home/farghani/Project/openwar/deploy/docker-compose.yml#L8): `DATABASE_URL: "postgres://openwar:openwar@..."` — password hardcoded.
- Tidak ada enkripsi koneksi (`sslmode=disable` implicit).

#### 5. Redis Tanpa Password & Tanpa Persistence Config
- [docker-compose.yml](file:///home/farghani/Project/openwar/deploy/docker-compose.yml#L29-L38): Redis tanpa `requirepass`, tanpa volume persistence, tanpa `appendonly yes`.
- Produksi: Restart Redis = **semua inventory counter, queue, heartbeat, dan reservation state hilang**. Ini adalah data loss yang menyebabkan oversell atau phantom tickets.

---

### 🟡 WARNING — Risiko Tinggi di Production

#### 6. Single Redis Instance = Single Point of Failure
- Seluruh state kritis (inventory, queue, heartbeat, token bucket, reservation, idempotency) bergantung pada **satu Redis node**.
- Produksi: Redis Sentinel atau Redis Cluster dengan failover otomatis.

#### 7. Tidak Ada Circuit Breaker ke Dependency Eksternal
- Jika Redis/NATS/Postgres lambat tapi tidak mati, Gateway akan menumpuk goroutine dan akhirnya OOM.
- Tidak ada timeout per-request ke Redis selain default Go context.
- Tidak ada circuit breaker pattern (half-open, backoff).

#### 8. CORS Wildcard di Production
- [cmd/gateway/main.go](file:///home/farghani/Project/openwar/cmd/gateway/main.go#L67): `AllowedOrigins: []string{"*"}` — membuka CSRF attack vector.

#### 9. Prometheus Metrics Cardinality Explosion
- [middleware/logger.go](file:///home/farghani/Project/openwar/internal/middleware/logger.go): Label `route` menggunakan `r.URL.Path` — setiap event ID unik = label baru. Dengan 1000 event, Prometheus akan memakan memori signifikan.

#### 10. Tidak Ada Rate Limiting pada Endpoint Queue
- Endpoint `POST /event/{id}/queue` dan `POST /event/{id}/queue/heartbeat` **tidak melewati token bucket** — hanya endpoint purchase yang di-rate-limit.
- Produksi: Bot bisa spam join queue jutaan kali dan membanjiri Redis sorted set.

#### 11. Tidak Ada Request Body Size Limit pada Queue Endpoints
- [handlers.go](file:///home/farghani/Project/openwar/internal/router/handlers.go#L27-L46): `join()` handler tidak membatasi body size. Hanya endpoint purchase yang punya `MaxBytesReader`.

#### 12. Logging Leaks Sensitive Data
- [cmd/worker/main.go](file:///home/farghani/Project/openwar/cmd/worker/main.go#L49): `logger.Info("postgres connected", "url", cfg.DatabaseURL)` — **mencetak DSN lengkap termasuk password** ke stdout/log.

---

### 🔵 INFO — Improvement untuk Production-Grade

#### 13. Tidak Ada Health Check yang Meaningful
- [handlers.go](file:///home/farghani/Project/openwar/internal/router/handlers.go) / [router.go](file:///home/farghani/Project/openwar/internal/router/router.go#L52-L55): `/healthz` hanya return `"ok"` — tidak memeriksa konektivitas Redis, NATS, atau Postgres.
- Produksi: Kubernetes liveness/readiness probe membutuhkan deep health check.

#### 14. Tidak Ada Structured Error Types
- Error handling menggunakan string comparison (`err.Error() == "stream name already in use"` di [queue.go](file:///home/farghani/Project/openwar/internal/queue/queue.go#L49-L50)) — fragile dan bisa break saat library update.

#### 15. Tidak Ada Migration Versioning
- Schema SQL menggunakan `CREATE TABLE IF NOT EXISTS` — tidak ada mekanisme migration versioning (goose, migrate, atlas). Perubahan schema di masa depan akan sangat sulit di-manage.

#### 16. Tidak Ada Retry dengan Exponential Backoff
- [order/service.go](file:///home/farghani/Project/openwar/internal/order/service.go#L103-L108): NATS publish gagal → langsung compensate. Tidak ada retry sebelum menyerah.

#### 17. Tidak Ada Context Timeout pada Redis Operations
- Seluruh operasi Redis menggunakan `context.Background()` atau parent context tanpa deadline eksplisit.

---

## Bagian 3: Pertanyaan Kritis untuk Refleksi

1. **Bagaimana jika Redis restart di tengah flash sale?** Seluruh inventory counter, queue position, heartbeat, dan reservation state hilang. Apakah ada mekanisme rebuild dari PostgreSQL?

2. **Bagaimana jika 2 worker instance berjalan bersamaan?** Apakah admission worker bisa double-admit? Apakah reconciler bisa double-compensate? (Leader lock ada, tapi dengan TTL — race window tetap ada.)

3. **Bagaimana jika network partition terjadi antara Gateway dan Redis?** Gateway memanggil `HasAdmission` (sekarang `ConsumeAdmission`) yang butuh Redis. Jika Redis tidak tercapai, **semua purchase diblokir** — bahkan dari user yang sudah sah.

4. **Apakah k6 test membuktikan sistem tidak oversell?** **Tidak.** Test tidak pernah sampai ke purchase path. Tidak ada validasi bahwa `sum(sold) <= sum(seeded_inventory)`.

5. **Apa yang terjadi jika NATS dan reconciler keduanya gagal?** Order tetap `PENDING_PAYMENT` selamanya, reservation TTL expired di Redis, tapi DB tidak pernah di-cancel. Stock locked forever.

---

## Bagian 4: Rekomendasi Prioritas Perbaikan

| Prioritas | Item | Effort |
|---|---|---|
| **P0** | Tulis ulang k6 scenario yang benar-benar menguji end-to-end purchase flow | 1 hari |
| **P0** | Tambahkan unit test untuk middleware, rate limiter, inventory router, idempotency | 2-3 hari |
| **P0** | Jangan start jika `JWT_SECRET` masih default; redact DSN dari log | 2 jam |
| **P0** | Redis persistence (`appendonly yes`) + password | 1 jam |
| **P1** | Deep health check (`/healthz` cek Redis + NATS + PG) | 2 jam |
| **P1** | Graceful shutdown: drain NATS subscribers, wait in-flight | 4 jam |
| **P1** | Rate limit queue join/heartbeat endpoint (per-IP) | 4 jam |
| **P1** | CORS whitelist, bukan wildcard | 30 menit |
| **P2** | Circuit breaker untuk Redis/NATS calls | 1 hari |
| **P2** | Migration tooling (goose/migrate) | 4 jam |
| **P2** | Redis Sentinel/Cluster setup | 1 hari |

---

> [!IMPORTANT]
> **Kesimpulan Akhir:** OpenWar memiliki **arsitektur yang sangat solid secara konseptual** — token bucket, sharded inventory, reservation state machine, dan layered defense semuanya dirancang dengan benar. Namun, **implementasi saat ini adalah prototype yang bagus, bukan production-ready system**. Gap terbesar ada di: (1) test coverage yang hampir tidak ada, (2) k6 scenario yang tidak menguji critical path, dan (3) operational hardening (secret management, persistence, graceful shutdown, health checks). Memperbaiki item P0 di atas akan membawa proyek ini dari "impressive demo" ke "deployable system".
