# 🌬️ Apache Airflow v3 — Docker Compose Setup

Repository ini berisi konfigurasi lengkap untuk menjalankan **Apache Airflow 3.x** menggunakan Docker Compose dengan arsitektur **CeleryExecutor**, cocok untuk kebutuhan development dan eksplorasi fitur-fitur terbaru Airflow 3.

---

## 📋 Daftar Isi

- [Arsitektur](#arsitektur)
- [Prasyarat](#prasyarat)
- [Struktur Direktori](#struktur-direktori)
- [Konfigurasi Environment](#konfigurasi-environment)
- [Cara Menjalankan](#cara-menjalankan)
- [Akses UI](#akses-ui)
- [Menghentikan Layanan](#menghentikan-layanan)
- [Catatan Penting](#catatan-penting)

---

## 🏗️ Arsitektur

```
                          ┌──────────────────┐
                          │   User / Browser  │
                          └────────┬─────────┘
                                   │ HTTP :8080
                    ┌──────────────▼──────────────┐
                    │         API Server           │
                    │    Web UI + REST API (:8080) │
                    └───────┬──────────┬───────────┘
                            │          │
               ┌────────────▼──┐   ┌───▼──────────────────┐
               │   Scheduler   │   │    DAG Processor       │
               │  Jadwalkan &  │   │  Parse & serialize     │
               │  trigger task │   │  DAG files ★ new v3   │
               └──────┬────────┘   └───────────┬───────────┘
                      │                         │
               ┌──────▼────────┐        ┌───────▼────────┐
               │  Redis :6379  │        │   Volume dags/  │
               │ Celery broker │        │  File Python    │
               └──┬────────────┘        │  DAG kamu       │
                  │                     └────────────────-┘
       ┌──────────┼──────────┐
       ▼          ▼          ▼                  ┌───────────┐
  ┌─────────┐┌─────────┐┌─────────┐            │ Triggerer │
  │Worker 1 ││Worker 2 ││Worker N ││            │  Deferred │
  │  Celery ││  Celery ││  Celery ││            │   tasks   │
  └────┬────┘└────┬────┘└────┬────┘            └─────┬─────┘
       │          │          │                        │
       └──────────┴──────────┴────────────────────────┘
                                    │
                    ┌───────────────▼──────────────────┐
                    │          PostgreSQL :5432          │
                    │  Metadata DB — DAG runs, task     │
                    │  instances, logs, koneksi, variabel│
                    └───────────────────────────────────┘
```

> **★ Perubahan utama Airflow v3:** DAG Processor kini berjalan sebagai **service terpisah** (tidak lagi menjadi bagian dari Scheduler seperti di Airflow 2). Pastikan service `airflow-dag-processor` selalu aktif agar DAG dapat terdeteksi.

### Komponen Docker Services

| Service | Fungsi |
|---|---|
| `postgres` | Database backend (PostgreSQL 16) |
| `redis` | Message broker untuk CeleryExecutor (Redis 7.2) |
| `airflow-apiserver` | API Server & Web UI Airflow (port 8080) |
| `airflow-scheduler` | Menjadwalkan dan memicu task |
| `airflow-dag-processor` | Memproses dan mem-parse file DAG (**baru di Airflow 3**) |
| `airflow-worker` | Celery worker yang mengeksekusi task |
| `airflow-triggerer` | Menangani deferred/async tasks |
| `airflow-init` | Inisialisasi database dan user admin |
| `flower` *(opsional)* | Monitoring Celery worker (port 5555) |

### Alur Eksekusi Task

1. User membuat / men-trigger DAG melalui **Web UI** atau **REST API**.
2. **DAG Processor** membaca file Python dari folder `dags/` dan mem-parse-nya ke database.
3. **Scheduler** membaca metadata dari PostgreSQL dan mengirim task ke antrian **Redis**.
4. **Celery Worker** mengambil task dari Redis dan mengeksekusinya.
5. Hasil dan log disimpan kembali ke **PostgreSQL**.

---

## ✅ Prasyarat

Pastikan sudah terinstal di mesin Anda:

- [Docker](https://docs.docker.com/get-docker/) versi 20.10+
- [Docker Compose](https://docs.docker.com/compose/install/) versi 2.x+
- Minimal **4 GB RAM** tersedia untuk Docker
- Minimal **2 CPU** tersedia untuk Docker
- Minimal **10 GB** ruang disk kosong

---

## 📁 Struktur Direktori

```
airflow_v3/
├── config/          # File konfigurasi Airflow (airflow.cfg)
├── dags/            # Letakkan file DAG Anda di sini
├── logs/            # Log eksekusi task (di-generate otomatis)
├── .env             # Environment variables (Fernet key, UID, dll)
├── .gitignore
├── docker-compose.yaml
└── README.md
```

---

## ⚙️ Konfigurasi Environment

File `.env` berisi variabel-variabel penting. Pastikan file ini sudah dikonfigurasi sebelum menjalankan stack.

| Variabel | Keterangan | Default |
|---|---|---|
| `AIRFLOW_IMAGE_NAME` | Docker image Airflow yang digunakan | `apache/airflow:3.2.2` |
| `AIRFLOW_UID` | UID user di dalam container | `50000` |
| `AIRFLOW_PROJ_DIR` | Path direktori project | `.` (current dir) |
| `FERNET_KEY` | Kunci enkripsi untuk koneksi & variabel | *wajib diisi* |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Secret JWT untuk autentikasi API | `airflow_jwt_secret` |
| `_AIRFLOW_WWW_USER_USERNAME` | Username admin UI | `airflow` |
| `_AIRFLOW_WWW_USER_PASSWORD` | Password admin UI | `airflow` |

**Generate Fernet Key:**

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Tambahkan hasilnya ke file `.env`:

```env
FERNET_KEY=<hasil_generate_di_atas>
```

**Set AIRFLOW_UID (Linux):**

```bash
echo -e "AIRFLOW_UID=$(id -u)" >> .env
```

---

## 🚀 Cara Menjalankan

### 1. Inisialisasi (jalankan sekali saja)

```bash
docker compose up airflow-init
```

Perintah ini akan melakukan migrasi database dan membuat user admin.

### 2. Jalankan semua services

```bash
docker compose up -d
```

### 3. Cek status services

```bash
docker compose ps
```

### 4. (Opsional) Jalankan dengan Flower monitoring

```bash
docker compose --profile flower up -d
```

### 5. Melihat log

```bash
# Semua services
docker compose logs -f

# Service tertentu
docker compose logs -f airflow-scheduler
```

---

## 🌐 Akses UI

Setelah semua service berjalan (tunggu sekitar 1-2 menit):

| Interface | URL | Kredensial Default |
|---|---|---|
| **Airflow Web UI** | http://localhost:8080 | `airflow` / `airflow` |
| **Flower (Celery Monitor)** | http://localhost:5555 | *(tanpa auth)* |

---

## 🛑 Menghentikan Layanan

```bash
# Hentikan semua container (data tetap tersimpan)
docker compose down

# Hentikan dan hapus semua data (termasuk database)
docker compose down --volumes --remove-orphans
```

---

## 📝 Catatan Penting

- Konfigurasi ini **hanya untuk development/lokal**, jangan digunakan di production.
- DAG baru cukup diletakkan di folder `dags/` — akan otomatis terdeteksi oleh `airflow-dag-processor`.
- Konfigurasi Airflow tambahan dapat diatur melalui file `config/airflow.cfg`.
- Untuk menambahkan Python package tambahan, gunakan variabel `_PIP_ADDITIONAL_REQUIREMENTS` di `.env` (hanya untuk pengujian cepat). Untuk production, sebaiknya buat custom Docker image.
- Airflow 3 memisahkan DAG Processor sebagai service tersendiri — pastikan service `airflow-dag-processor` berjalan agar DAG dapat terdeteksi.

---

## 📚 Referensi

- [Dokumentasi Resmi Apache Airflow](https://airflow.apache.org/docs/)
- [Panduan Docker Compose Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [Airflow 3 Migration Guide](https://airflow.apache.org/docs/apache-airflow/stable/migration-guide.html)