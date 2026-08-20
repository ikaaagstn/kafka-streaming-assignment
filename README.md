# Kafka Streaming Assignment

Streaming for Data Pipeline Kafka.
producer dengan Faker untuk generate data transaksi, lalu consumer dengan Spark Structured Streaming. 
Consumer melakukan validasi event, menulis ke topic valid atau DLQ.

## Arsitektur singkat

```
producer.py --> [topic: transactions] --> consumer.py --+--> [topic: transactions_valid]
                                                          +--> [topic: transactions_dlq]
                                                          +--> console (per-event debug, tumbling window, running total)
```

## Dokumentasi

- [Contoh output console](docs/console%20output.png)
- [Contoh event transaction dalam JSON](docs/transaction%20json.png)
- [Contoh transaction valid](docs/transaction%20valid.png)
- [Contoh event DLQ dalam JSON](docs/dlq%20json.png)
- [Topic Kafka: transaction, valid, dan DLQ](docs/topic%20kafka%20%28transaction,%20valid,%20dlq%29.png)

## Install dependency Python

```bash
# aktifin venv dulu 
pip install -r producer/requirements.txt
pip install -r streaming/requirements.txt
```

## Cara jalanin

**1. Nyalainx Kafka + AKHQ:**
```bash
docker compose up -d
```

**2. Buka 2 terminal**, jalanin masing-masingx:

```bash
# terminal 1
python producer/producer.py

# terminal 2
python streaming/consumer.py
```

Producer bakal langsung keliatan ngirim event tiap 1-2 detik. Consumer butuh waktu ~30-40 detik di awal (JVM start + download dependency) sebelum mulai muncul output.

## Cara lihat isi Kafka

Buka **AKHQ** di browser: [http://localhost:8080](http://localhost:8080)

Di situ bisa lihat 3 topic:
- **`transactions`** — semua event mentah dari producer (termasuk yang sengaja dirusak buat testing)
- **`transactions_valid`** — event yang lolos validasi
- **`transactions_dlq`** — event yang gagal validasi (dead-letter queue), lengkap sama `error_reason`-nya

## Apa yang muncul di terminal consumer

Ada 3 macam output yang akan terlihat:

**1. Tabel per-event** — nunjukin tiap event yang lewat, valid atau engga, dan alasannya kalau invalid:
```
+-------+---------+--------------------+------+--------+-------------------+------------------+
|user_id|amount   |timestamp           |source|is_valid|error_reason       |routed_to         |
+-------+---------+--------------------+------+--------+-------------------+------------------+
|U47212 |4501001  |2026-08-20T04:29:59Z|web   |true    |NULL               |transactions_valid|
|U50553 |4021001  |2026-08-20T04:30:15Z|null  |false   |invalid_source     |transactions_dlq  |
+-------+---------+--------------------+------+--------+-------------------+------------------+
```

**2. Tumbling window** — total transaksi per menit (window gak overlap):
```
+-------------------+-------------------+---------------+
|window_start       |window_end         |total_transaksi|
+-------------------+-------------------+---------------+
|2026-08-20 11:30:00|2026-08-20 11:31:00|23             |
+-------------------+-------------------+---------------+
```

**3. Running total** — total transaksi valid kumulatif sejak consumer nyala:
```
+-------------+-----------------------+
|running_total|timestamp              |
+-------------+-----------------------+
|34           |2026-08-20 11:30:40.199|
+-------------+-----------------------+
```

## Aturan validasi (kenapa suatu event masuk DLQ)

Event dianggap **invalid** kalau salah satu dari ini kejadian:

| `error_reason` | Kondisi |
|---|---|
| `missing_mandatory_field` | `user_id`/`amount`/`timestamp` kosong atau null |
| `invalid_type` | `amount` gak bisa di-convert ke angka, atau `timestamp` gak valid |
| `amount_out_of_range` | `amount` di luar rentang 1 - 10.000.000 |
| `invalid_source` | `source` bukan `mobile`/`web`/`pos` |
| `late_event` | `event_time`-nya udah lebih dari 3 menit dari waktu diproses |