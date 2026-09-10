# Webhook dedup: увімкнути розподілений лок

## Що зламано

`_webhook_dedup` у `main.py` — двошаровий: in-memory dict (швидкий шлях,
той самий інстанс) + Firestore `create()` як атомарний мьютекс між
інстансами Cloud Run. Zendesk шле 5–15 вебхуків на тікет, тому другий шар
і є справжнім захистом.

**Шар 2 не працював із дня написання до 02.09.2026.** У проєкті
`powerful-vine-426615-r2` не було жодної Firestore-БД, тому на **кожному**
реквесті:

```
WARNING | [176081] Firestore dedup error, falling back to in-memory:
404 The database (default) does not exist for project powerful-vine-426615-r2
```

Далі код fail-open'ився на per-instance dict, який між інстансами не
дедуплікує взагалі. 10.08.2026: 3,736 вебхуків на 1,201 унікальний тікет.

Не помітили, бо per-request WARNING, який не припиняється, читається як шум,
і бо тег `bot_handled` маскує більшу частину шкоди.

**Маскує більшу частину, не всю.** За серпень 2026:

| | |
|---|---|
| тікетів оброблено двічі й більше | 93 |
| **клієнтів отримали 2+ листи від бота** | **5** |
| гірший тікет | **#181910 — 4 листи** |

Інтервали між дублями: **0, 0, 2, 3, 5, 5 і 63 секунди**. Це гонка інстансів,
яка обганяє запис тега — `add_tag` це окремий POST, а читання тегів у Zendesk
eventually consistent.

## Фікс — одна команда (❗ вручну)

Локація Firestore-БД **не змінюється після створення**, тому цю команду я не
виконував. `europe-west1` = там, де живе Cloud Run.

```bash
gcloud firestore databases create --database='(default)' --location=europe-west1 --type=firestore-native --project=powerful-vine-426615-r2
```

Далі TTL-політика, щоб документи локу самі прибирались (в коментарі коду вона
описана з самого початку, але її ніколи не виконували):

```bash
gcloud firestore fields ttls update expire_at --collection-group=webhook_dedup --enable-ttl --project=powerful-vine-426615-r2
```

Перевірити, що шар 2 ожив:

```bash
gcloud logging read 'resource.labels.service_name="automatization-customer-support" AND textPayload:"claimed in Firestore"' --project=powerful-vine-426615-r2 --limit=5 --format='value(timestamp,textPayload)'
```

IAM правити не треба: сервіс ходить під
`991753937441-compute@developer.gserviceaccount.com`, у якого є `roles/owner`
і `roles/editor`. (Окремо: owner на дефолтному compute-SA — надлишкові права,
варто звузити, але це не про дедуп.)

## Що змінено в коді

Fail-open лишився — аварія дедупу не має зупиняти відповіді клієнтам. Але
вона більше не безкоштовна: `_report_firestore_degraded` розділяє причини.

| причина | реакція |
|---|---|
| БД немає / 403 / API вимкнено — **постійна** | `log.error` + Slack-алерт, **один раз на інстанс** |
| мережевий збій — **транзієнтна** | `log.warning`, не частіше 1/5 хв |
| `ALREADY_EXISTS` | тихо: це дедуп працює як задумано |

Тобто те, що місяцями тонуло в 3,700 однакових WARNING-ів на добу, тепер
дає один ERROR і один Slack. Тести — `tests/test_webhook_dedup_health.py`.
