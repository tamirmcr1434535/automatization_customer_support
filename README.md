Zendesk Cancellation Bot
Автоматично обробляє тікети скасування підписки через Claude AI.
Флоу: Zendesk → webhook → Cloud Function → Stripe cancel → Claude reply → Zendesk solved

Структура репозиторію
├── main.py              ← Cloud Function entry point
├── classifier.py        ← Claude Haiku → intent + language
├── zendesk_client.py    ← Zendesk API (read / reply / tag / solve)
├── stripe_client.py     ← Stripe cancel subscription
├── reply_generator.py   ← Claude Sonnet → EN/JP/KR reply
├── bq_logger.py         ← BigQuery logging
├── requirements.txt
└── .github/
    └── workflows/
        └── deploy.yml   ← Auto-deploy on push to main

Один раз налаштувати (10-15 хвилин)
1. Зібрати ключі
КлючДе взятиANTHROPIC_API_KEYconsole.anthropic.com → API KeysZENDESK_API_TOKENZendesk Admin → Apps & Integrations → Zendesk API → Enable → Add TokenSTRIPE_SECRET_KEYStripe Dashboard → Developers → API Keys → sk_test_... (для тестів)

2. Google Cloud — Service Account для GitHub
bashPROJECT=powerful-vine-426615-r2

# Створити service account
gcloud iam service-accounts create github-deployer \
  --project=$PROJECT \
  --display-name="GitHub Actions Deployer"

# Дати права
gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:github-deployer@$PROJECT.iam.gserviceaccount.com" \
  --role="roles/cloudfunctions.developer"

gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:github-deployer@$PROJECT.iam.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"

gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:github-deployer@$PROJECT.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

gcloud projects add-iam-policy-binding $PROJECT \
  --member="serviceAccount:github-deployer@$PROJECT.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

# Скачати ключ
gcloud iam service-accounts keys create key.json \
  --iam-account=github-deployer@$PROJECT.iam.gserviceaccount.com

3. Зберегти ключі в Secret Manager (GCP)
bash# Anthropic key
echo -n "sk-ant-YOUR_KEY" | gcloud secrets create anthropic-key \
  --data-file=- --project=$PROJECT

# Zendesk token
echo -n "YOUR_ZENDESK_TOKEN" | gcloud secrets create zendesk-token \
  --data-file=- --project=$PROJECT

# Stripe key
echo -n "sk_test_YOUR_KEY" | gcloud secrets create stripe-key \
  --data-file=- --project=$PROJECT

4. Налаштувати GitHub репозиторій
Secrets (GitHub → Settings → Secrets → Actions):
NameValueGCP_SA_KEYВміст key.json (весь JSON)
Variables (GitHub → Settings → Variables → Actions):
NameValueGCP_PROJECTpowerful-vine-426615-r2DRY_RUNtrueTEST_MODEtrueZENDESK_SUBDOMAINтвій субдомен (без .zendesk.com)ZENDESK_EMAILтвій email в Zendesk

5. Перший деплой
bashgit add .
git commit -m "Initial deploy"
git push origin main
GitHub Actions задеплоїть функцію. URL буде у форматі:
https://europe-west1-powerful-vine-426615-r2.cloudfunctions.net/cancellation-bot
Перевірити: відкрий URL у браузері — побачиш JSON зі статусом.

6. Налаштувати Zendesk Webhook
Zendesk Admin → Apps & Integrations → Webhooks → Create webhook:

Name: Cancellation Bot
URL: https://europe-west1-powerful-vine-426615-r2.cloudfunctions.net/cancellation-bot
Method: POST
Request format: JSON
Authentication: None

Zendesk Admin → Objects & Rules → Triggers → Create trigger:

Name: Bot — New ticket
Conditions: Ticket is Created
Actions: Notify webhook → Cancellation Bot
JSON body:

json{
  "ticket_id": "{{ticket.id}}"
}

Тест-режим (безпечний тест на живих тікетах)
Крок 1 — Test mode увімкнений (за замовчуванням)
DRY_RUN=true + TEST_MODE=true
Бот тільки читає тікети, нічого реально не робить.
Крок 2 — Протестувати на реальному тікеті

Відкрий будь-який закритий тікет скасування в Zendesk
Постав тег automation_test вручну
Перевідкрий тікет (поміняй статус на New)
Бот отримає webhook → прочитає тікет → залогує в BigQuery → але нічого не зробить

Крок 3 — Реальні дії, але тільки на тест-тікетах
DRY_RUN=false
TEST_MODE=true   ← тільки тікети з тегом automation_test
У GitHub Variables змінити DRY_RUN на false → push → новий деплой.
Крок 4 — Повний прод
DRY_RUN=false
TEST_MODE=false  ← всі нові тікети

Моніторинг в BigQuery
sql-- Статистика за сьогодні
SELECT status, intent, language, COUNT(*) as cnt
FROM `powerful-vine-426615-r2.zendesk_bot.cancellation_logs`
WHERE DATE(logged_at) = CURRENT_DATE()
GROUP BY 1, 2, 3
ORDER BY cnt DESC;

-- Всі оброблені ботом тікети
SELECT *
FROM `powerful-vine-426615-r2.zendesk_bot.cancellation_logs`
WHERE status = 'success'
ORDER BY logged_at DESC
LIMIT 50;
