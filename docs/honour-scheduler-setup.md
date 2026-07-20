# Honour Scheduler Setup

This feature now supports a production-safe daily trigger for honours.

## Why this is needed

The old approach used an in-process cron inside the Cloud Run API container. That is not reliable for midnight jobs because Cloud Run can scale to zero overnight, which means there may be no running process at `12:00 AM`.

The new approach keeps the honour logic in the API, but lets **Cloud Scheduler** trigger it every day.

## What changed in code

- Honour day boundaries now use `APP_TIMEZONE` and default to `America/Toronto`.
- The API now exposes `POST /api/internal/jobs/honour/run`.
- That route is protected by the `X-Honour-Job-Secret` header.

## Required production settings

Set these on the Cloud Run service:

- `APP_TIMEZONE=America/Toronto`
- `HONOUR_JOB_SECRET=<random secret>`

## Fastest setup

After deploying the backend changes, run:

```powershell
.\scripts\setup-honour-scheduler.ps1 -RunNow
```

That script will:

- resolve the current Cloud Run service URL
- generate a `HONOUR_JOB_SECRET` if you do not provide one
- update the Cloud Run env vars
- enable the `cloudscheduler.googleapis.com` API if needed
- create or update the `honour-daily` Cloud Scheduler job
- optionally run the job once immediately

If you want to provide your own secret:

```powershell
.\scripts\setup-honour-scheduler.ps1 -HonourJobSecret "replace-with-a-random-secret" -RunNow
```

## Manual setup

### 1. Update Cloud Run env vars

```bash
gcloud run services update nordikdriveapi \
  --project planar-ray-472112-e8 \
  --region us-west1 \
  --update-env-vars APP_TIMEZONE=America/Toronto,HONOUR_JOB_SECRET=replace-with-a-random-secret
```

### 2. Create the scheduler job

If this is the first time Cloud Scheduler is used in the project, enable the API first:

```bash
gcloud services enable cloudscheduler.googleapis.com \
  --project planar-ray-472112-e8
```

```bash
gcloud scheduler jobs create http honour-daily \
  --project planar-ray-472112-e8 \
  --location us-west1 \
  --schedule "0 0 * * *" \
  --time-zone "America/Toronto" \
  --uri "https://nordikdriveapi-724838782318.us-west1.run.app/api/internal/jobs/honour/run" \
  --http-method POST \
  --headers "X-Honour-Job-Secret=replace-with-a-random-secret,Content-Type=application/json" \
  --message-body "{}" \
  --attempt-deadline 300s \
  --max-retry-attempts 3 \
  --min-backoff 30s \
  --max-backoff 600s
```

If the job already exists, use `gcloud scheduler jobs update http honour-daily ...` with the same flags.

### 3. Run it once now

```bash
gcloud scheduler jobs run honour-daily \
  --project planar-ray-472112-e8 \
  --location us-west1
```

## How to verify

1. Run the scheduler job once.
2. Open the honour-enabled file in the UI.
3. Confirm the honour banner appears.
4. Optional: inspect Cloud Run logs for the request to `/api/internal/jobs/honour/run`.

## Notes

- Cloud Scheduler is a Google Cloud service, not part of the codebase.
- Cloud Scheduler cost is usually negligible for this case.
- If you later lock down Cloud Run ingress/auth more tightly, you can switch the scheduler to authenticated OIDC calls. The current implementation uses a shared secret header because the service is already publicly reachable on `run.app`.
