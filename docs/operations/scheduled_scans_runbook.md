# Scheduled Source Scans Runbook

This runbook configures reliable scheduled scans for `job_sources` using the recommender API.

## Preconditions

- recommender service reachable (default `http://localhost:8001`)
- at least one enabled source exists in `job_sources`
- `RECOMMENDER_API_KEY` or scoped token with `scan` scope is available for scheduler execution

## Manual verification

Run a scheduled-mode scan once before automating:

```bash
./scripts/scan_sources.sh http://localhost:8001 true scheduled
```

Optional history check:

```bash
curl -sS \
  -H "x-api-key: ${RECOMMENDER_API_KEY}" \
  "http://localhost:8001/job-sources/scan-history?limit=20&trigger=scheduled"
```

## Cron example

Run every 15 minutes:

```cron
*/15 * * * * cd /path/to/battleship && RECOMMENDER_API_KEY='replace-me' ./scripts/scan_sources.sh http://localhost:8001 true scheduled >> /var/log/battleship-scan.log 2>&1
```

Recommended:
- keep scheduler token scoped to `scan`
- log to a dedicated file and rotate with `logrotate`
- alert on repeated failures or stalled ingestion

## systemd timer example

Service unit (`/etc/systemd/system/battleship-scan.service`):

```ini
[Unit]
Description=OperationBattleship scheduled source scan
After=network.target

[Service]
Type=oneshot
WorkingDirectory=/path/to/battleship
Environment=RECOMMENDER_API_KEY=replace-me
ExecStart=/bin/bash -lc './scripts/scan_sources.sh http://localhost:8001 true scheduled'
```

Timer unit (`/etc/systemd/system/battleship-scan.timer`):

```ini
[Unit]
Description=Run OperationBattleship source scan every 15 minutes

[Timer]
OnCalendar=*:0/15
Persistent=true

[Install]
WantedBy=timers.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now battleship-scan.timer
sudo systemctl list-timers battleship-scan.timer
```

## Reliability checks

- `GET /job-sources/scan-history?trigger=scheduled&limit=50` shows regular entries
- source table in UI shows advancing `last_scan_at`
- no long-running growth in `consecutive_failures` unless upstream sources are down
