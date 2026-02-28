# Firefox Homelab Deployment Plan

## Target

- host: `firefox`
- role: first persistent Battleship deployment target
- scope: internal homelab deployment before any public cloud rollout

## Proposed runtime layout

- reverse proxy:
  - `Caddy` preferred
- application services:
  - `frontend`
  - `recommender`
- optional later service:
  - `emailer`
- persistence:
  - SQLite file on host storage
- scheduling:
  - `systemd` timer invoking scheduled source scans

## Host prerequisites

- Docker Engine or Podman
- Compose support
- persistent application directory, for example:
  - `/srv/battleship/compose`
  - `/srv/battleship/data`
  - `/srv/battleship/backups`
- outbound access for source ingestion targets
- reverse-proxy port exposure on LAN

## Initial deployment shape

Recommended first shape:

- expose frontend on LAN
- keep recommender private behind reverse proxy or LAN firewall rules
- use SQLite file under `/srv/battleship/data/recommender.sqlite3`
- enable scheduled scans every 15 minutes
- back up data nightly to another server

## Environment expectations

Minimum environment variables:

- `RECOMMENDER_API_KEY`
- `RECOMMENDER_BASE_URL`
- `RECOMMENDER_DB_PATH`
- optional `RECOMMENDER_API_TOKENS_JSON` only for bootstrap

Recommended operational model:

- use DB-managed scoped tokens for normal workflows
- keep bootstrap env key only for emergency admin access

## Deployment workflow target

Desired future deployment command on `firefox`:

```bash
cd /srv/battleship/compose
docker compose pull
docker compose up -d
docker compose ps
curl -fsS http://127.0.0.1:8000/health
curl -fsS http://127.0.0.1:8001/health
```

## First follow-up tasks

1. create Firefox-specific compose override or deployment directory
2. define reverse-proxy config and LAN hostname
3. define backup path and restore test procedure
4. configure a Gitea runner with SSH access to `firefox`
5. add deployment automation after the manual runbook is validated
