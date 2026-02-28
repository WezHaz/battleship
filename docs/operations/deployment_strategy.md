# Battleship Deployment Strategy

## Source of truth and remotes

- Gitea is the CI/CD source of truth for this project.
- GitHub remains a parallel mirror for visibility and backup.
- Local repo remote policy:
  - `gitea`: primary push target for deployment automation
  - `origin`: GitHub mirror
- Local git config for this repo sets `remote.pushDefault=gitea`.

Recommended push flow:

```bash
./scripts/push_remotes.sh main
```

## First deployment target

- Primary target host: `firefox`
- Deployment mode: homelab-only first
- Goal: validate real source ingestion, scheduled scans, recommendation quality, and operator workflows before adding public cloud complexity

## First deployment architecture

Keep the first deployment intentionally small:

- reverse proxy on `firefox` (`Caddy` preferred for automatic TLS if exposed later)
- `frontend` container
- `recommender` container
- SQLite-backed persistence on a host-mounted volume
- `systemd` timer or cron for scheduled source scans
- nightly backup of SQLite data to another homelab server

Avoid for the first deployment:

- Kubernetes
- multi-node clustering
- mandatory Postgres
- object storage dependencies
- managed cloud services

## Infrastructure profile by stage

### Stage 1: homelab validation

- host: `firefox`
- runtime: Docker Compose or Podman Compose
- database: SQLite
- scheduler: `systemd` timer
- secrets: `.env` file managed on host
- backup target: another homelab server

### Stage 2: hosted fallback

If homelab availability becomes a problem, use a small public VM with the same stack:

- provider options:
  - Hetzner Cloud
  - DigitalOcean
  - Linode/Akamai
  - small EC2 instance
- keep the same deployment model:
  - reverse proxy
  - frontend + recommender containers
  - SQLite initially
  - host-mounted persistent storage

This keeps the homelab and cloud paths nearly identical and avoids a premature platform rewrite.

### Stage 3: scale-up path

Move to Postgres only when one or more of these become true:

- multiple application instances are required
- concurrent writes increase materially
- backup/restore requirements exceed SQLite convenience
- uptime expectations justify managed database operations

## CI/CD direction

### CI

Primary CI should run in Gitea Actions:

- `ruff check .`
- `pytest -q`
- container builds for `recommender`, `frontend`, `emailer`

GitHub Actions may remain enabled as a mirror validation pipeline, but deployment should not depend on GitHub.

### CD

Homelab CD should target `firefox` over SSH from a Gitea runner:

- build versioned container images
- publish images to a registry or export locally
- SSH to `firefox`
- update environment/config if needed
- run `docker compose pull && docker compose up -d`
- run post-deploy health checks

## Near-term execution order

1. add real job source adapters
2. prepare a Firefox-specific deployment profile and runbook
3. validate one homelab deployment with SQLite
4. add Gitea CI workflow and runner documentation
5. add deployment workflow for Firefox
6. mirror pushes to GitHub continuously
7. define public-cloud fallback VM image and deployment runbook
