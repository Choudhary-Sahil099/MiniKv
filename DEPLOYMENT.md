# MiniKV demo deployment (Docker Compose)

This deployment runs three MiniKV nodes, Prometheus, and a provisioned Grafana dashboard.
It is intended for a controlled demonstration, not as a production database service.

## Architecture and exposure

- `minikv-a`, `minikv-b`, and `minikv-c` communicate over a private Docker bridge network.
- Each node has its own persistent Docker volume for WAL and snapshots.
- Prometheus scrapes each node's `/metrics` endpoint on the private network.
- MiniKV TCP ports 5000-5002 are published on host loopback only by default, allowing a local CLI or benchmark to connect without exposing the unauthenticated protocol to the LAN.
- Grafana binds to `127.0.0.1:3000`; Prometheus binds to `127.0.0.1:9090`.

**Do not publish unauthenticated MiniKV TCP ports directly to the public internet.** The current wire protocol does not provide client authentication or TLS. For remote administration, use an SSH tunnel or a VPN. For a public dashboard, put Grafana behind an HTTPS reverse proxy and restrict access; do not expose the default HTTP listener directly.

## Prerequisites

- Docker Engine with the Docker Compose v2 plugin.
- At least 4 CPU cores and 4 GB RAM is recommended for the three-node stack plus Prometheus/Grafana. Smaller hosts may run the demo with reduced throughput; benchmark the actual host before making performance claims.
- Go 1.26.3 is required for native builds and the full test suite, matching `go.mod`.

## Pre-deployment verification (Windows PowerShell)

Run the repository verification script before building or starting the stack. It requires the exact Go toolchain declared in `go.mod` and Docker Compose v2:

```powershell
.\scripts\verify.ps1
```

To run the Go checks and Compose configuration validation without building the image:

```powershell
.\scripts\verify.ps1 -SkipDockerBuild
```

## Start locally

PowerShell:

```powershell
Copy-Item .env.example .env
notepad .env
# Replace GRAFANA_ADMIN_PASSWORD with a unique strong password.
docker compose config
docker compose up -d --build
docker compose ps
docker compose logs -f minikv-a minikv-b minikv-c
```

Open `http://localhost:3000`, sign in using the values in `.env`, and open the **MiniKV / MiniKV Cluster Overview** dashboard. Prometheus targets can be inspected at `http://localhost:9090/targets`.

Prometheus scrapes all three node endpoints over the private Compose network. Check the targets page at `http://localhost:9090/targets` and confirm all three are **UP**.

Stop without deleting persisted data:

```powershell
docker compose down
```

Remove the demo and its persisted data only when you intentionally want a clean reset:

```powershell
docker compose down -v
```

## Offline data backup

The PowerShell backup script stops all three MiniKV nodes before archiving their named volumes, then attempts to start them again. This creates a consistent offline copy of each node's WAL and snapshot files, at the cost of brief cluster downtime.

```powershell
.\scripts\backup.ps1
```

The default backup directory is `./backups`. Store these archives outside the host and encrypt them according to your operational policy. Test restores periodically; an archive that has never been restored is not a verified backup.

To restore, stop the stack, then extract the matching node archive into its named volume. Example for Node A (replace the archive name with the file created by the backup script):

```powershell
docker compose stop minikv-a minikv-b minikv-c
docker run --rm --volume minikv-demo_minikv_a_data:/data --volume "${PWD}/backups:/backup" alpine:3.22 sh -c "tar -xzf /backup/minikv-minikv-a-YYYYMMDD-HHMMSS.tar.gz -C /data"
docker compose start minikv-a minikv-b minikv-c
```

Restore each node's own archive to its matching volume. Do not copy one node's data volume over another node's volume. If the Compose project name is overridden with `-p` or `COMPOSE_PROJECT_NAME`, the volume names will differ; inspect them with `docker volume ls` before running the restore command.

## Release validation status

The Compose configuration and deployment assets are prepared for a controlled single-host demonstration. This is not yet a production database release. In particular, hinted-handoff entries are currently memory-only, WAL segment cleanup is deliberately disabled because snapshot checkpoints are not tied to segment sequence numbers, and the text TCP protocol has no authentication or TLS. Complete the release gates in `AUDIT_REPORT.md` and validate the image and volume recovery on the target host before accepting production data.

## Public cloud demo

1. Provision one small Linux VM with Docker Engine and Compose v2. This is a cost-bearing cloud action; choose the region/instance and approve the expected cost in the provider console.
2. Copy the repository to the VM and create `.env` with a unique password. Never commit `.env`.
3. Keep ports 5000-5002 and 9090 closed in the cloud firewall/security group. Docker publishes MiniKV TCP ports on VM loopback by default; do not change the bind address to a public interface.
4. Create a DNS A/AAAA record for `GRAFANA_DOMAIN` pointing to the VM public IP. Copy `.env.example` to `.env`, set a unique strong Grafana password, and set `GRAFANA_DOMAIN` to that DNS name.
5. Set `GF_SERVER_ROOT_URL=https://<your-domain>/` in `.env`.
6. Allow inbound TCP 80 and 443 in the cloud firewall/security group. Keep 3000, 9090, and 5000-5002 closed to the internet. Restrict SSH (22) to your own IP.
7. Start the stack with `docker compose --profile public up -d --build`. Caddy obtains and renews the HTTPS certificate automatically. Verify all three node health checks and Prometheus targets before sharing `https://<GRAFANA_DOMAIN>`.

For local-only use, omit `--profile public`; Grafana remains bound to `127.0.0.1:3000` and Prometheus to `127.0.0.1:9090`.

A single VM running three containers demonstrates a three-process cluster, but it is not a multi-host fault-tolerance test: all nodes still share one machine and its failure domain. This public profile exposes only the authenticated Grafana dashboard over HTTPS; it does not expose the unauthenticated MiniKV TCP protocol to public clients.

## Important implementation notes

- `MINIKV_BIND_HOST` controls the server bind interface. Docker sets it to `0.0.0.0` so other containers can connect.
- `MINIKV_CLUSTER_CONFIG` selects the Docker DNS-based cluster membership file.
- `MINIKV_METRICS_ADDR` configures the metrics listener. Each container can safely use `:2112` because it has its own network namespace.
- `MINIKV_MAX_CONNECTIONS` bounds concurrent incoming TCP connections per node; the default is 1024. Tune it using load tests and host resource measurements, not by guesswork.
- `MINIKV_WAL_SEGMENT_BYTES` controls the WAL segment rotation threshold; the default is 67,108,864 bytes (64 MiB). Startup reuses the newest recovered segment and rotates before a commit batch would exceed the threshold. Old segments are intentionally retained: automatic deletion is unsafe until snapshot checkpoints are tied to WAL sequence numbers.
- All three node containers run read-only apart from their persistent data volumes and a small `/tmp` tmpfs, drop Linux capabilities, and enable `no-new-privileges`.
- The existing local `config/cluster.json` remains unchanged for native Windows development.
- Each node stores its data under `/app/data` in its own named volume.
