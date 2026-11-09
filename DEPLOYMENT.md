# MiniKV demo deployment (Docker Compose)

This deployment runs three MiniKV nodes, Prometheus, and a provisioned Grafana dashboard.
It is intended for a controlled demonstration, not as a production database service.

## Architecture and exposure

- `minikv-a`, `minikv-b`, and `minikv-c` communicate over a private Docker bridge network.
- Each node has its own persistent Docker volume for WAL and snapshots.
- Prometheus scrapes each node's `/metrics` endpoint on the private network.
- Grafana is the only service published to the host. By default it binds to `127.0.0.1:3000`.
- MiniKV's raw TCP ports (5000-5002) and Prometheus (9090) are not published to the host.

**Do not publish unauthenticated MiniKV TCP ports directly to the public internet.** The current wire protocol does not provide client authentication or TLS. For remote administration, use an SSH tunnel or a VPN. For a public dashboard, put Grafana behind an HTTPS reverse proxy and restrict access; do not expose the default HTTP listener directly.

## Prerequisites

- Docker Engine with the Docker Compose v2 plugin.
- At least 2 CPU cores and 2 GB RAM for a small demo; more is preferable when benchmarking.

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

## Public cloud demo

1. Provision one small Linux VM with Docker Engine and Compose v2. This is a cost-bearing cloud action; choose the region/instance and approve the expected cost in the provider console.
2. Copy the repository to the VM and create `.env` with a unique password. Never commit `.env`.
3. Keep ports 5000-5002 and 9090 closed in the cloud firewall/security group.
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
- The existing local `config/cluster.json` remains unchanged for native Windows development.
- Each node stores its data under `/app/data` in its own named volume.
