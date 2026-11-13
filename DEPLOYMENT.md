# MiniKV – AWS Deployment

MiniKV is a Dynamo-inspired distributed key-value store written in Go. This document describes the deployment of a three-node MiniKV cluster on  **AWS EC2** , including containerization, persistent data restoration, cluster validation, and performance testing.

## 1. Deployment Overview

MiniKV was deployed as a three-node Dockerized cluster on an AWS EC2 instance.

### Infrastructure

| Component         | Configuration                            |
| ----------------- | ---------------------------------------- |
| Cloud provider    | AWS                                      |
| Compute service   | Amazon EC2                               |
| Region            | Sydney (`<span>ap-southeast-2</span>`) |
| Instance type     | `<span>t3.small</span>`                |
| Compute resources | 2 vCPU, 2 GiB RAM                        |
| Operating system  | Ubuntu 26.04 LTS                         |
| Storage           | 20 GiB encrypted gp3 EBS                 |
| Container runtime | Docker Engine                            |
| Orchestration     | Docker Compose                           |

### Cluster Architecture

The deployment consists of three MiniKV nodes running as independent Docker containers on the same EC2 instance.

| Node     | Internal TCP port | Persistent volume                          |
| -------- | ----------------- | ------------------------------------------ |
| MiniKV-A | 5000              | `<span>minikv-demo_minikv_a_data</span>` |
| MiniKV-B | 5001              | `<span>minikv-demo_minikv_b_data</span>` |
| MiniKV-C | 5002              | `<span>minikv-demo_minikv_c_data</span>` |

The nodes communicate over the Docker network. Each node maintains its own persistent storage, while the cluster uses consistent hashing, replication, quorum-based operations, and gossip-based failure detection.

## 2. Deployment Architecture

```
                   Developer Machine
                          |
                     SSH Connection
                          |
                          v
                  AWS EC2 Instance
                    Ubuntu 26.04
                          |
                    Docker Compose
                          |
              +-----------+-----------+
              |           |           |
              v           v           v
          MiniKV-A    MiniKV-B    MiniKV-C
           :5000       :5001       :5002
              |           |           |
              v           v           v
          Volume A    Volume B    Volume C
              |           |           |
              +-----------+-----------+
                   Persistent Data
```

The EC2 security group restricts SSH access to the configured source IP. MiniKV TCP ports are not exposed publicly. Client access is provided through SSH tunnels.

## 3. Deployment Process

### Prerequisites

* An AWS account with an EC2 instance.
* Ubuntu-based EC2 environment.
* Docker Engine and Docker Compose.
* MiniKV Docker image.
* MiniKV Docker Compose configuration.
* Persistent data backup archives, where existing data needs to be restored.

### Step 1: Prepare the Docker Image

The MiniKV application was packaged into a Docker image:

```
docker build -t minikv:v0.1.0 .
```

The image was transferred to the EC2 instance and loaded into its local Docker image store:

```
docker load -i minikv-image.tar
```

### Step 2: Transfer Deployment Files

The deployment directory contains the Docker Compose configuration and supporting deployment files.

```
minikv-deploy/
├── docker-compose.yml
├── .env
├── deploy/
│   ├── Caddyfile
│   ├── prometheus/
│   │   └── prometheus.yml
│   └── grafana/
│       ├── provisioning/
│       └── dashboards/
└── backups/
    ├── minikv-a-backup.tar.gz
    ├── minikv-b-backup.tar.gz
    └── minikv-c-backup.tar.gz
```

The monitoring configuration is included in the deployment files, but Prometheus and Grafana were used in the local development environment and were not part of the running AWS deployment.

### Step 3: Restore Persistent Data

Three independent Docker volumes were created for the MiniKV nodes.

```
docker volume create minikv-demo_minikv_a_data
docker volume create minikv-demo_minikv_b_data
docker volume create minikv-demo_minikv_c_data
```

The corresponding backup archives were extracted into their respective volumes using temporary Alpine containers.

Example for Node A:

```
docker run --rm \
  -v minikv-demo_minikv_a_data:/data \
  -v "$PWD/backups:/backups:ro" \
  alpine \
  sh -c "tar -xzf /backups/minikv-a-backup.tar.gz -C /data"
```

The same process was performed for Nodes B and C using their respective backup archives and volumes.

The restored data included snapshots and Write-Ahead Log (WAL) files. File ownership was maintained for the application user.

### Step 4: Configure Environment Variables

The environment configuration was kept in a local `<span>.env</span>` file on the EC2 instance.

Sensitive values were not committed to the repository.

```
chmod 600 .env
```

### Step 5: Start the Three-Node Cluster

The MiniKV nodes were started using Docker Compose:

```
docker compose up -d --no-build \
  minikv-a minikv-b minikv-c
```

Check container health:

```
docker compose ps
```

Inspect node logs:

```
docker compose logs --tail=100 \
  minikv-a minikv-b minikv-c
```

The deployment was verified through successful container health checks, snapshot loading, TCP server startup, and gossip-based peer discovery.

## 4. Secure Client Access

MiniKV node ports were kept private and bound to localhost on the EC2 host.

SSH local port forwarding was used to access the cluster from the developer machine.

Example:

```
ssh -i "minikv-cloud-key.pem" \
  -L 15000:127.0.0.1:5000 \
  ubuntu@<EC2_PUBLIC_IP>
```

Additional tunnels were established for the other two nodes:

```
-L 15001:127.0.0.1:5001
-L 15002:127.0.0.1:5002
```

The MiniKV client could then connect through the forwarded local ports.

```
go run ./cmd/client 15000
```

This approach allowed remote testing without exposing the database ports directly to the public internet.

## 5. Deployment Validation

The AWS deployment was tested using the MiniKV client and Docker diagnostics.

### Functional Validation

The following operations were verified:

* Establishing client connections to the deployed nodes.
* Writing key-value pairs using `<span>SET</span>`.
* Retrieving stored values using `<span>GET</span>`.
* Deleting keys using `<span>DEL</span>`.
* Confirming deleted keys were no longer available.
* Testing client access through different cluster nodes.
* Verifying inter-node communication and replication.
* Confirming snapshot loading and WAL recovery after deployment.

### Cluster Health

The three MiniKV containers reached a healthy state.

```
docker compose ps
```

The application logs also confirmed that nodes recovered their peers through the gossip protocol.

## 6. AWS Performance Benchmark

Performance testing was conducted directly from the EC2 host using a Linux build of the MiniKV load-testing program.

This avoided measuring the additional latency introduced by the developer machine's internet connection and SSH tunnels.

### Benchmark Configuration

| Parameter          | Value                      |
| ------------------ | -------------------------- |
| Environment        | AWS EC2                    |
| Cluster            | Three MiniKV nodes         |
| Workload           | SET-only                   |
| Total requests     | 20,000                     |
| Concurrent clients | 100                        |
| Connection model   | Persistent TCP connections |
| Request timeout    | 10 seconds                 |

### Benchmark Results

Two recorded 20,000-write runs produced the following results:

| Metric            | Run 1               | Run 2               |
| ----------------- | ------------------- | ------------------- |
| Successful writes | 20,000              | 20,000              |
| Failed writes     | 0                   | 0                   |
| Throughput        | 4,230.01 writes/sec | 4,160.30 writes/sec |
| p50 latency       | 19.40 ms            | 19.40 ms            |
| p95 latency       | 36.53 ms            | 39.11 ms            |
| p99 latency       | 48.22 ms            | 60.85 ms            |

**Observed result:** Both runs completed 20,000 writes with zero failures and achieved approximately 4.16K–4.23K successful writes per second.

These measurements represent a SET-only workload on the specified EC2 environment. They should not be interpreted as GET throughput or as a production capacity guarantee.

## 7. Security and Operational Considerations

* EC2 SSH access was restricted to the configured source IP.
* MiniKV application ports were not opened to the public internet.
* Remote client access used SSH port forwarding.
* Persistent data was stored in separate Docker volumes.
* The EC2 environment used encrypted EBS storage.
* Credentials and environment secrets were kept out of source control.
* The instance can be stopped when it is not needed for demonstrations or testing.

## 8. Deployment Evidence

Screenshots and a demonstration video will be added here.

Suggested evidence:

| Evidence                  | File                                          |
| ------------------------- | --------------------------------------------- |
| AWS EC2 instance details  | `<span>images/aws-ec2-instance.png</span>`  |
| Three-node Docker cluster | `<span>images/cluster-health.png</span>`    |
| Gossip and recovery logs  | `<span>images/cluster-logs.png</span>`      |
| Client CRUD operations    | `<span>images/client-operations.png</span>` |
| AWS benchmark output      | `<span>images/aws-benchmark.png</span>`     |

### Deployment Demonstration

**Video:** Add your demonstration video link here.

---

## 9. Summary

MiniKV v0.1.0 was deployed on AWS EC2 as a three-node Dockerized distributed key-value store.

The deployment restored persistent snapshots and WAL data, verified node health and gossip-based peer discovery, and successfully handled client operations across the cluster.

A 20,000-request SET-only benchmark achieved approximately 4.2K successful writes per second with zero failed writes in the recorded runs.

This deployment demonstrates the implementation and cloud deployment of a distributed storage system, including containerization, persistent data recovery, cluster communication, and performance testing.
