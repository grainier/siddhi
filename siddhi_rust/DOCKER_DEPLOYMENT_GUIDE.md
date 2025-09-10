# Siddhi Rust Docker Deployment Guide

## Overview

This guide explains how to deploy Siddhi Rust using Docker, following industry standards from Apache Flink, Kafka Streams, and WSO2 Siddhi. The deployment model separates the **engine** (running in containers) from **applications** (deployed via YAML or .siddhi files).

## Architecture

```
┌─────────────────────────────────────┐
│         Siddhi Apps (.siddhi)       │  <- User-defined streaming queries
├─────────────────────────────────────┤
│      Deployment Descriptors (YAML)   │  <- Configuration and metadata
├─────────────────────────────────────┤
│      Siddhi Runtime Container       │  <- Engine running in Docker
├─────────────────────────────────────┤
│         State Backend (Redis)       │  <- Distributed state storage
└─────────────────────────────────────┘
```

## Quick Start

### 1. Single-Node Deployment

```bash
# Build the Docker image
docker build -t siddhi-rust:latest .

# Run single node with docker-compose
docker-compose -f docker-compose-single.yml up

# Or run directly with Docker
docker run -p 8006:8006 -p 8080:8080 \
  -v $(pwd)/apps:/opt/siddhi/apps \
  siddhi-rust:latest
```

### 2. Distributed Cluster Deployment

```bash
# Start a 3-node cluster with manager and workers
docker-compose -f docker-compose-cluster.yml up --build

# Scale workers
docker-compose -f docker-compose-cluster.yml up --scale siddhi-worker=5
```

### 3. Deploy Siddhi Applications

Applications can be deployed in multiple ways:

#### Method 1: Volume Mount (.siddhi files)
```bash
# Place .siddhi files in the apps directory
cp my-app.siddhi ./apps/

# Container will auto-deploy on startup
docker run -v $(pwd)/apps:/opt/siddhi/apps siddhi-rust:latest
```

#### Method 2: YAML Deployment Descriptor
```yaml
# apps/my-app.yaml
apiVersion: siddhi.io/v1beta1
kind: SiddhiApp
metadata:
  name: my-app
spec:
  query: |
    @app:name('MyApp')
    define stream Input (name string, value double);
    from Input[value > 100]
    select * insert into Output;
```

#### Method 3: HTTP API Deployment
```bash
# POST the Siddhi app to the runtime
curl -X POST http://localhost:8006/siddhi-apps \
  -H "Content-Type: text/plain" \
  --data-binary @my-app.siddhi
```

## Container Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `NODE_ID` | Unique node identifier | `hostname` |
| `CLUSTER_ENABLED` | Enable distributed mode | `false` |
| `SEED_NODES` | Comma-separated seed nodes | - |
| `PERSISTENCE_TYPE` | State persistence type | `file` |
| `REDIS_ENDPOINT` | Redis connection string | `redis:6379` |
| `STATE_BACKEND` | Distributed state backend | `redis` |
| `SIDDHI_THREADS` | Number of worker threads | `8` |
| `METRICS_ENABLED` | Enable Prometheus metrics | `true` |
| `SIDDHI_PROPERTIES` | Additional properties | - |

### Using SIDDHI_PROPERTIES

Following Flink's pattern with `FLINK_PROPERTIES`:

```bash
docker run -e SIDDHI_PROPERTIES="
  engine.threads: 16
  engine.batch_size: 5000
  persistence.checkpoint_interval: 30s
" siddhi-rust:latest
```

## Deployment Patterns

### Pattern 1: Development (Single Container)

```yaml
# docker-compose.yml
services:
  siddhi:
    image: siddhi-rust:latest
    ports:
      - "8006:8006"
      - "8080:8080"
    volumes:
      - ./apps:/opt/siddhi/apps
      - ./state:/opt/siddhi/state
```

### Pattern 2: Production Cluster

```yaml
# docker-compose-cluster.yml
services:
  siddhi-manager:
    image: siddhi-rust:latest
    command: ["manager"]
    environment:
      - CLUSTER_ENABLED=true
      
  siddhi-worker:
    image: siddhi-rust:latest
    command: ["worker"]
    deploy:
      replicas: 3
    environment:
      - CLUSTER_ENABLED=true
      - SEED_NODES=siddhi-manager:7000
```

### Pattern 3: Kubernetes Deployment

```bash
# Deploy to Kubernetes
kubectl apply -f k8s-deployment.yaml

# Check status
kubectl get pods -n siddhi

# Deploy a Siddhi app via ConfigMap
kubectl create configmap siddhi-apps \
  --from-file=apps/ \
  -n siddhi
```

## Volume Mounts

| Path | Purpose | Mode |
|------|---------|------|
| `/opt/siddhi/apps` | Siddhi application files | Read-only |
| `/opt/siddhi/deployment` | Hot deployment directory | Read-write |
| `/opt/siddhi/config` | Configuration files | Read-only |
| `/opt/siddhi/state` | Persistent state | Read-write |
| `/opt/siddhi/extensions` | Custom extensions | Read-only |
| `/opt/siddhi/logs` | Application logs | Read-write |

## Networking

### Exposed Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 8006 | HTTP | App deployment API |
| 8080 | HTTP | Health check endpoint |
| 7000 | TCP/gRPC | Cluster communication |
| 9090 | HTTP | Prometheus metrics |

### Health Checks

```bash
# Check health
curl http://localhost:8080/health

# Response
{
  "status": "UP",
  "node_id": "worker-1",
  "cluster": {
    "mode": "distributed",
    "nodes": 3,
    "leader": "manager-1"
  }
}
```

## Monitoring

### Prometheus Metrics

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'siddhi'
    static_configs:
      - targets: 
        - 'siddhi-manager:9090'
        - 'siddhi-worker-1:9090'
        - 'siddhi-worker-2:9090'
```

### Available Metrics

- `siddhi_events_processed_total` - Total events processed
- `siddhi_events_rate` - Current event rate
- `siddhi_query_latency_seconds` - Query execution latency
- `siddhi_state_size_bytes` - State size per operator
- `siddhi_checkpoint_duration_seconds` - Checkpoint duration
- `siddhi_cluster_nodes` - Number of cluster nodes

## Multi-Stage Build

The Dockerfile uses multi-stage builds for optimization:

```dockerfile
# Stage 1: Builder (compile Rust)
FROM rust:1.75 as builder

# Stage 2: Runtime base (minimal dependencies)
FROM debian:slim as runtime-base

# Stage 3: Production image
FROM runtime-base as siddhi-runner

# Stage 4: Development image (with tools)
FROM siddhi-runner as siddhi-dev

# Stage 5: Alpine variant (smaller size)
FROM alpine:3.18 as siddhi-runner-alpine
```

### Image Variants

| Variant | Base | Size | Use Case |
|---------|------|------|----------|
| `siddhi-runner` | Debian slim | ~150MB | Production |
| `siddhi-dev` | Debian slim | ~200MB | Development |
| `siddhi-runner-alpine` | Alpine | ~50MB | Minimal deployments |

## Examples

### Example 1: Stream Processing with Kafka

```yaml
# apps/kafka-processing.yaml
apiVersion: siddhi.io/v1beta1
kind: SiddhiApp
metadata:
  name: kafka-processor
spec:
  query: |
    @source(type='kafka', 
            topic='input-topic',
            bootstrap.servers='kafka:9092')
    define stream InputStream (id string, value double);
    
    @sink(type='kafka',
          topic='output-topic',
          bootstrap.servers='kafka:9092')
    define stream OutputStream (id string, total double);
    
    from InputStream#window.time(1 min)
    select id, sum(value) as total
    group by id
    insert into OutputStream;
```

### Example 2: HTTP API Integration

```yaml
# apps/http-api.yaml
apiVersion: siddhi.io/v1beta1
kind: SiddhiApp
metadata:
  name: http-processor
spec:
  sources:
    - type: http
      config:
        receiver.url: "http://0.0.0.0:8006/events"
  sinks:
    - type: http
      config:
        publisher.url: "http://api-service:8080/results"
```

### Example 3: Pattern Detection

```siddhi
@app:name('FraudDetection')

define stream Transaction (userId string, amount double, location string);
define stream FraudAlert (userId string, message string);

from every t1 = Transaction 
  -> t2 = Transaction[t1.userId == userId and 
                      location != t1.location]
  within 5 min
select t1.userId, 
       str:concat('Suspicious activity: transactions from ', 
                  t1.location, ' and ', t2.location) as message
insert into FraudAlert;
```

## Troubleshooting

### Container Won't Start

```bash
# Check logs
docker logs siddhi-container

# Debug with shell
docker run -it --entrypoint /bin/bash siddhi-rust:latest

# Check health
docker exec siddhi-container curl localhost:8080/health
```

### Applications Not Deploying

```bash
# Check deployment directory
docker exec siddhi-container ls -la /opt/siddhi/deployment

# Check logs for deployment errors
docker exec siddhi-container tail -f /opt/siddhi/logs/siddhi.log
```

### Cluster Formation Issues

```bash
# Check network connectivity
docker network inspect siddhi-network

# Test cluster communication
docker exec siddhi-worker-1 nc -zv siddhi-manager 7000

# Check seed nodes
docker exec siddhi-worker-1 env | grep SEED_NODES
```

## Best Practices

1. **Resource Limits**: Always set memory and CPU limits
2. **Health Checks**: Configure proper health checks for orchestrators
3. **Persistence**: Use external state backends for production
4. **Monitoring**: Enable metrics and set up dashboards
5. **Security**: Run as non-root user, use secrets for passwords
6. **Logging**: Configure centralized logging
7. **Backup**: Regular state backups for disaster recovery

## Migration from Java Siddhi

1. **Apps are compatible**: Most Siddhi queries work unchanged
2. **Mount existing apps**: Use volume mounts for .siddhi files
3. **Configuration mapping**: Environment variables map to config
4. **State migration**: Export/import state if needed

## Next Steps

- Deploy to Kubernetes using provided manifests
- Set up monitoring with Prometheus and Grafana
- Implement custom extensions
- Configure distributed processing with Raft coordination
- Integrate with Kafka/Pulsar for event streaming

## References

- [Apache Flink Docker](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/resource-providers/standalone/docker/)
- [Kafka Streams Docker](https://docs.docker.com/guides/kafka/)
- [WSO2 Streaming Integrator](https://ei.docs.wso2.com/en/latest/streaming-integrator/quick-start-guide/hello-world-with-docker/)