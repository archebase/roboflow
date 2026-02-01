# Roboflow Kubernetes Deployment

This directory contains Kubernetes manifests for deploying Roboflow as long-running worker pods.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    roboflow-worker Deployment               │
│  ┌───────────────────────────────────────────────────────┐  │
│  │               Worker Container                         │  │
│  │  - Claims jobs from TiKV queue                       │  │
│  │  - Processes bag/MCAP to LeRobot datasets             │  │
│  │  - Sends heartbeats to TiKV                           │  │
│  │  - Health server on :8080                              │  │
│  └───────────────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────┐  │
│  │               Scanner Sidecar                         │  │
│  │  - Discovers files in storage                        │  │
│  │  - Creates jobs in TiKV                              │  │
│  │  - Leader election via TiKV locks                    │  │
│  │  - Health server on :8080 (different process)        │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                            ↓
                    ┌───────────────┐
                    │  TiKV Cluster  │
                    │  (Coordination)│
                    └───────────────┘
                            ↓
                    ┌───────────────┐
                    │  S3/OSS Storage│
                    │  (Input/Output)│
                    └───────────────┘
```

## Manifests

| File | Description |
|------|-------------|
| `namespace.yaml` | Roboflow namespace |
| `configmap.yaml` | Configuration (TiKV endpoints, timeouts, paths) |
| `secrets.yaml` | Secret template for cloud storage credentials |
| `deployment.yaml` | Worker deployment with scanner sidecar |
| `scanner-standalone.yaml` | Optional standalone scanner deployment |
| `service.yaml` | Included in deployment.yaml |
| `hpa.yaml` | HorizontalPodAutoscaler for auto-scaling |
| `pdb.yaml` | PodDisruptionBudget for graceful updates |
| `servicemonitor.yaml` | Prometheus ServiceMonitor for metrics |

## Quick Start

### 1. Create Namespace

```bash
kubectl apply -f deploy/k8s/namespace.yaml
```

### 2. Create ConfigMap

```bash
kubectl apply -f deploy/k8s/configmap.yaml
```

### 3. Create Secret (for cloud storage)

```bash
kubectl create secret generic roboflow-secrets \
  --from-literal=AWS_ACCESS_KEY_ID=your_key_id \
  --from-literal=AWS_SECRET_ACCESS_KEY=your_secret \
  --from-literal=AWS_REGION=us-east-1 \
  --namespace=roboflow
```

Or use IRSA (IAM Roles for Service Accounts) for AWS, which is recommended for production.

### 4. Deploy Workers

```bash
kubectl apply -f deploy/k8s/deployment.yaml
```

### 5. Deploy HPA and PDB

```bash
kubectl apply -f deploy/k8s/hpa.yaml
kubectl apply -f deploy/k8s/pdb.yaml
```

### 6. (Optional) Deploy ServiceMonitor

```bash
kubectl apply -f deploy/k8s/servicemonitor.yaml
```

## Configuration

### Environment Variables

Configuration is managed via the `roboflow-config` ConfigMap. Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `TIKV_PD_ENDPOINTS` | `127.0.0.1:2379` | TiKV placement driver endpoints |
| `STORAGE_URL` | - | S3/OSS storage URL |
| `WORKER_MAX_CONCURRENT_JOBS` | `1` | Max jobs per worker |
| `WORKER_POLL_INTERVAL_SECS` | `5` | Job poll interval |
| `HEALTH_PORT` | `8080` | Health server port |

### Resource Requirements

Default resource requests/limits per pod:

| Container | CPU Request | CPU Limit | Memory Request | Memory Limit |
|-----------|-------------|----------|----------------|--------------|
| Worker | 4 | 8 | 16Gi | 32Gi |
| Scanner | 500m | 1 | 512Mi | 1Gi |

### GPU Support

Uncomment the GPU resource limit in `deployment.yaml`:

```yaml
resources:
  limits:
    nvidia.com/gpu: "1"
```

## Health Probes

All containers expose HTTP health endpoints on port 8080:

- `/health/live` - Liveness probe (always returns 200 if process running)
- `/health/ready` - Readiness probe (200 when connected to TiKV)
- `/health` - Basic health check

Test from within the cluster:

```bash
kubectl exec -n roboflow deployment/roboflow-worker -c worker -- \
  curl http://localhost:8080/health
```

## Scaling

### Manual Scaling

```bash
kubectl scale deployment/roboflow-worker --replicas=10 -n roboflow
```

### Auto-Scaling

The HPA is configured to scale based on CPU and memory utilization:

```bash
kubectl get hpa -n roboflow
```

For custom metrics (e.g., pending jobs in TiKV), configure Prometheus Adapter and update `hpa.yaml`.

## Monitoring

Metrics are exposed in Prometheus format on `/metrics` (port 8080).

Available metrics:
- `roboflow_jobs_claimed_total` - Total jobs claimed
- `roboflow_jobs_completed_total` - Total jobs completed
- `roboflow_jobs_failed_total` - Total jobs failed
- `roboflow_active_jobs` - Current active jobs
- `roboflow_scanner_files_discovered_total` - Files discovered
- `roboflow_scanner_jobs_created_total` - Jobs created

## Logs

View logs for a specific pod:

```bash
kubectl logs -n roboflow deployment/roboflow-worker -c worker -f
```

View scanner logs:

```bash
kubectl logs -n roboflow deployment/roboflow-worker -c scanner -f
```

## Troubleshooting

### Worker not claiming jobs

1. Check TiKV connectivity:
   ```bash
   kubectl exec -n roboflow deployment/roboflow-worker -c worker -- \
     nc -zv tikv.tikv.svc.cluster.local 2379
   ```

2. Check worker logs for errors:
   ```bash
   kubectl logs -n roboflow deployment/roboflow-worker -c worker --tail=100
   ```

3. Verify jobs exist in TiKV (requires TiKV CLI)

### Scanner not creating jobs

1. Check scanner logs:
   ```bash
   kubectl logs -n roboflow deployment/roboflow-worker -c scanner --tail=100
   ```

2. Verify storage accessibility and `SCANNER_INPUT_PREFIX`

3. Check if scanner is the leader (only leader creates jobs)

### Pod failing readiness probe

1. Check if TiKV is reachable from the pod
2. Verify `TIKV_PD_ENDPOINTS` in ConfigMap
3. Check network policies allow pod-to-TiKV communication

## Upgrading

Rolling updates are configured with 25% max unavailable and 25% max surge:

```bash
kubectl set image deployment/roboflow-worker \
  worker=roboflow:v2.0.0 \
  scanner=roboflow:v2.0.0 \
  -n roboflow
```

Watch the rollout:

```bash
kubectl rollout status deployment/roboflow-worker -n roboflow
```

## Cleanup

Delete all resources:

```bash
kubectl delete namespace roboflow
```

Or delete individual resources:

```bash
kubectl delete -f deploy/k8s/
```
