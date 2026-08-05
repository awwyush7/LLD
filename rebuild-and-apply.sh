#!/usr/bin/env bash
# rebuild-and-apply.sh
# Rebuilds all Python service images inside Minikube's Docker daemon,
# then applies all k8s changes. Run from the repo root (LLD/).
#
# Usage:
#   minikube start --cpus=8 --memory=6144   # do this first if minikube isn't running
#   eval $(minikube docker-env)              # point Docker CLI at minikube's daemon
#   bash rebuild-and-apply.sh

set -euo pipefail

# ── Sanity checks ──────────────────────────────────────────────────────────────
if ! docker info &>/dev/null; then
  echo "ERROR: Docker daemon not reachable. Run: eval \$(minikube docker-env)"
  exit 1
fi

if ! minikube status | grep -q "Running"; then
  echo "ERROR: Minikube not running. Start it first."
  exit 1
fi

echo "=== Building images inside Minikube's Docker daemon ==="
echo "(imagePullPolicy: Never means k8s will only use locally-built images)"

# All images use the LLD root as build context so PYTHONPATH=/app works
# and 'from Zoom.CarRentalSystem...' imports resolve correctly.

docker build -t carrental-ingestion-service:latest \
  -f Zoom/CarRentalSystem/IngestionService/Dockerfile . &
PIDS=($!)

docker build -t carrental-booking-service:latest \
  -f Zoom/CarRentalSystem/BookingService/Dockerfile . &
PIDS+=($!)

docker build -t carrental-payment-service:latest \
  -f Zoom/CarRentalSystem/PaymentService/Dockerfile . &
PIDS+=($!)

docker build -t carrental-ticket-service:latest \
  -f Zoom/CarRentalSystem/TicketService/Dockerfile . &
PIDS+=($!)

docker build -t carrental-prometheus-dashboard:latest \
  -f Prometheus/Dockerfile . &
PIDS+=($!)

echo "Building 5 images in parallel (PIDs: ${PIDS[*]})..."
wait "${PIDS[@]}"
echo "All images built."

# ── Apply k8s changes ──────────────────────────────────────────────────────────
echo ""
echo "=== Applying k8s manifests ==="

# kafka-deployment.yaml changed kind from Deployment to StatefulSet (same name,
# different kind = a different API object) — the old Deployment won't be
# replaced by `kubectl apply`, it'll keep running alongside the new StatefulSet
# and both will match the "app: kafka" selector. Delete it first, once.
kubectl delete deployment kafka --ignore-not-found

# Kafka: 3-broker KRaft StatefulSet, needs its headless Service for pod DNS.
# emptyDir storage means restart wipes all messages (fine for benchmarking).
kubectl apply -f k8s/kafka-headless-service.yaml
kubectl apply -f k8s/kafka-service.yaml
kubectl apply -f k8s/kafka-deployment.yaml
kubectl rollout status statefulset/kafka --timeout=180s
echo "Kafka is now a 3-broker cluster, replication factor 3."

# Postgres primary
kubectl apply -f k8s/postgres-deployment.yaml
kubectl rollout restart deployment/postgres
kubectl rollout status deployment/postgres --timeout=120s
echo "Postgres primary restarted."

# PgBouncer — sits in front of the primary; apply before the services that depend on it
kubectl apply -f k8s/pgbouncer-configmap.yaml
kubectl apply -f k8s/pgbouncer-secret.yaml
kubectl apply -f k8s/pgbouncer-deployment.yaml
kubectl apply -f k8s/pgbouncer-service.yaml
kubectl rollout status deployment/pgbouncer --timeout=120s
echo "PgBouncer up."

# Read replica — pg_basebackup clones the primary on first boot, so this can
# take a while the very first time. Safe to re-run; it skips re-cloning if the
# data dir already exists.
kubectl apply -f k8s/postgres-replica-pvc.yaml
kubectl apply -f k8s/postgres-replica-deployment.yaml
kubectl apply -f k8s/postgres-replica-service.yaml
kubectl rollout status deployment/postgres-replica --timeout=300s
echo "Postgres read replica up."

# Apply all service deployments + their Services
kubectl apply -f k8s/ingestion-service-deployment.yaml
kubectl apply -f k8s/booking-service-deployment.yaml
kubectl apply -f k8s/booking-service-service.yaml
kubectl apply -f k8s/payment-service-deployment.yaml
kubectl apply -f k8s/payment-service-service.yaml
kubectl apply -f k8s/ticket-service-deployment.yaml

# Prometheus dashboard — scrapes the above over their k8s Service DNS names
kubectl apply -f k8s/prometheus-deployment.yaml
kubectl apply -f k8s/prometheus-service.yaml

# Rolling restart forces pods to pick up the new images / env vars
kubectl rollout restart deployment/ingestion-service
kubectl rollout restart deployment/booking-service
kubectl rollout restart deployment/payment-service
kubectl rollout restart deployment/ticket-service
kubectl rollout restart deployment/prometheus-dashboard

# Wait for all rollouts
echo "Waiting for all services to roll out..."
kubectl rollout status deployment/ingestion-service    --timeout=180s
kubectl rollout status deployment/booking-service      --timeout=180s
kubectl rollout status deployment/payment-service      --timeout=180s
kubectl rollout status deployment/ticket-service       --timeout=180s
kubectl rollout status deployment/prometheus-dashboard --timeout=120s

# ── Verify ─────────────────────────────────────────────────────────────────────
echo ""
echo "=== Final pod state ==="
kubectl get pods -o wide

echo ""
echo "=== Verifying Kafka partition count (after first producer connects) ==="
echo "Wait ~30s then run:"
echo "  KAFKA_POD=\$(kubectl get pod -l app=kafka -o jsonpath='{.items[0].metadata.name}')"
echo "  kubectl exec \$KAFKA_POD -- /opt/kafka/bin/kafka-topics.sh \\"
echo "    --bootstrap-server localhost:9092 --describe"

echo ""
echo "=== Verifying Postgres max_connections ==="
echo "Run (after port-forwarding postgres):"
echo "  PGPASSWORD='Ayush@2002' psql -h localhost -p 15432 -U postgres -d carrental \\"
echo "    -c 'SHOW max_connections;'"

echo ""
echo "Done. Total pods:"
kubectl get pods --no-headers | wc -l
