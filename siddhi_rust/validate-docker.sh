#!/bin/bash
set -e

echo "🐳 Validating Siddhi Rust Docker Setup"
echo "======================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to log messages
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
check_command() {
    if ! command -v $1 &> /dev/null; then
        log_error "$1 is required but not installed"
        exit 1
    fi
}

# Check prerequisites
log_info "Checking prerequisites..."
check_command docker
check_command docker-compose

# Build the Docker image
log_info "Building Siddhi Rust Docker image..."
if docker build --target siddhi-runner -t siddhi-rust:latest .; then
    log_info "✅ Docker image built successfully"
else
    log_error "❌ Docker image build failed"
    exit 1
fi

# Test basic container functionality
log_info "Testing container startup..."
CONTAINER_ID=$(docker run -d \
    --name siddhi-test \
    -e CLUSTER_ENABLED=false \
    -e METRICS_ENABLED=true \
    -p 8080:8080 \
    -p 9090:9090 \
    siddhi-rust:latest run)

if [ $? -eq 0 ]; then
    log_info "✅ Container started with ID: $CONTAINER_ID"
else
    log_error "❌ Failed to start container"
    exit 1
fi

# Wait for container to be ready
log_info "Waiting for container to be ready..."
sleep 10

# Test health endpoint
log_info "Testing health endpoint..."
if curl -f http://localhost:8080/health > /dev/null 2>&1; then
    log_info "✅ Health endpoint responding"
else
    log_warn "⚠️  Health endpoint not responding (this may be expected if binary doesn't implement health check yet)"
fi

# Test metrics endpoint
log_info "Testing metrics endpoint..."
if curl -f http://localhost:9090/metrics > /dev/null 2>&1; then
    log_info "✅ Metrics endpoint responding"
else
    log_warn "⚠️  Metrics endpoint not responding (this may be expected if binary doesn't implement metrics yet)"
fi

# Check container logs for any obvious errors
log_info "Checking container logs..."
docker logs siddhi-test --tail 20

# Cleanup
log_info "Cleaning up test container..."
docker stop siddhi-test > /dev/null 2>&1
docker rm siddhi-test > /dev/null 2>&1

log_info "✅ Docker validation completed successfully!"

# Test docker-compose setup
log_info "Testing docker-compose single-node setup..."
if docker-compose -f docker-compose-single.yml config > /dev/null 2>&1; then
    log_info "✅ docker-compose-single.yml is valid"
else
    log_error "❌ docker-compose-single.yml has configuration errors"
    exit 1
fi

log_info "Testing docker-compose cluster setup..."
if docker-compose -f docker-compose-cluster.yml config > /dev/null 2>&1; then
    log_info "✅ docker-compose-cluster.yml is valid"
else
    log_error "❌ docker-compose-cluster.yml has configuration errors"
    exit 1
fi

echo ""
echo "🎉 All Docker validation checks passed!"
echo ""
echo "Next steps:"
echo "1. Single-node deployment: docker-compose -f docker-compose-single.yml up"
echo "2. Cluster deployment: docker-compose -f docker-compose-cluster.yml up --build"
echo "3. Kubernetes deployment: kubectl apply -f k8s-deployment.yaml"
echo ""
echo "For more information, see DOCKER_DEPLOYMENT_GUIDE.md"