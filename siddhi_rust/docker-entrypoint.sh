#!/bin/bash
set -e

# Docker entrypoint for Siddhi Rust
# Handles environment-based configuration and different run modes
# Following patterns from Flink, Kafka Streams, and WSO2 Siddhi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

log_debug() {
    if [[ "${DEBUG}" == "true" ]]; then
        echo -e "${BLUE}[DEBUG]${NC} $1"
    fi
}

# Configuration generation function
generate_config() {
    # Use a writable location for generated config
    local config_file="/tmp/siddhi-generated.yaml"
    
    log_info "Generating Siddhi configuration at ${config_file}"
    
    # Start with base configuration
    cat > "${config_file}" << EOF
# Siddhi Runtime Configuration
# Generated from environment variables

runtime:
  name: "${NODE_ID:-siddhi-node}"
  mode: "${RUNTIME_MODE:-single-node}"
  cluster_enabled: ${CLUSTER_ENABLED:-false}
  
# Thread pool configuration
threading:
  core_threads: ${SIDDHI_THREADS:-$(nproc)}
  max_threads: ${SIDDHI_MAX_THREADS:-$(($(nproc) * 2))}
  queue_size: ${SIDDHI_MAX_QUEUE:-100000}
  batch_size: ${SIDDHI_BATCH_SIZE:-1000}

# Persistence configuration
persistence:
  enabled: ${PERSISTENCE_ENABLED:-true}
  type: "${PERSISTENCE_TYPE:-file}"
  interval_seconds: ${PERSISTENCE_INTERVAL:-30}
  state_dir: "${SIDDHI_STATE_DIR}"
EOF

    # Add cluster configuration if enabled
    if [[ "${CLUSTER_ENABLED}" == "true" ]]; then
        cat >> "${config_file}" << EOF

# Cluster configuration
cluster:
  node_id: "${NODE_ID:-$(hostname)}"
  seed_nodes: "${SEED_NODES:-localhost:7000}"
  bind_address: "${BIND_ADDRESS:-0.0.0.0}"
  bind_port: ${BIND_PORT:-7000}
  election_timeout_ms: ${ELECTION_TIMEOUT:-5000}
  heartbeat_interval_ms: ${HEARTBEAT_INTERVAL:-1000}
EOF
    fi

    # Add Redis configuration if specified
    if [[ "${PERSISTENCE_TYPE}" == "redis" ]]; then
        cat >> "${config_file}" << EOF

# Redis persistence configuration
redis:
  url: "${REDIS_URL:-redis://localhost:6379}"
  pool_size: ${REDIS_POOL_SIZE:-10}
  timeout_ms: ${REDIS_TIMEOUT:-5000}
  max_retries: ${REDIS_MAX_RETRIES:-3}
EOF
    fi

    # Add monitoring configuration
    cat >> "${config_file}" << EOF

# Monitoring configuration
monitoring:
  metrics_enabled: ${METRICS_ENABLED:-true}
  metrics_port: ${METRICS_PORT:-9090}
  health_check_port: ${HEALTH_CHECK_PORT:-8080}
  
# API configuration
api:
  enabled: ${API_ENABLED:-true}
  port: ${API_PORT:-8006}
  bind_address: "${API_BIND_ADDRESS:-0.0.0.0}"

# Logging configuration
logging:
  level: "${LOG_LEVEL:-info}"
  format: "${LOG_FORMAT:-json}"
  file: "${SIDDHI_LOG_DIR}/siddhi.log"
EOF

    # Process additional properties from SIDDHI_PROPERTIES (like Flink)
    if [[ -n "${SIDDHI_PROPERTIES}" ]]; then
        log_info "Processing additional Siddhi properties"
        echo "" >> "${config_file}"
        echo "# Additional properties from SIDDHI_PROPERTIES" >> "${config_file}"
        echo "additional:" >> "${config_file}"
        echo "${SIDDHI_PROPERTIES}" | tr ';' '\n' | while read -r prop; do
            if [[ -n "${prop}" ]]; then
                # Convert key=value to YAML format
                local key="${prop%=*}"
                local value="${prop#*=}"
                echo "  ${key}: \"${value}\"" >> "${config_file}"
            fi
        done
    fi
    
    log_info "Configuration generated successfully"
    if [[ "${DEBUG}" == "true" ]]; then
        log_debug "Generated configuration:"
        cat "${config_file}"
    fi
}

# Function to deploy applications from directory
deploy_apps() {
    local apps_dir="${1:-${SIDDHI_APPS_DIR}}"
    
    if [[ -d "${apps_dir}" ]] && [[ "$(ls -A "${apps_dir}")" ]]; then
        log_info "Deploying applications from ${apps_dir}"
        
        for app_file in "${apps_dir}"/*.{siddhi,yaml,yml}; do
            if [[ -f "${app_file}" ]]; then
                local app_name=$(basename "${app_file}")
                log_info "Deploying application: ${app_name}"
                
                # Copy to deployment directory for hot deployment
                cp "${app_file}" "${SIDDHI_DEPLOYMENT_DIR}/"
                
                # TODO: Add API call to deploy app when HTTP API is implemented
                # curl -X POST "http://localhost:${API_PORT}/apps" -F "file=@${app_file}"
            fi
        done
    else
        log_warn "No applications found in ${apps_dir}"
    fi
}

# Function to wait for dependencies
wait_for_dependencies() {
    # Wait for Redis if using Redis persistence
    if [[ "${PERSISTENCE_TYPE}" == "redis" ]]; then
        local redis_host="${REDIS_HOST:-redis}"
        local redis_port="${REDIS_PORT:-6379}"
        
        log_info "Waiting for Redis at ${redis_host}:${redis_port}"
        
        # Wait for Redis to be ready (try connecting with timeout)
        local retries=0
        while [[ ${retries} -lt 30 ]]; do
            if timeout 3 bash -c "exec 3<>/dev/tcp/${redis_host}/${redis_port}" 2>/dev/null; then
                log_info "Redis is ready"
                break
            fi
            log_warn "Redis is unavailable - sleeping (${retries}/30)"
            sleep 2
            ((retries++))
        done
        
        if [[ ${retries} -eq 30 ]]; then
            log_warn "Redis still not available after 60 seconds, continuing anyway"
        fi
    fi
    
    # Wait for seed nodes if clustering is enabled
    if [[ "${CLUSTER_ENABLED}" == "true" ]] && [[ -n "${SEED_NODES}" ]]; then
        log_info "Checking seed nodes connectivity"
        
        IFS=',' read -ra NODES <<< "${SEED_NODES}"
        for node in "${NODES[@]}"; do
            IFS=':' read -ra ADDR <<< "${node}"
            local host="${ADDR[0]}"
            local port="${ADDR[1]:-7000}"
            
            # Don't wait for ourselves
            if [[ "${host}" != "$(hostname)" ]] && [[ "${host}" != "localhost" ]] && [[ "${host}" != "0.0.0.0" ]]; then
                log_info "Checking connectivity to seed node ${host}:${port}"
                
                # Give seed nodes some time to start, but don't block forever
                local retries=0
                while [[ ${retries} -lt 10 ]]; do
                    if timeout 3 bash -c "exec 3<>/dev/tcp/${host}/${port}" 2>/dev/null; then
                        log_info "Seed node ${host}:${port} is reachable"
                        break
                    fi
                    log_warn "Seed node ${host}:${port} not yet reachable, retrying in 3s... (${retries}/10)"
                    sleep 3
                    ((retries++))
                done
            fi
        done
    fi
}

# Pre-flight checks
preflight_checks() {
    log_info "Running pre-flight checks"
    
    # Check if binary exists
    if [[ ! -f "/opt/siddhi/bin/siddhi-runner" ]]; then
        log_error "Siddhi runner binary not found at /opt/siddhi/bin/siddhi-runner"
        exit 1
    fi
    
    # Check if binary is executable
    if [[ ! -x "/opt/siddhi/bin/siddhi-runner" ]]; then
        log_error "Siddhi runner binary is not executable"
        exit 1
    fi
    
    # Ensure required directories exist
    mkdir -p "${SIDDHI_STATE_DIR}" "${SIDDHI_LOG_DIR}" "${SIDDHI_DEPLOYMENT_DIR}"
    
    # Check disk space for state directory
    local available_space=$(df "${SIDDHI_STATE_DIR}" | awk 'NR==2 {print $4}')
    if [[ ${available_space} -lt 1048576 ]]; then  # Less than 1GB in KB
        log_warn "Low disk space available for state directory: ${available_space}KB"
    fi
    
    log_info "Pre-flight checks completed"
}

# Function to handle graceful shutdown
graceful_shutdown() {
    log_info "Received shutdown signal, stopping Siddhi gracefully..."
    
    if [[ -n "${SIDDHI_PID}" ]]; then
        # Send SIGTERM to the process
        kill -TERM "${SIDDHI_PID}" 2>/dev/null || true
        
        # Wait for graceful shutdown
        local count=0
        while kill -0 "${SIDDHI_PID}" 2>/dev/null && [[ ${count} -lt 30 ]]; do
            log_info "Waiting for Siddhi to shutdown gracefully... (${count}/30)"
            sleep 1
            ((count++))
        done
        
        # Force kill if still running
        if kill -0 "${SIDDHI_PID}" 2>/dev/null; then
            log_warn "Forcefully terminating Siddhi"
            kill -KILL "${SIDDHI_PID}" 2>/dev/null || true
        fi
    fi
    
    log_info "Shutdown completed"
    exit 0
}

# Set up signal handlers
trap graceful_shutdown SIGTERM SIGINT SIGQUIT

# Main execution
main() {
    local command="${1:-run}"
    shift || true
    
    log_info "Starting Siddhi Docker container"
    log_info "Command: ${command}"
    log_info "Node ID: ${NODE_ID:-$(hostname)}"
    log_info "Cluster Mode: ${CLUSTER_ENABLED:-false}"
    
    # Run pre-flight checks
    preflight_checks
    
    case "${command}" in
        "run")
            log_info "Starting Siddhi in standalone mode"
            
            # Generate configuration
            generate_config
            
            # Wait for dependencies
            wait_for_dependencies
            
            # Deploy applications
            deploy_apps
            
            # Start Siddhi runner
            log_info "Starting Siddhi runtime..."
            
            # Check if there are any Siddhi files to process
            if ls /opt/siddhi/apps/*.siddhi 1> /dev/null 2>&1; then
                # Process the first available Siddhi file
                local siddhi_file=$(ls /opt/siddhi/apps/*.siddhi | head -1)
                log_info "Processing Siddhi file: $(basename ${siddhi_file})"
                exec /opt/siddhi/bin/siddhi-runner \
                    --config "/tmp/siddhi-generated.yaml" \
                    "${siddhi_file}" \
                    "$@" &
            else
                log_warn "No Siddhi files found in /opt/siddhi/apps/, starting with a default empty query"
                # Create a minimal default query
                echo '@app:name("default-app")
define stream InputStream (message string);
from InputStream 
select message
insert into OutputStream;' > /tmp/default.siddhi
                
                exec /opt/siddhi/bin/siddhi-runner \
                    --config "/tmp/siddhi-generated.yaml" \
                    /tmp/default.siddhi \
                    "$@" &
            fi
            
            SIDDHI_PID=$!
            log_info "Siddhi started with PID ${SIDDHI_PID}"
            
            # Wait for the process
            wait "${SIDDHI_PID}"
            ;;
            
        "worker")
            log_info "Starting Siddhi in worker mode"
            
            # Set worker-specific defaults
            export CLUSTER_ENABLED="true"
            export RUNTIME_MODE="distributed-worker"
            
            # Generate configuration
            generate_config
            
            # Wait for dependencies
            wait_for_dependencies
            
            # Start as worker
            log_info "Starting Siddhi worker..."
            
            # Workers process applications the same way as standalone
            if ls /opt/siddhi/apps/*.siddhi 1> /dev/null 2>&1; then
                local siddhi_file=$(ls /opt/siddhi/apps/*.siddhi | head -1)
                log_info "Worker processing Siddhi file: $(basename ${siddhi_file})"
                exec /opt/siddhi/bin/siddhi-runner \
                    --config "/tmp/siddhi-generated.yaml" \
                    "${siddhi_file}" \
                    "$@" &
            else
                log_warn "No Siddhi files found for worker, using default query"
                echo '@app:name("worker-app")
define stream WorkerInputStream (data string);
from WorkerInputStream 
select data
insert into WorkerOutputStream;' > /tmp/worker.siddhi
                
                exec /opt/siddhi/bin/siddhi-runner \
                    --config "/tmp/siddhi-generated.yaml" \
                    /tmp/worker.siddhi \
                    "$@" &
            fi
            
            SIDDHI_PID=$!
            log_info "Siddhi worker started with PID ${SIDDHI_PID}"
            wait "${SIDDHI_PID}"
            ;;
            
        "manager")
            log_info "Starting Siddhi in manager mode"
            
            # Set manager-specific defaults
            export CLUSTER_ENABLED="true"
            export RUNTIME_MODE="distributed-manager"
            
            # Generate configuration
            generate_config
            
            # Wait for dependencies
            wait_for_dependencies
            
            # Deploy applications
            deploy_apps
            
            # Start as manager
            log_info "Starting Siddhi manager..."
            
            # Managers coordinate applications
            if ls /opt/siddhi/apps/*.siddhi 1> /dev/null 2>&1; then
                local siddhi_file=$(ls /opt/siddhi/apps/*.siddhi | head -1)
                log_info "Manager processing Siddhi file: $(basename ${siddhi_file})"
                exec /opt/siddhi/bin/siddhi-runner \
                    --config "/tmp/siddhi-generated.yaml" \
                    "${siddhi_file}" \
                    "$@" &
            else
                log_warn "No Siddhi files found for manager, using default coordination query"
                echo '@app:name("manager-app")
define stream ManagerInputStream (command string);
from ManagerInputStream 
select command
insert into ManagerOutputStream;' > /tmp/manager.siddhi
                
                exec /opt/siddhi/bin/siddhi-runner \
                    --config "/tmp/siddhi-generated.yaml" \
                    /tmp/manager.siddhi \
                    "$@" &
            fi
            
            SIDDHI_PID=$!
            log_info "Siddhi manager started with PID ${SIDDHI_PID}"
            wait "${SIDDHI_PID}"
            ;;
            
        "deploy")
            log_info "Deploying applications via API"
            
            local app_file="${1}"
            if [[ -z "${app_file}" ]]; then
                log_error "Usage: deploy <app_file>"
                exit 1
            fi
            
            if [[ ! -f "${app_file}" ]]; then
                log_error "Application file not found: ${app_file}"
                exit 1
            fi
            
            # Wait for API to be ready
            local api_url="http://localhost:${API_PORT:-8006}"
            log_info "Waiting for Siddhi API at ${api_url}"
            
            local retries=0
            while [[ ${retries} -lt 30 ]]; do
                if curl -f "${api_url}/health" > /dev/null 2>&1; then
                    break
                fi
                log_info "API not ready, waiting... (${retries}/30)"
                sleep 2
                ((retries++))
            done
            
            if [[ ${retries} -eq 30 ]]; then
                log_error "API did not become ready in time"
                exit 1
            fi
            
            # Deploy the application
            log_info "Deploying ${app_file} via API"
            curl -X POST "${api_url}/apps" -F "file=@${app_file}" || {
                log_error "Failed to deploy application"
                exit 1
            }
            
            log_info "Application deployed successfully"
            ;;
            
        "help"|"-h"|"--help")
            echo "Siddhi Docker Container"
            echo ""
            echo "Usage: docker run siddhi-rust [COMMAND] [OPTIONS]"
            echo ""
            echo "Commands:"
            echo "  run              Start Siddhi in standalone mode (default)"
            echo "  worker           Start as distributed worker node"
            echo "  manager          Start as distributed manager node"
            echo "  deploy <file>    Deploy application via API"
            echo "  help             Show this help message"
            echo ""
            echo "Environment Variables:"
            echo "  NODE_ID                 Node identifier (default: hostname)"
            echo "  CLUSTER_ENABLED         Enable cluster mode (default: false)"
            echo "  SEED_NODES              Comma-separated list of seed nodes"
            echo "  PERSISTENCE_ENABLED     Enable state persistence (default: true)"
            echo "  PERSISTENCE_TYPE        Persistence type: file|redis (default: file)"
            echo "  METRICS_ENABLED         Enable metrics (default: true)"
            echo "  SIDDHI_THREADS          Number of processing threads (default: CPU cores)"
            echo "  SIDDHI_PROPERTIES       Additional config properties (key=value;...)"
            echo ""
            echo "Ports:"
            echo "  8080                    Health check endpoint"
            echo "  8006                    HTTP API"
            echo "  7000                    Cluster communication"
            echo "  9090                    Prometheus metrics"
            ;;
            
        *)
            log_error "Unknown command: ${command}"
            log_info "Use 'help' for usage information"
            exit 1
            ;;
    esac
}

# Execute main function with all arguments
main "$@"