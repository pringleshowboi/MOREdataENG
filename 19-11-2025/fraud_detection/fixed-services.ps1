# PowerShell script to fix and restart services
# Run this from your project root directory

Write-Host "🔧 Starting MLflow and API Server fix..." -ForegroundColor Cyan
Write-Host ""

# Step 1: Stop all services
Write-Host "Step 1: Stopping all services..." -ForegroundColor Yellow
docker-compose down
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Failed to stop services" -ForegroundColor Red
    exit 1
}
Write-Host "✅ Services stopped" -ForegroundColor Green
Write-Host ""

# Step 2: Remove orphaned containers
Write-Host "Step 2: Cleaning up orphaned containers..." -ForegroundColor Yellow
docker-compose down --remove-orphans
Write-Host "✅ Cleanup complete" -ForegroundColor Green
Write-Host ""

# Step 3: Start infrastructure services first
Write-Host "Step 3: Starting infrastructure (PostgreSQL, Redis, Zookeeper)..." -ForegroundColor Yellow
docker-compose up -d postgres redis zookeeper
Write-Host "⏳ Waiting 15 seconds for infrastructure to initialize..." -ForegroundColor Cyan
Start-Sleep -Seconds 15
Write-Host "✅ Infrastructure started" -ForegroundColor Green
Write-Host ""

# Step 4: Start Kafka
Write-Host "Step 4: Starting Kafka..." -ForegroundColor Yellow
docker-compose up -d kafka
Write-Host "⏳ Waiting 20 seconds for Kafka to initialize..." -ForegroundColor Cyan
Start-Sleep -Seconds 20
Write-Host "✅ Kafka started" -ForegroundColor Green
Write-Host ""

# Step 5: Start MLflow
Write-Host "Step 5: Starting MLflow server..." -ForegroundColor Yellow
docker-compose up -d mlflow-server
Write-Host "⏳ Waiting 30 seconds for MLflow to initialize..." -ForegroundColor Cyan
Start-Sleep -Seconds 30
Write-Host "✅ MLflow started" -ForegroundColor Green
Write-Host ""

# Step 6: Initialize Airflow
Write-Host "Step 6: Initializing Airflow..." -ForegroundColor Yellow
docker-compose up airflow-init
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Airflow initialization failed" -ForegroundColor Red
    exit 1
}
Write-Host "✅ Airflow initialized" -ForegroundColor Green
Write-Host ""

# Step 7: Start all remaining services
Write-Host "Step 7: Starting all services..." -ForegroundColor Yellow
docker-compose up -d
Write-Host "⏳ Waiting 60 seconds for services to start..." -ForegroundColor Cyan
Start-Sleep -Seconds 60
Write-Host "✅ All services started" -ForegroundColor Green
Write-Host ""

# Step 8: Check health status
Write-Host "Step 8: Checking service health..." -ForegroundColor Yellow
Write-Host ""
docker-compose ps
Write-Host ""

# Step 9: Test endpoints
Write-Host "Step 9: Testing endpoints..." -ForegroundColor Yellow
Write-Host ""

Write-Host "Testing MLflow (http://localhost:5000/health)..." -ForegroundColor Cyan
try {
    $mlflowResponse = Invoke-WebRequest -Uri "http://localhost:5000/health" -TimeoutSec 5 -UseBasicParsing
    Write-Host "✅ MLflow is responding" -ForegroundColor Green
} catch {
    Write-Host "⚠️  MLflow not responding yet (may still be starting)" -ForegroundColor Yellow
}
Write-Host ""

Write-Host "Testing API Server (http://localhost:8081/api/v2/version)..." -ForegroundColor Cyan
try {
    $apiResponse = Invoke-WebRequest -Uri "http://localhost:8081/api/v2/version" -TimeoutSec 5 -UseBasicParsing
    Write-Host "✅ API Server is responding" -ForegroundColor Green
} catch {
    Write-Host "⚠️  API Server not responding yet (may need more time - wait 2-3 minutes)" -ForegroundColor Yellow
}
Write-Host ""

Write-Host "Testing Airflow UI (http://localhost:8085/health)..." -ForegroundColor Cyan
try {
    $webResponse = Invoke-WebRequest -Uri "http://localhost:8085/health" -TimeoutSec 5 -UseBasicParsing
    Write-Host "✅ Airflow UI is responding" -ForegroundColor Green
} catch {
    Write-Host "⚠️  Airflow UI not responding yet (may still be starting)" -ForegroundColor Yellow
}
Write-Host ""

# Final instructions
Write-Host "═══════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "🎉 Setup complete!" -ForegroundColor Green
Write-Host "═══════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Wait 2-3 minutes for API Server to become healthy"
Write-Host "2. Check container status: docker-compose ps"
Write-Host "3. View logs: docker-compose logs -f airflow-apiserver"
Write-Host "4. Access Airflow UI: http://localhost:8085"
Write-Host "5. Access MLflow UI: http://localhost:5000"
Write-Host ""
Write-Host "To monitor startup progress:" -ForegroundColor Yellow
Write-Host "  docker-compose logs -f airflow-apiserver airflow-worker mlflow-server"
Write-Host ""