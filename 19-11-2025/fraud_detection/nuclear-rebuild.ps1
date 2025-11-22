# Nuclear option - complete rebuild from scratch
Write-Host "🔥 NUCLEAR REBUILD - Starting fresh..." -ForegroundColor Red
Write-Host ""

# Step 0: Verify files exist
Write-Host "Step 0: Verifying files..." -ForegroundColor Yellow
if (-not (Test-Path "./airflow/Dockerfile")) {
    Write-Host "❌ ./airflow/Dockerfile not found!" -ForegroundColor Red
    exit 1
}
if (-not (Test-Path "./airflow/requirements.txt")) {
    Write-Host "❌ ./airflow/requirements.txt not found!" -ForegroundColor Red
    exit 1
}
$reqContent = Get-Content "./airflow/requirements.txt" -Raw
if ($reqContent -notmatch "imbalanced-learn") {
    Write-Host "❌ imbalanced-learn not in requirements.txt!" -ForegroundColor Red
    exit 1
}
Write-Host "✅ Files verified" -ForegroundColor Green
Write-Host ""

# Step 1: Nuclear stop
Write-Host "Step 1: Stopping and removing EVERYTHING..." -ForegroundColor Yellow
docker-compose down -v --remove-orphans
Write-Host "✅ Stopped" -ForegroundColor Green
Write-Host ""

# Step 2: Remove ALL airflow and fraud_detection images
Write-Host "Step 2: Removing ALL related images..." -ForegroundColor Yellow
docker images --format "{{.Repository}}:{{.Tag}} {{.ID}}" | ForEach-Object {
    if ($_ -match "airflow|fraud|mlflow") {
        $parts = $_ -split '\s+'
        $imageId = $parts[-1]
        Write-Host "  Removing: $($parts[0])" -ForegroundColor Gray
        docker rmi -f $imageId 2>$null
    }
}
Write-Host "✅ Images removed" -ForegroundColor Green
Write-Host ""

# Step 3: Build Airflow image DIRECTLY (not via compose)
Write-Host "Step 3: Building Airflow image directly with Docker..." -ForegroundColor Yellow
Write-Host "  This will take 5-10 minutes..." -ForegroundColor Cyan
docker build -t fraud-detection-airflow:latest --no-cache ./airflow
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Docker build failed!" -ForegroundColor Red
    exit 1
}
Write-Host "✅ Image built" -ForegroundColor Green
Write-Host ""

# Step 4: TEST the image BEFORE starting services
Write-Host "Step 4: Testing imblearn in built image..." -ForegroundColor Yellow
$testResult = docker run --rm fraud-detection-airflow:latest python -c "import imblearn; import mlflow; import sklearn; print('ALL OK')" 2>&1
if ($testResult -match "ALL OK") {
    Write-Host "✅ imblearn imports successfully!" -ForegroundColor Green
    Write-Host $testResult -ForegroundColor Green
} else {
    Write-Host "❌ IMBLEARN IMPORT FAILED IN IMAGE!" -ForegroundColor Red
    Write-Host $testResult -ForegroundColor Red
    Write-Host ""
    Write-Host "Debugging - installed packages:" -ForegroundColor Yellow
    docker run --rm fraud-detection-airflow:latest pip list | Select-String "imbalanced"
    exit 1
}
Write-Host ""

# Step 5: Start infrastructure
Write-Host "Step 5: Starting infrastructure..." -ForegroundColor Yellow
docker-compose up -d postgres redis zookeeper
Start-Sleep -Seconds 15
docker-compose up -d kafka
Start-Sleep -Seconds 20
docker-compose up -d mlflow-server
Start-Sleep -Seconds 30
Write-Host "✅ Infrastructure started" -ForegroundColor Green
Write-Host ""

# Step 6: Init Airflow
Write-Host "Step 6: Initializing Airflow..." -ForegroundColor Yellow
docker-compose up airflow-init
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Init failed!" -ForegroundColor Red
    exit 1
}
Write-Host "✅ Airflow initialized" -ForegroundColor Green
Write-Host ""

# Step 7: Start Airflow services
Write-Host "Step 7: Starting Airflow services..." -ForegroundColor Yellow
docker-compose up -d airflow-scheduler airflow-webserver airflow-worker airflow-triggerer airflow-dag-processor airflow-apiserver
Write-Host "⏳ Waiting 60 seconds..." -ForegroundColor Cyan
Start-Sleep -Seconds 60
Write-Host "✅ Services started" -ForegroundColor Green
Write-Host ""

# Step 8: Verify worker
Write-Host "Step 8: Verifying worker container..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

# Check what image worker is using
$workerImage = docker inspect fraud_detection-airflow-worker-1 --format='{{.Config.Image}}' 2>$null
Write-Host "  Worker using image: $workerImage" -ForegroundColor Cyan

# Test import in worker
$workerTest = docker exec fraud_detection-airflow-worker-1 python -c "import imblearn; print('✅ WORKER OK')" 2>&1
if ($workerTest -match "WORKER OK") {
    Write-Host "✅ Worker can import imblearn!" -ForegroundColor Green
} else {
    Write-Host "❌ WORKER STILL CANNOT IMPORT!" -ForegroundColor Red
    Write-Host $workerTest -ForegroundColor Red
    Write-Host ""
    Write-Host "Debugging worker packages:" -ForegroundColor Yellow
    docker exec fraud_detection-airflow-worker-1 pip list | Select-String "imbalanced"
    Write-Host ""
    Write-Host "Worker is using image: $workerImage" -ForegroundColor Yellow
    exit 1
}
Write-Host ""

# Step 9: Check DAG
Write-Host "Step 9: Checking DAG import..." -ForegroundColor Yellow
Start-Sleep -Seconds 10
$dagLogs = docker-compose logs airflow-scheduler 2>&1 | Select-String "fd_pipeline"
if ($dagLogs -match "error|Error|ERROR|ModuleNotFoundError") {
    Write-Host "⚠️  DAG import errors detected!" -ForegroundColor Red
    Write-Host $dagLogs -ForegroundColor Red
} else {
    Write-Host "✅ DAG appears to be loading" -ForegroundColor Green
}
Write-Host ""

# Final status
Write-Host "═══════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "🎉 Nuclear rebuild complete!" -ForegroundColor Green
Write-Host "═══════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""
Write-Host "Access Airflow UI: http://localhost:8085" -ForegroundColor Yellow
Write-Host "Access MLflow UI: http://localhost:5000" -ForegroundColor Yellow
Write-Host ""
Write-Host "If DAG still has errors, run:" -ForegroundColor Yellow
Write-Host "  docker-compose logs airflow-scheduler | Select-String 'fd_pipeline'" -ForegroundColor Gray
Write-Host ""