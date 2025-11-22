# Force rebuild Airflow with ML packages
Write-Host "🔧 Force rebuilding Airflow with imblearn..." -ForegroundColor Cyan
Write-Host ""

# Step 1: Stop all Airflow services
Write-Host "Step 1: Stopping all Airflow services..." -ForegroundColor Yellow
docker-compose stop airflow-webserver airflow-scheduler airflow-worker airflow-triggerer airflow-dag-processor airflow-apiserver
Write-Host "✅ Airflow services stopped" -ForegroundColor Green
Write-Host ""

# Step 2: Remove Airflow containers
Write-Host "Step 2: Removing Airflow containers..." -ForegroundColor Yellow
docker-compose rm -f airflow-webserver airflow-scheduler airflow-worker airflow-triggerer airflow-dag-processor airflow-apiserver
Write-Host "✅ Containers removed" -ForegroundColor Green
Write-Host ""

# Step 3: Remove old Airflow images
Write-Host "Step 3: Removing old Airflow images..." -ForegroundColor Yellow
docker images --format "{{.Repository}}:{{.Tag}} {{.ID}}" | Select-String "airflow|fraud" | ForEach-Object {
    $parts = $_ -split '\s+'
    $imageId = $parts[1]
    if ($imageId) {
        Write-Host "  Removing: $($parts[0])" -ForegroundColor Gray
        docker rmi -f $imageId 2>$null
    }
}
Write-Host "✅ Old images removed" -ForegroundColor Green
Write-Host ""

# Step 4: Verify requirements.txt
Write-Host "Step 4: Checking airflow/requirements.txt..." -ForegroundColor Yellow
if (Test-Path "./airflow/requirements.txt") {
    $content = Get-Content "./airflow/requirements.txt" -Raw
    if ($content -match "imbalanced-learn") {
        Write-Host "✅ imbalanced-learn found in requirements.txt" -ForegroundColor Green
    } else {
        Write-Host "❌ imbalanced-learn NOT found in requirements.txt!" -ForegroundColor Red
        Write-Host "Add this line: imbalanced-learn>=0.11.0" -ForegroundColor Yellow
        exit 1
    }
} else {
    Write-Host "❌ ./airflow/requirements.txt not found!" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Step 5: Build with no cache
Write-Host "Step 5: Building Airflow image (5-10 minutes)..." -ForegroundColor Yellow
docker-compose build --no-cache --progress=plain airflow-webserver 2>&1 | Tee-Object -Variable buildOutput
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Build failed!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Common issues:" -ForegroundColor Yellow
    Write-Host "  - Not enough disk space (need 10GB+)"
    Write-Host "  - Not enough memory (need 8GB+ for Docker)"
    Write-Host "  - Network issues downloading packages"
    Write-Host ""
    Write-Host "Check Docker Desktop Settings > Resources" -ForegroundColor Yellow
    exit 1
}
Write-Host "✅ Image built successfully" -ForegroundColor Green
Write-Host ""

# Step 6: Test the new image
Write-Host "Step 6: Testing imblearn in new image..." -ForegroundColor Yellow
$imageName = (docker images --format "{{.Repository}}:{{.Tag}}" | Select-String "fraud.*airflow" | Select-Object -First 1).ToString().Trim()
if (-not $imageName) {
    $imageName = "apache/airflow:3.1.3"
}
Write-Host "  Using image: $imageName" -ForegroundColor Gray

$testResult = docker run --rm $imageName python -c "import imblearn; print('OK')" 2>&1
if ($testResult -match "OK") {
    Write-Host "✅ imblearn imports successfully in image" -ForegroundColor Green
} else {
    Write-Host "❌ imblearn import failed in image!" -ForegroundColor Red
    Write-Host $testResult -ForegroundColor Red
    Write-Host ""
    Write-Host "The package didn't install. Check if requirements.txt is being copied correctly." -ForegroundColor Yellow
    exit 1
}
Write-Host ""

# Step 7: Restart Airflow services
Write-Host "Step 7: Starting Airflow services..." -ForegroundColor Yellow
docker-compose up -d airflow-webserver airflow-scheduler airflow-worker airflow-triggerer airflow-dag-processor airflow-apiserver
Write-Host "⏳ Waiting 60 seconds for services to start..." -ForegroundColor Cyan
Start-Sleep -Seconds 60
Write-Host "✅ Services started" -ForegroundColor Green
Write-Host ""

# Step 8: Verify in running worker
Write-Host "Step 8: Verifying imblearn in running worker..." -ForegroundColor Yellow
Start-Sleep -Seconds 10

$workerName = (docker ps --format "{{.Names}}" | Select-String "airflow-worker" | Select-Object -First 1).ToString().Trim()
if ($workerName) {
    Write-Host "  Testing in: $workerName" -ForegroundColor Gray
    $workerTest = docker exec $workerName python -c "import imblearn; print('✅ Worker OK')" 2>&1
    if ($workerTest -match "OK") {
        Write-Host "✅ Worker can import imblearn" -ForegroundColor Green
    } else {
        Write-Host "❌ Worker cannot import imblearn!" -ForegroundColor Red
        Write-Host $workerTest -ForegroundColor Red
        Write-Host ""
        Write-Host "Check installed packages:" -ForegroundColor Yellow
        Write-Host "  docker exec $workerName pip list | Select-String 'imbalanced'" -ForegroundColor Gray
    }
} else {
    Write-Host "⚠️  Worker container not found yet" -ForegroundColor Yellow
}
Write-Host ""

# Final status
Write-Host "═══════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "🎉 Rebuild complete!" -ForegroundColor Green
Write-Host "═══════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Go to Airflow UI: http://localhost:8085"
Write-Host "2. Refresh the DAGs page (F5)"
Write-Host "3. Check if fd_pipeline.py loads without errors"
Write-Host "4. Trigger the DAG"
Write-Host ""
Write-Host "If still getting errors:" -ForegroundColor Yellow
Write-Host "  docker exec $workerName pip list | Select-String 'imbalanced'"
Write-Host "  docker-compose logs airflow-worker | Select-String 'imblearn'"
Write-Host ""