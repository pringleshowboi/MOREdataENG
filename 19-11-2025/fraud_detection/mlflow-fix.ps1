# Final MLflow fix - Host header issue
Write-Host "🔧 Fixing MLflow Host header issue..." -ForegroundColor Cyan
Write-Host ""

# Step 1: Verify docker-compose.yml has the fix
Write-Host "Step 1: Checking docker-compose.yml configuration..." -ForegroundColor Yellow
$composeContent = Get-Content "./docker-compose.yml" -Raw
if ($composeContent -match "forwarded-allow-ips") {
    Write-Host "✅ docker-compose.yml has the fix" -ForegroundColor Green
} else {
    Write-Host "❌ docker-compose.yml is missing the fix!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Your mlflow-server section must have this command:" -ForegroundColor Yellow
    Write-Host @"
    command: >
      sh -c "pip install gunicorn &&
      mlflow server
      --host 0.0.0.0
      --port 5000
      --backend-store-uri postgresql+psycopg2://airflow:airflow@postgres/airflow_mlflow
      --default-artifact-root wasbs://\`${AZURE_CONTAINER_NAME:-mlflow}@\`${AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/
      --serve-artifacts
      --gunicorn-opts '--timeout 120 --worker-class sync --forwarded-allow-ips=* --access-logfile -'"
"@ -ForegroundColor Gray
    Write-Host ""
    Write-Host "Update docker-compose.yml first, then run this script again." -ForegroundColor Yellow
    exit 1
}
Write-Host ""

# Step 2: Stop MLflow
Write-Host "Step 2: Stopping MLflow..." -ForegroundColor Yellow
docker-compose stop mlflow-server
docker-compose rm -f mlflow-server
Write-Host "✅ MLflow stopped and removed" -ForegroundColor Green
Write-Host ""

# Step 3: Rebuild MLflow (in case Dockerfile changed)
Write-Host "Step 3: Rebuilding MLflow image..." -ForegroundColor Yellow
docker-compose build --no-cache mlflow-server
Write-Host "✅ MLflow rebuilt" -ForegroundColor Green
Write-Host ""

# Step 4: Start MLflow
Write-Host "Step 4: Starting MLflow with new configuration..." -ForegroundColor Yellow
docker-compose up -d mlflow-server
Write-Host "⏳ Waiting 45 seconds for MLflow to start..." -ForegroundColor Cyan
Start-Sleep -Seconds 45
Write-Host "✅ MLflow started" -ForegroundColor Green
Write-Host ""

# Step 5: Check if Gunicorn is running
Write-Host "Step 5: Verifying Gunicorn is running..." -ForegroundColor Yellow
$gunicornCheck = docker exec fraud_detection-mlflow-server-1 ps aux 2>$null | Select-String "gunicorn"
if ($gunicornCheck) {
    Write-Host "✅ Gunicorn is running!" -ForegroundColor Green
    Write-Host "  Process: $($gunicornCheck -replace '\s+', ' ')" -ForegroundColor Gray
    
    # Check for forwarded-allow-ips
    $allowIpsCheck = docker exec fraud_detection-mlflow-server-1 ps aux 2>$null | Select-String "forwarded-allow-ips"
    if ($allowIpsCheck) {
        Write-Host "✅ --forwarded-allow-ips is configured" -ForegroundColor Green
    } else {
        Write-Host "⚠️  --forwarded-allow-ips not found in process args" -ForegroundColor Yellow
    }
} else {
    Write-Host "❌ Gunicorn NOT running - still using Werkzeug!" -ForegroundColor Red
    Write-Host ""
    Write-Host "MLflow processes:" -ForegroundColor Yellow
    docker exec fraud_detection-mlflow-server-1 ps aux 2>$null | Select-String "mlflow"
    Write-Host ""
    Write-Host "Check MLflow logs:" -ForegroundColor Yellow
    Write-Host "  docker-compose logs mlflow-server | Select-String -Pattern 'gunicorn|error'" -ForegroundColor Gray
    exit 1
}
Write-Host ""

# Step 6: Test MLflow health
Write-Host "Step 6: Testing MLflow health endpoint..." -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://localhost:5000/health" -TimeoutSec 10 -UseBasicParsing
    Write-Host "✅ MLflow health check passed: $($response.StatusCode)" -ForegroundColor Green
} catch {
    Write-Host "⚠️  MLflow health check failed (may still be starting)" -ForegroundColor Yellow
    Write-Host "  Error: $($_.Exception.Message)" -ForegroundColor Gray
}
Write-Host ""

# Step 7: Test from worker
Write-Host "Step 7: Testing connection from Airflow worker..." -ForegroundColor Yellow
$workerTest = @"
python -c "
import requests
try:
    response = requests.get('http://mlflow-server:5000/health', timeout=5)
    print(f'✅ Status: {response.status_code}')
except Exception as e:
    print(f'❌ Error: {e}')
"
"@

$result = docker exec fraud_detection-airflow-worker-1 bash -c $workerTest 2>&1
Write-Host $result
if ($result -match "✅") {
    Write-Host "✅ Worker can connect to MLflow!" -ForegroundColor Green
} else {
    Write-Host "❌ Worker cannot connect yet" -ForegroundColor Red
}
Write-Host ""

# Step 8: Test MLflow API endpoint
Write-Host "Step 8: Testing MLflow API endpoint..." -ForegroundColor Yellow
$apiTest = @"
python -c "
import mlflow
mlflow.set_tracking_uri('http://mlflow-server:5000')
try:
    client = mlflow.tracking.MlflowClient()
    experiments = client.search_experiments()
    print('✅ MLflow API works!')
except Exception as e:
    print(f'❌ MLflow API error: {e}')
"
"@

$apiResult = docker exec fraud_detection-airflow-worker-1 bash -c $apiTest 2>&1
Write-Host $apiResult
Write-Host ""

# Final instructions
Write-Host "═══════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "🎉 MLflow fix complete!" -ForegroundColor Green
Write-Host "═══════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "1. Go to Airflow UI: http://localhost:8085"
Write-Host "2. Clear the failed 'train_model' task"
Write-Host "3. Trigger the DAG again"
Write-Host "4. Monitor: docker-compose logs -f airflow-worker mlflow-server"
Write-Host ""
Write-Host "If still getting 'Invalid Host header' error:" -ForegroundColor Yellow
Write-Host "  docker-compose logs mlflow-server | Select-String 'command\|gunicorn\|error'" -ForegroundColor Gray
Write-Host ""