# run_system.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Steel Energy Hub - Sistem Başlatılıyor" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# 1. Docker servisleri başlat
Write-Host "`n[1/7] Docker servisleri başlatılıyor..." -ForegroundColor Yellow
docker compose up -d

# 2. TimescaleDB hazır olana kadar bekle
Write-Host "`n[2/7] TimescaleDB bekleniyor..." -ForegroundColor Yellow
do {
    $status = docker inspect --format='{{.State.Health.Status}}' timescaledb 2>$null
    if ($status -ne "healthy") {
        Write-Host "  Bekleniyor..." -ForegroundColor Gray
        Start-Sleep -Seconds 5
    }
} while ($status -ne "healthy")
Write-Host "  TimescaleDB hazir!" -ForegroundColor Green

# 3. Kafka hazır olana kadar bekle ve topic oluştur
Write-Host "`n[3/7] Kafka bekleniyor..." -ForegroundColor Yellow
$kafkaReady = $false
$attempt = 0
while (-not $kafkaReady -and $attempt -lt 20) {
    $result = docker exec kafka /opt/kafka/bin/kafka-topics.sh `
        --list --bootstrap-server kafka:9092 2>$null
    if ($LASTEXITCODE -eq 0) {
        $kafkaReady = $true
    } else {
        Write-Host "  Kafka henuz hazir degil, bekleniyor..." -ForegroundColor Gray
        Start-Sleep -Seconds 5
        $attempt++
    }
}

if ($kafkaReady) {
    Write-Host "  Kafka hazir! Topic olusturuluyor..." -ForegroundColor Green
    docker exec kafka /opt/kafka/bin/kafka-topics.sh `
        --create `
        --if-not-exists `
        --topic energy_raw `
        --bootstrap-server kafka:9092 `
        --partitions 1 `
        --replication-factor 1
    Write-Host "  energy_raw topic hazir!" -ForegroundColor Green
} else {
    Write-Host "  Kafka baslatilamadi!" -ForegroundColor Red
    exit 1
}

# 4. Dummy data generator
Write-Host "`n[4/7] Dummy data generator baslatiliyor..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", `
    "Write-Host 'DUMMY GENERATOR' -ForegroundColor Cyan; docker exec -it spark_worker python /home/jovyan/work/src/producer/generate_dummy_data.py"
Write-Host "  Dummy generator terminali acildi." -ForegroundColor Green

# 5. Feature pipeline
Write-Host "`n[5/7] Feature pipeline baslatiliyor..." -ForegroundColor Yellow
Start-Sleep -Seconds 10
Start-Process powershell -ArgumentList "-NoExit", "-Command", `
    "Write-Host 'FEATURE PIPELINE' -ForegroundColor Cyan; docker exec -it spark_worker python /home/jovyan/work/src/pipelines/feature_pipeline.py"
Write-Host "  Feature pipeline terminali acildi." -ForegroundColor Green

# 6. Kafka producer
Write-Host "`n[6/7] Kafka producer baslatiliyor..." -ForegroundColor Yellow
Start-Sleep -Seconds 15
Start-Process powershell -ArgumentList "-NoExit", "-Command", `
    "Write-Host 'KAFKA PRODUCER' -ForegroundColor Cyan; docker exec -it spark_worker python /home/jovyan/work/src/producer/energyProducer.py"
Write-Host "  Kafka producer terminali acildi." -ForegroundColor Green

# 7. Spark streaming
Write-Host "`n[7/7] Spark streaming baslatiliyor..." -ForegroundColor Yellow
Start-Sleep -Seconds 5
Start-Process powershell -ArgumentList "-NoExit", "-Command", `
    "Write-Host 'SPARK STREAMING' -ForegroundColor Cyan; docker exec -it spark_worker python /home/jovyan/work/spark/streaming/energy_streaming.py"
Write-Host "  Spark streaming terminali acildi." -ForegroundColor Green

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  Sistem hazir!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Jupyter   : http://localhost:8888" -ForegroundColor White
Write-Host "  Kafka UI  : http://localhost:8090" -ForegroundColor White
Write-Host "  Grafana   : http://localhost:3000" -ForegroundColor White
Write-Host "  pgAdmin   : http://localhost:8085" -ForegroundColor White
Write-Host ""
Write-Host "Durdurmak icin: docker compose down" -ForegroundColor Gray