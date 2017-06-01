#!/usr/bin/env bash
if [ ! -f minio ]; then
    echo "Downloading minio"
    wget https://dl.minio.io/server/minio/release/linux-amd64/minio
    chmod +x minio
fi
echo "Starting minio"
export MINIO_ACCESS_KEY=minio_access_key
export MINIO_SECRET_KEY=minio_secret_key
export MINIO_BROWSER=off
mkdir -p tmp
./minio server tmp &
sleep 3

echo "Running test"
./gradlew --rerun-tasks --info test jacocoTestReport

echo "Killing server"
ps ax | grep minio | awk '{print $1}' | head -n 1 | xargs kill
