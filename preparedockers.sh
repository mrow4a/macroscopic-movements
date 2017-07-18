#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker build -t "movements/spark-s3" $DIR/spark-s3

echo
echo "# Clean dockers"
#docker rm -f datastore
docker rm -f s3bucket
docker rm -f master
docker rm -f slave1
docker rm -f slave2

echo
echo "# Create master and 2 slaves"
docker run --restart unless-stopped -d --name master -p 8080:8080 -p 7077:7077 movements/spark-s3 ./start-master
docker run --restart unless-stopped -d --link master --name slave1 movements/spark-s3 ./start-worker
docker run --restart unless-stopped -d --link master --name slave2 movements/spark-s3 ./start-worker

echo
echo "# Created minio S3"
docker run --restart unless-stopped -d -p 9000:9000 --name s3bucket \
  -e "MINIO_ACCESS_KEY=movements" \
  -e "MINIO_SECRET_KEY=movements" \
  minio/minio server /export

docker ps

function hostname_master {
    docker exec -it master hostname -i | tr -d '\015'
}

function hostname_s3 {
    docker exec -it s3bucket hostname -i | tr -d '\015'
}

hostname_s3_port='9000'

echo
echo "# SparkMaster node UI localhost:8080, spark://$(hostname_master):7077"
echo
echo "# Created S3 on http://$(hostname_s3):9000 with user: [movements] and password: [movements]"
