USAGE:	

zcat file.gz | java -Xmx4g -jar target/kafka-benchmark-jar-with-dependencies.jar --read-buffer-in-kb 1024 --topic rt-test --records-per-message 1 --sampling 100 --producer-props acks=1 bootstrap.servers=$SERVER_INSTANCE batch.size=65536 compression.type=snappy retries=1 request.timeout.ms=60000
