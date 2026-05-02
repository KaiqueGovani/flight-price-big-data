# Flight Price Big Data — common operations
# Usage: make <target>   (run `make help` for the list)

COMPOSE   ?= MSYS_NO_PATHCONV=1 docker compose
HDFS_DATA ?= /data
HDFS_USER ?= root
INPUT_CSV  ?= hdfs://namenode:9000$(HDFS_DATA)/itineraries.csv
OUTPUT_DIR ?= hdfs://namenode:9000$(HDFS_DATA)/itineraries.parquet

.PHONY: help up down stop restart ps logs format-namenode \
        upload size hdfs-ls hdfs-rm-parquet \
        convert convert-local verify clean-stack

help:
	@echo "Stack:"
	@echo "  make up                 Start the cluster (HDFS + Spark + Jupyter)"
	@echo "  make down               Stop and remove containers (volumes kept)"
	@echo "  make stop               Stop containers (keep them)"
	@echo "  make restart            Restart all services"
	@echo "  make ps                 Show service status"
	@echo "  make logs s=namenode    Tail logs for one service"
	@echo "  make format-namenode    First-time HDFS format (destructive — wipes namenode volume)"
	@echo ""
	@echo "Data:"
	@echo "  make upload             Upload data/*.csv to HDFS $(HDFS_DATA)/"
	@echo "  make size               Show HDFS usage + cluster capacity"
	@echo "  make hdfs-ls            List HDFS $(HDFS_DATA)/"
	@echo "  make hdfs-rm-parquet    Delete the parquet output from HDFS"
	@echo ""
	@echo "Pipeline:"
	@echo "  make convert-local      Read CSV from data/ (local mount), write Parquet to HDFS — skips upload"
	@echo "  make convert            Read CSV from HDFS, write Parquet to HDFS — needs prior 'make upload'"
	@echo "  make verify             Read parquet back, print row count & schema"
	@echo ""
	@echo "Danger zone:"
	@echo "  make clean-stack        docker compose down -v  (DELETES HDFS DATA)"

# ---------- stack ----------

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

stop:
	$(COMPOSE) stop

restart:
	$(COMPOSE) restart

ps:
	$(COMPOSE) ps

logs:
	@if [ -z "$(s)" ]; then echo "Usage: make logs s=<service>"; exit 1; fi
	$(COMPOSE) logs -f --tail=100 $(s)

# Run once before the very first `make up`. Wipes the namenode volume.
format-namenode:
	$(COMPOSE) down -v
	$(COMPOSE) run --rm --user root namenode bash -c \
	  "chown -R hadoop:hadoop /tmp/hadoop-hadoop && \
	   su -p hadoop -c '/opt/hadoop/bin/hdfs namenode -format -nonInteractive -force'"

# ---------- data ----------

upload:
	$(COMPOSE) exec namenode /scripts/upload-to-hdfs.sh

size:
	@$(COMPOSE) exec namenode bash -c '\
	  echo "=== $(HDFS_DATA) (recursive) ==="; \
	  hdfs dfs -ls -R -h $(HDFS_DATA); \
	  echo; \
	  echo "=== Top-level totals (size  disk-with-replication  path) ==="; \
	  hdfs dfs -du -h $(HDFS_DATA); \
	  echo; \
	  echo "=== Cluster usage ==="; \
	  hdfs dfsadmin -report | sed -n "1,5p"'

hdfs-ls:
	$(COMPOSE) exec namenode hdfs dfs -ls -R $(HDFS_DATA)

hdfs-rm-parquet:
	$(COMPOSE) exec namenode hdfs dfs -rm -r -skipTrash $(HDFS_DATA)/itineraries.parquet

# ---------- pipeline ----------

convert:
	$(COMPOSE) exec \
	  -e INPUT_CSV=$(INPUT_CSV) \
	  -e OUTPUT_DIR=$(OUTPUT_DIR) \
	  -e HADOOP_USER_NAME=$(HDFS_USER) \
	  spark-master /opt/spark/bin/spark-submit \
	    --master spark://spark-master:7077 \
	    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
	    /scripts/csv-to-parquet.py

# Read CSV from local mount, write Parquet straight to HDFS — avoids the slow
# Windows→WSL bind-mount upload step entirely. ~3x faster than upload+convert.
convert-local:
	$(COMPOSE) exec \
	  -e INPUT_CSV=file:///data/itineraries.csv \
	  -e OUTPUT_DIR=$(OUTPUT_DIR) \
	  -e HADOOP_USER_NAME=$(HDFS_USER) \
	  spark-master /opt/spark/bin/spark-submit \
	    --master spark://spark-master:7077 \
	    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
	    --conf spark.hadoop.dfs.blocksize=268435456 \
	    --conf spark.hadoop.dfs.replication=1 \
	    /scripts/csv-to-parquet.py

verify:
	$(COMPOSE) exec \
	  -e PARQUET_DIR=$(OUTPUT_DIR) \
	  -e HADOOP_USER_NAME=$(HDFS_USER) \
	  spark-master /opt/spark/bin/spark-submit \
	    --master spark://spark-master:7077 \
	    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
	    /scripts/verify-parquet.py

# ---------- danger ----------

clean-stack:
	$(COMPOSE) down -v
