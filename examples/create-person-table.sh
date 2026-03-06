#!/bin/bash

# 在 mysql-2, mysql-3, mysql-4 容器中创建 person 表

CONTAINERS=("dbwarp-mysql-1" "dbwarp-mysql-2" "dbwarp-mysql-3" "dbwarp-mysql-4")
DB="test_db"
USER="test_user"
PASSWORD="test123"

DROP_SQL="DROP TABLE IF EXISTS person;"

CREATE_SQL="CREATE TABLE person (
  id bigint unsigned NOT NULL AUTO_INCREMENT,
  age int NOT NULL DEFAULT '0',
  name varchar(100) NOT NULL DEFAULT '',
  sex varchar(10) NOT NULL DEFAULT '',
  birthday datetime DEFAULT NULL,
  id_no decimal(13,0) DEFAULT NULL COMMENT '身份证号',
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"

for container in "${CONTAINERS[@]}"; do
    echo "Recreating person table in $container..."
    docker exec -i "$container" mysql -u"$USER" -p"$PASSWORD" "$DB" -e "$DROP_SQL"
    docker exec -i "$container" mysql -u"$USER" -p"$PASSWORD" "$DB" -e "$CREATE_SQL"
    if [ $? -eq 0 ]; then
        echo "✓ $container: person table created successfully"
    else
        echo "✗ $container: failed to create person table"
    fi
done

echo "Done!"
