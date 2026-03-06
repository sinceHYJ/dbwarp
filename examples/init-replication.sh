#!/bin/bash
set -e

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 等待所有数据库完全就绪
echo "Waiting for all databases to be ready..."
sleep 5

# 配置主从复制的函数
setup_replication() {
    local master_host=$1
    local slave_host=$2

    echo "Setting up replication: $master_host -> $slave_host"

    # 在主库创建复制用户
    mysql -h"$master_host" -uroot -proot123 -e "
        CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY 'repl123';
        GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
        FLUSH PRIVILEGES;
    " 2>/dev/null || echo "Replication user may already exist on $master_host"

    # 等待一下确保用户创建完成
    sleep 2

    # 在从库配置复制
    mysql -h"$slave_host" -uroot -proot123 -e "
        STOP REPLICA;
        RESET REPLICA ALL;
        CHANGE REPLICATION SOURCE TO
            SOURCE_HOST='$master_host',
            SOURCE_USER='repl',
            SOURCE_PASSWORD='repl123',
            SOURCE_AUTO_POSITION=1;
        START REPLICA;
    " 2>/dev/null

    # 检查复制状态
    sleep 2
    echo "Checking replication status for $slave_host:"
    mysql -h"$slave_host" -uroot -proot123 -e "SHOW REPLICA STATUS\G" 2>/dev/null | grep -E "Replica_IO_Running|Replica_SQL_Running" || true

    echo "Replication $master_host -> $slave_host configured successfully!"
    echo ""
}

# 同步表结构到从库的函数
sync_table_structures() {
    local slave_host=$1
    
    echo "Creating tables on $slave_host..."
    
    # 复用 init/init.sql 文件创建表结构
    mysql -h"$slave_host" -uroot -proot123 test_db < "$SCRIPT_DIR/init/init.sql" 2>/dev/null
    
    echo "Tables created on $slave_host successfully!"
}

# 配置所有主从关系
echo "Starting replication setup..."
echo "================================"

setup_replication "mysql-1" "mysql-1-slave"
setup_replication "mysql-2" "mysql-2-slave"
setup_replication "mysql-3" "mysql-3-slave"
setup_replication "mysql-4" "mysql-4-slave"

echo "================================"
echo "All replications configured!"

# 同步表结构到所有从库
echo ""
echo "================================"
echo "Syncing table structures to all slaves..."

sync_table_structures "mysql-1-slave"
sync_table_structures "mysql-2-slave"
sync_table_structures "mysql-3-slave"
sync_table_structures "mysql-4-slave"

echo "================================"
echo "All table structures synced successfully!"