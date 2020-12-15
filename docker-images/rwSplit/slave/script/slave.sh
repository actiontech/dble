if [ -n "$MYSQL_REPLICATION_USER" ] && [ -n "$MYSQL_REPLICATION_PASSWORD" ] && [ -n "$MYSQL_MASTER_SERVICE_HOST" ] ; then
    echo "STOP SLAVE;" | "${mysql[@]}"

			if [ "$MASTER_LOG_FILE" -a "$MASTER_LOG_POS" ]; then
				echo "CHANGE MASTER TO master_host='$MYSQL_MASTER_SERVICE_HOST', master_user='$MYSQL_REPLICATION_USER', master_password='$MYSQL_REPLICATION_PASSWORD', master_log_file='$MASTER_LOG_FILE', master_log_pos=$MASTER_LOG_POS ;" | "${mysql[@]}"
			else
 				echo "CHANGE MASTER TO master_host='$MYSQL_MASTER_SERVICE_HOST', master_user='$MYSQL_REPLICATION_USER', master_password='$MYSQL_REPLICATION_PASSWORD' ;" | "${mysql[@]}"
			fi

			echo "START SLAVE;" | "${mysql[@]}"
fi