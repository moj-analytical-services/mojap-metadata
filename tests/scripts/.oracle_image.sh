docker run -d --name oracle_db \
-p 1521:1521 \
-e ORACLE_RANDOM_PASSWORD="y" \
-e APP_USER=$DB_USER \
-e APP_USER_PASSWORD=$DB_PASSWORD \
gvenzl/oracle-xe