docker run -d --name postgres_db \
-p 5432:5432 \
-e POSTGRES_PASSWORD=$DB_PASSWORD \
postgres