#https://betterprogramming.pub/customize-your-mysql-database-in-docker-723ffd59d8fb
FROM mysql

COPY ./sql-scripts /docker-entrypoint-initdb.d/

EXPOSE 3306
