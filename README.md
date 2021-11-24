# README

- recommended java version: openjdk8

## application.conf minimal requirements database

```
spark {
  appName = "SimpleDbApp"
  # optional
  db {
    config {
      "driver": "com.mysql.cj.jdbc.Driver"
      "url": "jdbc:mysql://localhost:3306/sparkdb"
      "user": "ronny"
      "password": "password"
    }
  }
}
```

## (optional) install test mongodb

```yaml
# docker-compose.yml
version: "3.8"
services:
  mongodb:
    image : mongo
    container_name: mongodb
    environment:
    - PUID=1000
    - PGID=1000
    volumes:
    - /Users/rwels/Coding/docker/mongodb:/data/db
    ports:
    - 27017:27017
    restart: unless-stopped
```

## (optional) install test db

[Example DB](https://www.w3resource.com/sql/sql-table.php)

```bash
# download test db image
docker pull ronakkany/spark-test-maria-db:latest
docker run --name sparkdb -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=password -d mysql --default-authentication-plugin=mysql_native_password
docker exec -it <container-id> bash -l

### configure Maria/MySqlDB Deployment ###
# 1 pen & Edit /etc/my.cnf or /etc/mysql/my.cnf, depending on your distro.
# 2 Add skip-grant-tables under [mysqld]
# 3 Restart Mysql
# 4 You should be able to login to mysql now using the below command mysql -u root -p
# 5 Run mysql> flush privileges;
# 6 Set new password by ALTER USER 'root' IDENTIFIED BY 'NewPassword';
# 7 Go back to /etc/my.cnf and remove/comment skip-grant-tables
# 8 Restart Mysql
# 9 Now you will be able to login with the new password mysql -u root -p

# see
# https://medium.com/tech-learn-share/docker-mysql-access-denied-for-user-172-17-0-1-using-password-yes-c5eadad582d3
```

