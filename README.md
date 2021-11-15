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
      "url": "jdbc:mysql://localhost:3305/sparkdb"
      "user": "ronny"
      "password": "password"
    }
  }
}
```

## (optional) install test db

```bash
# download test db image
docker pull ronakkany/spark-test-maria-db:latest
docker run --name budni_dev_local_mysql -p 3306:3306 \
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

