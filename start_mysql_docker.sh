#!/bin/bash

docker run --name ticclat-mysql -e MYSQL_ROOT_PASSWORD=root12345 -p 3306:3306 -p 33060:33060 -d mysql --default-authentication-plugin=mysql_native_password