# How to Run Tests

Standard tests are run just by calling `pytest`.

There are some tests that require connection to existing OracleDB and write access to one of it's schemas.
If this is not available, these tests are simply skipped.

The easiest way how to provide the connection is to spin up a Docker container with OracleDB. To do so:

1. Prepare space for the db files

```bash
mkdir -p ~/tmp/oracle-19c/oradata
sudo chown -R 54321:54321 ~/tmp/oracle-19c/
```

2. Start the container (can take a few minutes)

```bash
docker run --name oracle-19c \
-p 1521:1521 \
-e ORACLE_SID=XE \
-e ORACLE_PWD=abc123 \
-v ~/tmp/oracle-19c/oradata/:/opt/oracle/oradata \
doctorkirk/oracle-19c
```

3. Create user test_user and grant him admin priviledges

```oracle
CREATE USER test_user IDENTIFIED BY 123456
GRANT DBA TO test_user
```

> **Troubleshooting Connection to the Database**
>
> ERROR: Got minus one from a read call
> https://stackoverflow.com/a/73150106
>
> In the oracle-19c container run:
> ```
> echo "DISABLE_OOB=ON" >>/opt/oracle/oradata/dbconfig/XE/sqlnet.ora
> ```

4. Run tests with the following environemntal variables set

```bash
ORACLE_CONNECTION_STRING=oracle+cx_oracle://TEST_USER:123456@localhost:1521/?service_name=XE
ORACLE_SCHEMA=TEST_USER
```
