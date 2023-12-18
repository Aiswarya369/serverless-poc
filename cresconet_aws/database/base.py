"""
A series of database utility functions that can be used to execute create a database session.
"""
import logging
import os
from typing import Union, Tuple, Optional

import sqlalchemy
from sqlalchemy.pool import NullPool

from ..secrets_manager import get_secret_value_dict

# Environment Variables
LOG_LEVEL = os.environ.get("LOG_LEVEL", logging.INFO)

logger = logging.getLogger("cresconet_database")
logger.setLevel(LOG_LEVEL)

# Common JSON Keys
SECRET_KEY_DATABASE = "database"

# Read-write JSON Keys
SECRET_KEY_HOST = "host"
SECRET_KEY_PORT = "port"
SECRET_KEY_USERNAME = "username"
SECRET_KEY_PASSWORD = "password"

# Read-only JSON Keys
SECRET_KEY_RO_HOST = "ro_host"
SECRET_KEY_RO_PORT = "ro_port"
SECRET_KEY_RO_USERNAME = "ro_username"
SECRET_KEY_RO_PASSWORD = "ro_password"


def get_db_engine(secret_name: str,
                  application_name: str,
                  use_rds_proxy: Optional[Union[str, bool]] = True,
                  database_key: str = SECRET_KEY_DATABASE,
                  host_key: str = SECRET_KEY_HOST,
                  port_key: str = SECRET_KEY_PORT,
                  username_key: str = SECRET_KEY_USERNAME,
                  password_key: str = SECRET_KEY_PASSWORD,
                  pool_pre_ping: Optional[Union[str, bool]] = True,
                  pool_recycle: int = 3600,
                  pool_size: int = 5,
                  max_overflow: int = 10,
                  pool_timeout: int = 30) -> sqlalchemy.engine.Engine:
    """
    Creates a database engine to use for database queries.

    :param secret_name:         name of Secret containing database credentials.
    :param application_name:    the name of the 'application' that is using the database connection.
    :param use_rds_proxy:       whether to use RDS proxy.
    :param database_key:        defaults to 'database'. Key for retrieving database name from JSON database Secret
    :param host_key:            defaults to 'host'. Key for retrieving host from JSON database Secret
    :param port_key:            defaults to 'port'. Key for retrieving port from JSON database Secret
    :param username_key:        defaults to 'username'. Key for retrieving username from JSON database Secret
    :param password_key:        defaults to 'password'. Key for retrieving password from JSON database Secret
    :param pool_pre_ping:       indicates if the connection should be tested when being picked up.
    :param pool_recycle:        indicates the timeout (seconds) before the connection is recycled.
    :param pool_size:           the largest number of connections to keep persistently in the pool.
    :param max_overflow:        the number of 'additional' connections allowed in the pool, that are returned when not in use.
    :param pool_timeout:        number of seconds to wait before giving up on returning a connection.
    :return: the database engine.
    """
    # Can pass through either a boolean or string value, and it is converted to a string.
    if use_rds_proxy is None:
        raise ValueError("'use_rds_proxy' must not be None.")

    use_rds_proxy = str(use_rds_proxy).upper() == "TRUE"
    pool_pre_ping = str(pool_pre_ping).upper() == "TRUE"

    db_host, db_name, db_user, db_password, db_port = get_db_config(secret_name=secret_name,
                                                                    database_key=database_key,
                                                                    host_key=host_key,
                                                                    port_key=port_key,
                                                                    username_key=username_key,
                                                                    password_key=password_key)

    connection_string = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    if use_rds_proxy:
        logger.debug("Using RDS proxy...")

        # NullPool = no connection pooling - we want RDS proxy to do the pooling.
        db_engine = sqlalchemy.engine.create_engine(connection_string,
                                                    client_encoding='utf8',
                                                    poolclass=NullPool,
                                                    connect_args={"application_name": application_name})
    else:
        logger.debug("Not using RDS proxy...")

        # QueuePool is default pooling class.
        db_engine = sqlalchemy.engine.create_engine(connection_string,
                                                    client_encoding='utf8',
                                                    pool_pre_ping=pool_pre_ping,
                                                    pool_recycle=pool_recycle,
                                                    pool_size=pool_size,
                                                    max_overflow=max_overflow,
                                                    pool_timeout=pool_timeout,
                                                    connect_args={"application_name": application_name})

    return db_engine


def get_db_config(secret_name: str,
                  database_key: str,
                  host_key: str,
                  port_key: str,
                  username_key: str,
                  password_key: str) -> Tuple[str, str, str, str, int]:
    """
    Fetches database config values from Secrets Manager using DB_SEC_ID.

    :param secret_name: name of Secret containing the database credentials.
    :param database_key: key for retrieving database name from JSON database Secret
    :param host_key: key for retrieving host from JSON database Secret
    :param port_key: key for retrieving port from JSON database Secret
    :param username_key: key for retrieving username from JSON database Secret
    :param password_key: key for retrieving password from JSON database Secret

    :return: a dictionary containing: host, database, username, password, port.
    """
    logger.debug("Getting database configuration from Secret Manager.")
    db_conf: dict = get_secret_value_dict(secret_name)

    return db_conf[host_key], db_conf[database_key], db_conf[username_key], \
           db_conf[password_key], db_conf[port_key]
