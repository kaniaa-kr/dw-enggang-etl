import os
from dotenv import load_dotenv

load_dotenv()

DW_DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_NAME", "dwh_enggang_khatulistiwa"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "submarine19"),
    "port": os.getenv("DB_PORT", "5432")
}

OLTP_DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("OLTP_DB_NAME", "oltp_enggang_khatulistiwa"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "submarine19"),
    "port": os.getenv("DB_PORT", "5432")
}
