DB_NAME = "snac-test"
DB_USER = "PG_USERNAME"
DB_PASS = "PG_PASSWORD"

def get_db_uri():
    return "postgresql+psycopg2://%s:%s@/%s" % (DB_USER, DB_PASS, DB_NAME)

def get_db_name():
    return DB_NAME
