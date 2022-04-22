import json
import operator
import os
import sentry_sdk
import sqlalchemy
from sqlalchemy import create_engine
from utils import weighter
from dotenv import load_dotenv
load_dotenv()

HOST = os.getenv("HOST")
DATABASE = os.getenv("DATABASE")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
FLASK_ENV = os.environ.get("FLASK_ENV")
CLOUD_SQL_CONNECTION_NAME = os.environ.get("CLOUD_SQL_CONNECTION_NAME")

class DatabaseConnectionError (Exception):
    pass

# error type was 'Error' so using the error message here so it's more specific
UNPACK_FROM_ERROR="unpack_from requires a buffer of at least 5 bytes for unpacking 5 bytes at offset"

if FLASK_ENV == "test":
    # Connect via TCP https://cloud.google.com/sql/docs/postgres/connect-overview?_ga=2.75606289.-504393901.1597779336#expandable-1
    print("> ENVIRONMENT test ")
    # ORIGINAL
    db = create_engine('postgresql://' + USERNAME + ':' + PASSWORD + '@' + HOST + ':5432/' + DATABASE)
    
    # URL ACCORDING TO DOCUMENTATION
    # db = create_engine('postgresql+pg8000://' + USERNAME + ':' + PASSWORD + '@' + HOST + ':5432/' + DATABASE)
    
    # FULL CODE EXAMPLE ACCORDING TO DOCUMENTATION
    # db = sqlalchemy.create_engine(
    #     # postgresql+pg8000://<db_user>:<db_pass>@<db_host>:<db_port>/<db_name>
    #     # AttributeError: type object 'URL' has no attribute 'create'
    #     # sqlalchemy.engine.url.URL.create(
    #     sqlalchemy.engine.url.URL( 
    #         drivername="postgresql+pg8000",
    #         username=USERNAME,  # e.g. "my-database-user"
    #         password=PASSWORD,  # e.g. "my-database-password"
    #         host=HOST,  # e.g. "127.0.0.1"
    #         port=5432,  # e.g. 5432
    #         database=DATABASE  # e.g. "my-database-name"
    #     ),
    #     # **db_config
    # )
    print("create_engine COMPLETE")
else:
    # Connect via Unix Sockets https://cloud.google.com/sql/docs/postgres/connect-overview?_ga=2.75606289.-504393901.1597779336#expandable-2
    print("> ENVIRONMENT production ")
    print("> CLOUD_SQL_CONNECTION_NAME", CLOUD_SQL_CONNECTION_NAME)
    db = sqlalchemy.create_engine(
        sqlalchemy.engine.url.URL(
            drivername='postgres+pg8000',
            username=USERNAME,
            password=PASSWORD,
            database=DATABASE,
            query={
                'unix_sock': '/cloudsql/{}/.s.PGSQL.5432'.format(CLOUD_SQL_CONNECTION_NAME)
            }
        )
    )

# N+1 because a sql query for every product n
def get_products():
    results = []
    try:
        with sentry_sdk.start_span(op="get_products", description="db.connect"):
            print(">>>>>>")
            connection = db.connect()
            print(">>>>>>PRODUCTS COMPLETE")

        with sentry_sdk.start_span(op="get_products", description="db.query") as span:
            n = weighter(operator.le, 12)

            products = connection.execute(
                "SELECT *, pg_sleep(%s) FROM products" % (n)
            ).fetchall()
            span.set_tag("totalProducts",len(products))
            span.set_data("products",products)
        
        with sentry_sdk.start_span(op="get_products.reviews", description="db.query") as span:
            for product in products:
                reviews = connection.execute(
                    "SELECT *, pg_sleep(0.25) FROM reviews WHERE productId = {}".format(product.id)
                ).fetchall()
                result = dict(product)
                result["reviews"] = []

                for review in reviews:
                    result["reviews"].append(dict(review))
                results.append(result)
            span.set_data("reviews", results)

        with sentry_sdk.start_span(op="serialization", description="json"):
            result = json.dumps(results, default=str)
        return result
    except BrokenPipeError as err:
        raise DatabaseConnectionError('get_products')
    except Exception as err:
        err_string = str(err)
        if UNPACK_FROM_ERROR in err_string:
            raise DatabaseConnectionError('get_products')
        else:
            raise(err)

# 2 sql queries max, then sort in memory
def get_products_join():
    results = []
    try:
        with sentry_sdk.start_span(op="get_products_join", description="db.connect"):
            connection = db.connect()
        
        with sentry_sdk.start_span(op="get_products_join", description="db.query") as span:
            products = connection.execute(
                "SELECT * FROM products"
            ).fetchall()
            span.set_tag("totalProducts",len(products))
            span.set_data("products",products)

        with sentry_sdk.start_span(op="get_products_join.reviews", description="db.query") as span:
            reviews = connection.execute(
                "SELECT reviews.id, products.id AS productid, reviews.rating, reviews.customerId, reviews.description, reviews.created FROM reviews INNER JOIN products ON reviews.productId = products.id"
            ).fetchall()
            span.set_data("reviews",reviews)
    except BrokenPipeError as err:
        raise DatabaseConnectionError('get_products_join')
    except Exception as err:
        err_string = str(err)
        if UNPACK_FROM_ERROR in err_string:
            raise DatabaseConnectionError('get_products_join')
        else:
            raise(err)

    with sentry_sdk.start_span(op="get_products_join.format_results", description="function") as span:
        for product in products:
            result = dict(product)
            result["reviews"] = []

            for review in reviews:
                productId=review[1]
                if productId == product["id"]:
                    result["reviews"].append(dict(review))
            results.append(result)
        span.set_data("results", results)

    with sentry_sdk.start_span(op="serialization", description="json"):
        result = json.dumps(results, default=str)

    return result

def get_inventory(cart):
    print("> get_inventory")

    quantities = cart['quantities']

    print("> quantities", quantities)

    productIds = []
    for productId in quantities:
        productIds.append(productId)

    productIds = formatArray(productIds)
    print("> productIds", productIds)

    try:
        with sentry_sdk.start_span(op="get_inventory", description="db.connect"):
            connection = db.connect()
        with sentry_sdk.start_span(op="get_inventory", description="db.query") as span:
            inventory = connection.execute(
                "SELECT * FROM inventory WHERE productId in %s" % (productIds)
            ).fetchall()
            span.set_data("inventory",inventory)
    except BrokenPipeError as err:
        raise DatabaseConnectionError('get_inventory')
    except Exception as err:
        err_string = str(err)
        if UNPACK_FROM_ERROR in err_string:
            raise DatabaseConnectionError('get_inventory')
        else:
            raise(err)

    return inventory



def formatArray(ids):
    numbers = ""
    for _id in ids:
        numbers += (_id + ",")
    output = "(" + numbers[:-1] + ")"
    return output