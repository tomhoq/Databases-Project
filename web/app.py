#!/usr/bin/python3
from logging.config import dictConfig

import psycopg
from flask import flash
from flask import Flask
from flask import jsonify
from flask import redirect
from flask import render_template
from flask import request
from flask import url_for
from psycopg.rows import namedtuple_row
from psycopg_pool import ConnectionPool


# postgres://{user}:{password}@{hostname}:{port}/{database-name}
DATABASE_URL = "postgres://db:db@postgres/db"

pool = ConnectionPool(conninfo=DATABASE_URL)
# the pool starts connecting immediately.

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s:%(lineno)s - %(funcName)20s(): %(message)s",
            }
        },
        "handlers": {
            "wsgi": {
                "class": "logging.StreamHandler",
                "stream": "ext://flask.logging.wsgi_errors_stream",
                "formatter": "default",
            }
        },
        "root": {"level": "INFO", "handlers": ["wsgi"]},
    }
)

app = Flask(__name__)
log = app.logger


@app.route("/", methods=("GET",))
@app.route("/products", methods=("GET",))
def product_index():
    """Show all the products, ordered by name alphabetically."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            products = cur.execute(
                """
                SELECT sku, name, description, price, ean
                FROM product
                ORDER BY name ASC;
                """,
                {},
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    # API-like response is returned to clients that request JSON explicitly (e.g., fetch)
    if (
        request.accept_mimetypes["application/json"]
        and not request.accept_mimetypes["text/html"]
    ):
        return jsonify(products)

    return render_template("product/product_index.html", products=products)


@app.route("/orders", methods=("GET",))
def order_index():
    """Show all the unpaid orders, ordered from most recent date."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            orders = cur.execute(
                """
                SELECT order_no, cust_no, date
                FROM orders o
                WHERE NOT EXISTS (
                    SELECT order_no FROM pay p
                    WHERE p.order_no = o.order_no
                )
                ORDER BY date DESC;
                """,
                {},
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    # API-like response is returned to clients that request JSON explicitly (e.g., fetch)
    if (
        request.accept_mimetypes["application/json"]
        and not request.accept_mimetypes["text/html"]
    ):
        return jsonify(orders)

    return render_template("order/order_index.html", orders=orders)


@app.route("/customers", methods=("GET",))
def customer_index():
    """Show all customers, ordered by customer number."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            customers = cur.execute(
                """
                SELECT cust_no, name, email, phone, address
                FROM customer
                ORDER BY cust_no ASC;
                """,
                {},
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    # API-like response is returned to clients that request JSON explicitly (e.g., fetch)
    if (
        request.accept_mimetypes["application/json"]
        and not request.accept_mimetypes["text/html"]
    ):
        return jsonify(customers)

    return render_template("customer/customer_index.html", customers=customers)


@app.route("/suppliers", methods=("GET",))
def supplier_index():
    """Show all the suppliers, ordered by name alphabetically."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            suppliers = cur.execute(
                """
                SELECT TIN, name, address, sku, date
                FROM supplier
                ORDER BY name ASC;
                """,
                {},
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")

    # API-like response is returned to clients that request JSON explicitly (e.g., fetch)
    if (
        request.accept_mimetypes["application/json"]
        and not request.accept_mimetypes["text/html"]
    ):
        return jsonify(suppliers)

    return render_template("supplier/supplier_index.html", suppliers=suppliers)

@app.route("/products/<sku>/update", methods=("POST","GET"))
def product_update(sku):
    """Update the product information."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            product = cur.execute(
                """
                SELECT sku, price, description
                FROM product
                WHERE sku = %(sku)s;
                """,
                    {"sku": sku},
            ).fetchone()
            log.debug(f"Found {cur.rowcount} rows.")

    if request.method == "POST":
        price = request.form["price"]

        error = None

        if not price:
            error = "Price is required."
            if not price.isnumeric():
                error = "Price is required to be numeric."

        if error is not None:
            flash(error)
        else:
            with pool.connection() as conn:
                with conn.cursor(row_factory=namedtuple_row) as cur:
                    cur.execute(
                        """
                        UPDATE product
                        SET price = %(price)s
                        WHERE sku = %(sku)s;
                        """,
                        {"sku": sku, "price": price},
                    )
                conn.commit()
            return redirect(url_for("product_index"))

    return render_template("product/update.html", product=product)


@app.route("/product/product_register", methods=("POST","GET"))
def product_register():
    """Add a new product to the db"""
    if request.method == "POST":
        sku = request.form["sku"]
        name = request.form["name"]
        desc = request.form["description"]
        price = request.form["price"]
        ean = request.form["ean"]
        
        error = None

        if not sku:
            error = "Sku is required."
        if not name:
            error = "Name is required." 
        if not desc:
            desc = None
        if not price:
            error = "Price is required."
        if not price.isnumeric():
            error = "Price is required to be numeric."
        if ean and not ean.isnumeric():
            error = "EAN is required to be numeric."
        if not ean:
            ean = None
        if error is not None:
            flash(error)
            
        else:
            log.debug(f"\n\n\n\n\n\na: %s", ean)
            with pool.connection() as conn:
                with conn.cursor(row_factory=namedtuple_row) as cur:
                    cur.execute(
                        """
                        INSERT INTO product (sku, name, description, price, ean)
                        VALUES (%(sku)s, %(name)s, %(desc)s, %(price)s, %(ean)s);
                        """,
                        {"sku": sku, "name": name, "desc": desc, "price": price, "ean": ean},
                    )
                conn.commit()
            return redirect(url_for("product_index"))
    return render_template("product/product_register.html")
            
        

    


@app.route("/products/<sku>/delete", methods=("POST",))
def product_delete(sku):
    """Delete the account."""
    """TODO acho que isto ta mal"""
    
    # with pool.connection() as conn:
    #     with conn.cursor(row_factory=namedtuple_row) as cur:
    #         cur.execute("""START TRANSACTION;""")

    #         cur.execute(
    #             """
    #             DELETE FROM contains
    #             WHERE sku = %(sku)s;
    #             """,
    #             {"sku": sku},
    #         )

    #         cur.execute(
    #             """
    #             DELETE FROM contains
    #             WHERE sku = %(sku)s;
    #             """,
    #             {"sku": sku},
    #         )

    #         cur.execute(
    #             """
    #             DELETE FROM product
    #             WHERE sku = %(sku)s;
    #             """,
    #             {"sku": sku},
    #         )

    #         cur.execute(""" COMMIT;""")

    #     conn.commit()
        
    return redirect(url_for("product_index"))

@app.route("/ping", methods=("GET",))
def ping():
    log.debug("ping!")
    return jsonify({"message": "pong!", "status": "success"})


if __name__ == "__main__":
    app.run()
