#!/usr/bin/python3
import os
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
from datetime import datetime


# postgres://{user}:{password}@{hostname}:{port}/{database-name}
DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://db:db@postgres/db")

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
@app.route("/product_index", methods=("GET", "POST"))
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
    
    if request.method == "POST":
        quantities = request.form.getlist("qty")
        skus = request.form.getlist("sku")
        cust_no = int(request.form.getlist("cust_no")[0])
        log.debug(f"\n\n\n\n {quantities}")
        log.debug(f"\n\n\n\n {skus}")
        log.debug(f"\n\n\n\n {cust_no}")

        error = None
        
        ok=0
        for q in quantities:
            if q!="0":
                ok=1;
            
        if ok != 1:
            error = "Quantities are required."

        if not cust_no:
            error = "No cust_no given"
        if error is not None:
            flash(error)
            
        else:
            with pool.connection() as conn:
                with conn.cursor(row_factory=namedtuple_row) as cur:
                    cn = cur.execute(
                        """
                        SELECT cust_no FROM customer WHERE cust_no=%(cust_no)s;
                        """,
                        {"cust_no": cust_no},
                    ).fetchone()
                conn.commit()
            if not cn:
                error = "No customer with such number"
                flash(error)
            with pool.connection() as conn:
                with conn.cursor(row_factory=namedtuple_row) as cur:
                    n_orders = cur.execute(
                        """
                        SELECT COUNT(*) as count
                        FROM orders;
                        """,
                        {},
                    ).fetchall()
                    log.debug(f"Found {cur.rowcount} rows.")
            
            order_no = n_orders[0][0] + 1
            current_date = datetime.now().date()
            date = current_date.strftime('%Y-%m-%d')
            
            
            with pool.connection() as conn:
                with conn.cursor(row_factory=namedtuple_row) as cur:
                    cur.execute(
                        """
                        INSERT INTO orders (order_no, cust_no, date)
                        VALUES (%(order_no)s, %(cust_no)s, %(date)s)
                        
                        """,
                        {"order_no": order_no, "cust_no": cust_no, "date": date},
                    )
                    for i in range(len(quantities)):
                        qty = int(quantities[i])
                        sku = skus[i]
                        
                        if qty != 0:
                            cur.execute(
                                """
                                INSERT INTO contains (order_no, SKU, qty)
                                VALUES (%(order_no)s, %(sku)s, %(qty)s)
                                """,
                                {"order_no": order_no, "sku": sku, "qty": qty},
                            )
                conn.commit()
            
            return redirect(url_for("product_index"))
    
    # API-like response is returned to clients that request JSON explicitly (e.g., fetch)
    if (
        request.accept_mimetypes["application/json"]
        and not request.accept_mimetypes["text/html"]
    ):
        return jsonify(products)

    return render_template("product/product_index.html", products=products)


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
        description = request.form["description"]

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
                        SET price = %(price)s,
                        description = %(description)s
                        WHERE sku = %(sku)s;
                        """,
                        {"sku": sku, "description": description, "price": price},
                    )
                conn.commit()
            return redirect(url_for("product_index"))

    return render_template("product/product_update.html", product=product)


@app.route("/product/register", methods=("POST","GET"))
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
            error = "SKU is required."
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
    """Delete the product."""
    
    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute("""START TRANSACTION;""")

            cur.execute(
                """
                DELETE FROM delivery d
                WHERE EXISTS (
                    SELECT * FROM supplier s
                    WHERE s.TIN = d.TIN
                    AND sku = %(sku)s
                );
                """,
                {"sku": sku},
            )

            cur.execute(
                """
                DELETE FROM supplier
                WHERE sku = %(sku)s;
                """,
                {"sku": sku},
            )

            cur.execute(
                """
                DELETE FROM contains
                WHERE sku = %(sku)s;
                """,
                {"sku": sku},
            )

            cur.execute(
                """
                DELETE FROM product
                WHERE sku = %(sku)s;
                """,
                {"sku": sku},
            )

            cur.execute(""" COMMIT;""")

        conn.commit()
    return redirect(url_for("product_index"))


@app.route("/orders", methods=("GET",))
def order_index():
    """Show all the unpaid orders, ordered from most recent date."""

    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            orders = cur.execute(
                """
                SELECT o.order_no, o.cust_no, o.date
                FROM orders o
                WHERE NOT EXISTS (
                    SELECT p.order_no FROM pay p
                    WHERE p.order_no = o.order_no
                )
                ORDER BY o.order_no DESC;
                """,
                {},
            ).fetchall()
            order_products = cur.execute(
                """
                SELECT o.order_no, c.sku, c.qty
                FROM orders o
                INNER JOIN contains c USING (order_no)
                ORDER BY o.order_no DESC;
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

    return render_template("order/order_index.html", orders=orders, order_products=order_products)


@app.route("/orders/<order_no>-<cust_no>/pay", methods=("POST","GET"))
def pay_order(order_no, cust_no):
    log.debug(f" {order_no, cust_no} ")
    
    return redirect(url_for("verify_payment", order_no=order_no, cust_no=cust_no))


@app.route("/orders/<order_no>-<cust_no>/verify_payment", methods=("POST","GET"))
def verify_payment(order_no, cust_no):
    if request.method == "POST":
        user = request.form["cust_no"]
        error = None
        if user!=cust_no:
            error = "This is order does not belong to the user"
        if error:
            flash(error)
        with pool.connection() as conn:
            with conn.cursor(row_factory=namedtuple_row) as cur:
                cur.execute(
                    """
                    INSERT INTO pay (order_no, cust_no)
                    VALUES (%(order_no)s, %(cust_no)s)
                    """,
                    {"order_no": order_no, "cust_no": cust_no},
                )

            conn.commit()

            return redirect(url_for("order_index"))
    return render_template("order/order_payment.html")


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


@app.route("/customer/register", methods=("POST","GET"))
def customer_register():
    """Add a new customer to the db"""
    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            count = cur.execute(
                """
                SELECT COUNT(*) as count
                FROM customer;
                """,
                {},
            ).fetchall()
            log.debug(f"Found {cur.rowcount} rows.")
            log.debug(f"\n\n\n\n {count}")
            
    cust_no = count[0][0] + 1
    
    if request.method == "POST":
        log.debug(f"\n\n\n\n {cust_no}")
        name = request.form["name"]
        email = request.form["email"]
        phone = request.form["phone"]
        address = request.form["address"]
        
        error = None

        if not count:
            error = "Count is required."
        if not name:
            error = "Name is required."
        if not email:
            error = "Email is required."
        if not phone:
            phone = None
        if not address:
            address = None
        if error is not None:
            flash(error)
            
        else:
            with pool.connection() as conn:
                with conn.cursor(row_factory=namedtuple_row) as cur:
                    cur.execute(
                        """
                        INSERT INTO customer (cust_no, name, email, phone, address)
                        VALUES (%(cust_no)s, %(name)s, %(email)s, %(phone)s, %(address)s);
                        """,
                        {"cust_no": cust_no, "name": name, "email": email, 
                         "phone": phone, "address": address},
                    )
                conn.commit()
            return redirect(url_for("customer_index"))
    return render_template("customer/customer_register.html", cust_no=cust_no)


@app.route("/customers/<cust_no>/delete", methods=("POST",))
def customer_delete(cust_no):
    """Delete the customer."""
    
    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute("""START TRANSACTION;""")

            cur.execute(
                """
                DELETE FROM contains c
                WHERE EXISTS (
                    SELECT * FROM orders o
                    WHERE o.order_no = c.order_no
                    AND cust_no = %(cust_no)s
                );
                """,
                {"cust_no": cust_no},
            )

            cur.execute(
                """
                DELETE FROM process p
                WHERE EXISTS (
                    SELECT * FROM orders o
                    WHERE o.order_no = p.order_no
                    AND cust_no = %(cust_no)s
                );
                """,
                {"cust_no": cust_no},
            )

            cur.execute(
                """
                DELETE FROM pay p
                WHERE EXISTS (
                    SELECT * FROM orders o
                    WHERE o.order_no = p.order_no
                    AND cust_no = %(cust_no)s
                );
                """,
                {"cust_no": cust_no},
            )

            cur.execute(
                """
                DELETE FROM pay
                WHERE cust_no = %(cust_no)s;
                """,
                {"cust_no": cust_no},
            )

            cur.execute(
                """
                DELETE FROM orders
                WHERE cust_no = %(cust_no)s;
                """,
                {"cust_no": cust_no},
            )

            cur.execute(
                """
                DELETE FROM customer
                WHERE cust_no = %(cust_no)s;
                """,
                {"cust_no": cust_no},
            )

            cur.execute(""" COMMIT;""")

        conn.commit()
    return redirect(url_for("customer_index"))


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


@app.route("/supplier/register", methods=("POST","GET"))
def supplier_register():
    """Add a new supplier to the db"""
    if request.method == "POST":
        tin = request.form["tin"]
        name = request.form["name"]
        address = request.form["address"]
        sku = request.form["sku"]
        date = request.form["date"]
        
        error = None

        if not tin:
            error = "TIN is required."
        if tin and not tin.isnumeric():
            error = "TIN is required to be numeric."
        if len(tin) != 9:
            error = "TIN must have 9 digits."
        if not name:
            name = None
        if not address:
            address = None
        if not sku:
            error = "SKU is required."
        if not date:
            date = None
        
        with pool.connection() as conn:
            with conn.cursor(row_factory=namedtuple_row) as cur:
                _tin = cur.execute(
                    """
                    SELECT tin FROM supplier WHERE tin=%(tin)s;
                    """,
                    {"tin": tin},
                ).fetchone()
            conn.commit()
        if _tin:
            error = "TIN already exists."
        
        with pool.connection() as conn:
            with conn.cursor(row_factory=namedtuple_row) as cur:
                _sku = cur.execute(
                    """
                    SELECT sku FROM product WHERE sku=%(sku)s;
                    """,
                    {"sku": sku},
                ).fetchone()
            conn.commit()
        if not _sku:
            error = "Product does not exist."

        if error is not None:
            flash(error)    
        else:
            with pool.connection() as conn:
                with conn.cursor(row_factory=namedtuple_row) as cur:
                    cur.execute(
                        """
                        INSERT INTO supplier (tin, name, address, sku, date)
                        VALUES (%(tin)s, %(name)s, %(address)s, %(sku)s, %(date)s);
                        """,
                        {"tin": tin, "name": name, "address": address, "sku": sku, "date": date},
                    )
                conn.commit()
            return redirect(url_for("supplier_index"))
    return render_template("supplier/supplier_register.html")


@app.route("/suppliers/<TIN>/delete", methods=("POST",))
def supplier_delete(TIN):
    """Delete the supplier."""
    
    with pool.connection() as conn:
        with conn.cursor(row_factory=namedtuple_row) as cur:
            cur.execute("""START TRANSACTION;""")

            cur.execute(
                """
                DELETE FROM delivery
                WHERE TIN = %(TIN)s;
                """,
                {"TIN": TIN},
            )

            cur.execute(
                """
                DELETE FROM supplier
                WHERE TIN = %(TIN)s;
                """,
                {"TIN": TIN},
            )

            cur.execute(""" COMMIT;""")

        conn.commit()
    return redirect(url_for("supplier_index"))


@app.route("/ping", methods=("GET",))
def ping():
    log.debug("ping!")
    return jsonify({"message": "pong!", "status": "success"})


if __name__ == "__main__":
    app.run()
