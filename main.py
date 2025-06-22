import psycopg2
from pymongo import MongoClient
from bson import Binary
import datetime
from datetime import date
import uuid
import logging
import sys
from decimal import Decimal

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

PG_CONFIG = {
    "host": "212.67.13.83",
    "port": 5440,
    "database": "aw",
    "user": "student",
    "password": "Student112"
}

MONGO_URI = "mongodb://root:rootpass@localhost:27017"
MONGO_DB_NAME = "adventureworks"
BATCH_SIZE = 1000

MODULE_PREFIXES = {
    "person": "person_",
    "human_resources": "hr_",
    "production": "prod_",
    "purchasing": "purch_",
    "sales": "sales_"
}

PRIMARY_KEYS = {
    "address_type": "address_type_id",
    "contact_type": "contact_type_id",
    "country_region": "country_region_code",
    "state_province": "state_province_id",
    "address": "address_id",
    "business_entity": "business_entity_id",
    "department": "department_id",
    "shift": "shift_id",
    "product": "product_id",
    "vendor": "business_entity_id",
    "ship_method": "ship_method_id",
    "currency": "currency_code",
    "credit_card": "credit_card_id",
    "culture": "culture_id",
    "location": "location_id",
    "product_category": "product_category_id",
    "product_description": "product_description_id",
    "product_model": "product_model_id",
    "product_photo": "product_photo_id",
    "scrap_reason": "scrap_reason_id",
    "unit_measure": "unit_measure_code",
    "sales_reason": "sales_reason_id",
    "sales_tax_rate": "sales_tax_rate_id",
    "sales_territory": "territory_id",
    "special_offer": "special_offer_id",
    "employee": "business_entity_id",
    "person": "business_entity_id",
    "job_candidate": "job_candidate_id",
    "work_order": "work_order_id",
    "sales_person": "business_entity_id"
}

MODULE_PREFIXES = {
    "person": "person_",
    "human_resources": "hr_",
    "production": "prod_",
    "purchasing": "purch_",
    "sales": "sales_"
}

id_mappings = {}


def convert_value(value):
    """Конвертация типов PostgreSQL в MongoDB-совместимые"""
    try:
        if value is None:
            return None

        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, date):
            return datetime.datetime(value.year, value.month, value.day)
        if isinstance(value, datetime.time):
            return value.isoformat()

        if isinstance(value, (bytes, bytearray, memoryview)):
            return Binary(bytes(value))

        if isinstance(value, uuid.UUID):
            return str(value)
        if isinstance(value, Decimal):
            return float(value)

        return value

    except Exception as e:
        if convert_value.error_count < 10:
            logger.error(f"Error converting value: {value} ({type(value)}) - {str(e)}")
            convert_value.error_count += 1
        return value


convert_value.error_count = 0


def fetch_data(pg_cur, query, params=None):
    """Выполнение SQL-запроса с пагинацией"""
    try:
        pg_cur.execute(query, params)
        columns = [desc[0] for desc in pg_cur.description]

        while True:
            rows = pg_cur.fetchmany(BATCH_SIZE)
            if not rows:
                break

            for row in rows:
                yield {col: convert_value(val) for col, val in zip(columns, row)}

    except Exception as e:
        logger.error(f"Query failed: {query} - {str(e)}")
        raise


def migrate_person(pg_conn, mongo_db):
    """Миграция модуля Person"""
    prefix = MODULE_PREFIXES["person"]
    logger.info(f"Starting Person module migration with prefix: {prefix}")

    collections = [
        "address_type", "contact_type", "country_region",
        "state_province", "address", "business_entity", "phone_number_type"
    ]
    with pg_conn.cursor() as pg_cur:
        for col in collections:
            collection_name = f"{prefix}{col}"
            logger.info(f"Migrating {collection_name}")

            mongo_db[collection_name].delete_many({})

            if col == "address_type":
                query = """
                SELECT 
                    address_type_id,
                    name,
                    row_guid::text AS row_guid,
                    modified_date
                FROM person.address_type
                """
            else:
                query = f"SELECT * FROM person.{col}"

            data = list(fetch_data(pg_cur, query))
            logger.info(f"Found {len(data)} records for {collection_name}")

            if data:
                result = mongo_db[collection_name].insert_many(data)
                pk_name = PRIMARY_KEYS.get(col)

                if pk_name and pk_name in data[0]:
                    id_mappings[col] = {d[pk_name]: id_
                                        for d, id_ in zip(data, result.inserted_ids)}
                    logger.info(f"Created ID mapping for {collection_name} ({len(data)} records)")
                else:
                    logger.warning(f"Skipped ID mapping for {col} - primary key not found")

        collection_name = f"{prefix}persons"
        logger.info(f"Migrating {collection_name} with nested data")
        persons = []

        for person in fetch_data(pg_cur, "SELECT * FROM person.person"):
            entity_id = person["business_entity_id"]

            emails = list(fetch_data(
                pg_cur,
                "SELECT * FROM person.email_address WHERE business_entity_id = %s",
                (entity_id,)
            ))
            person["emails"] = emails

            passwords = list(fetch_data(
                pg_cur,
                "SELECT * FROM person.password WHERE business_entity_id = %s",
                (entity_id,)
            ))
            if passwords:
                person["password"] = passwords[0]

            phones = list(fetch_data(
                pg_cur,
                """
                SELECT pp.*, pnt.name as phone_type 
                FROM person.person_phone pp
                JOIN person.phone_number_type pnt ON pp.phone_number_type_id = pnt.phone_number_type_id
                WHERE pp.business_entity_id = %s
                """,
                (entity_id,)
            ))
            for phone in phones:
                if "phone_type" in phone:
                    phone["phone_type"] = {"name": phone.pop("phone_type")}
                else:
                    phone["phone_type"] = {"name": "Unknown"}
            person["phones"] = phones

            credit_cards = list(fetch_data(
                pg_cur,
                """
                SELECT pcc.*, cc.card_type, cc.card_number 
                FROM sales.person_credit_card pcc
                JOIN sales.credit_card cc ON pcc.credit_card_id = cc.credit_card_id
                WHERE pcc.business_entity_id = %s
                """,
                (entity_id,)
            ))
            person["credit_cards"] = credit_cards

            persons.append(person)

        if persons:
            mongo_db[collection_name].delete_many({})
            result = mongo_db[collection_name].insert_many(persons)
            id_mappings["person"] = {
                p["business_entity_id"]: id_ for p, id_ in zip(persons, result.inserted_ids)
            }
            logger.info(f"Migrated {len(persons)} persons to {collection_name}")

        logger.info("Migrating entity addresses and contacts")

        collection_name = f"{prefix}business_entity_addresses"
        logger.info(f"Migrating {collection_name}")
        entity_addresses = []

        mongo_db[collection_name].delete_many({})
        for addr in fetch_data(pg_cur, "SELECT * FROM person.business_entity_address"):
            if "address" in id_mappings:
                addr["address_ref"] = id_mappings["address"].get(addr["address_id"])
            entity_addresses.append(addr)

        if entity_addresses:
            mongo_db[collection_name].insert_many(entity_addresses)
            logger.info(f"Migrated {len(entity_addresses)} entity addresses to {collection_name}")

        collection_name = f"{prefix}business_entity_contacts"
        logger.info(f"Migrating {collection_name}")
        entity_contacts = []

        mongo_db[collection_name].delete_many({})
        for contact in fetch_data(pg_cur, "SELECT * FROM person.business_entity_contact"):
            if "person" in id_mappings:
                contact["person_ref"] = id_mappings["person"].get(contact["person_id"])
            entity_contacts.append(contact)

        if entity_contacts:
            mongo_db[collection_name].insert_many(entity_contacts)
            logger.info(f"Migrated {len(entity_contacts)} entity contacts to {collection_name}")

        logger.info("Person module completed")


def migrate_hr(pg_conn, mongo_db):
    """Миграция модуля Human Resources""" 
    prefix = MODULE_PREFIXES["human_resources"]
    logger.info(f"Starting HR module migration with prefix: {prefix}")
    with pg_conn.cursor() as pg_cur:
        collections = ["department", "shift", "employee_pay_history", "employee_department_history"]
        for col in collections:
            collection_name = f"{prefix}{col}"
            logger.info(f"Migrating {collection_name}")

            mongo_db[collection_name].delete_many({})

            query = f"SELECT * FROM human_resources.{col}"
            data = list(fetch_data(pg_cur, query))

            if data:
                result = mongo_db[collection_name].insert_many(data)
                pk_name = PRIMARY_KEYS.get(col)

                if pk_name and pk_name in data[0]:
                    id_mappings[col] = {d[pk_name]: id_
                                        for d, id_ in zip(data, result.inserted_ids)}
                    logger.info(f"Created ID mapping for {collection_name} ({len(data)} records)")
                else:
                    logger.warning(f"Skipped ID mapping for {col} - primary key not found")

        collection_name = f"{prefix}employees"
        logger.info(f"Migrating {collection_name} with nested data")
        employees = []

        mongo_db[collection_name].delete_many({})

        for emp in fetch_data(pg_cur, "SELECT * FROM human_resources.employee"):
            entity_id = emp["business_entity_id"]

            if "person" in id_mappings:
                emp["person_ref"] = id_mappings["person"].get(entity_id)

            pay_history = list(fetch_data(
                pg_cur,
                "SELECT * FROM human_resources.employee_pay_history WHERE business_entity_id = %s",
                (entity_id,)
            ))
            emp["pay_history"] = pay_history

            dept_history = []
            for hist in fetch_data(
                    pg_cur,
                    """
                    SELECT edh.*, d.name as department_name, s.name as shift_name 
                    FROM human_resources.employee_department_history edh
                    JOIN human_resources.department d ON edh.department_id = d.department_id
                    JOIN human_resources.shift s ON edh.shift_id = s.shift_id
                    WHERE edh.business_entity_id = %s
                    """,
                    (entity_id,)
            ):
                dept_doc = {
                    "id": hist["department_id"],
                    "name": hist.pop("department_name")
                }
                shift_doc = {
                    "name": hist.pop("shift_name")
                }
                hist["department"] = dept_doc
                hist["shift"] = shift_doc
                dept_history.append(hist)

            emp["department_history"] = dept_history
            employees.append(emp)

        if employees:
            result = mongo_db[collection_name].insert_many(employees)
            id_mappings["employee"] = {
                e["business_entity_id"]: id_ for e, id_ in zip(employees, result.inserted_ids)
            }
            logger.info(f"Migrated {len(employees)} employees to {collection_name}")

        collection_name = f"{prefix}job_candidates"
        logger.info(f"Migrating {collection_name}")
        candidates = []

        mongo_db[collection_name].delete_many({})

        for cand in fetch_data(pg_cur, "SELECT * FROM human_resources.job_candidate"):
            if "employee" in id_mappings:
                cand["employee_ref"] = id_mappings["employee"].get(cand["business_entity_id"])
            candidates.append(cand)

        if candidates:
            mongo_db[collection_name].insert_many(candidates)
            logger.info(f"Migrated {len(candidates)} job candidates to {collection_name}")

        logger.info("HR module completed")


def migrate_production(pg_conn, mongo_db):
    """Миграция модуля Production"""
    prefix = MODULE_PREFIXES["production"]
    logger.info(f"Starting Production module migration with prefix: {prefix}")
    with pg_conn.cursor() as pg_cur:
        collections = [
            "culture", "location", "product_category", "product_subcategory",
            "product_description", "product_model", "product_photo", "scrap_reason",
            "unit_measure", "illustration", "document", "transaction_history",
            "transaction_history_archive", "product_review", "product_cost_history",
            "product_list_price_history", "product_inventory", "product_document",
            "product_model_illustration", "product_model_product_description_culture"
        ]

        for col in collections:
            collection_name = f"{prefix}{col}"
            logger.info(f"Migrating {collection_name}")

            mongo_db[collection_name].delete_many({})

            query = f"SELECT * FROM production.{col}"
            data = list(fetch_data(pg_cur, query))

            if data:
                result = mongo_db[collection_name].insert_many(data)
                pk_name = PRIMARY_KEYS.get(col, f"{col}_id")

                if pk_name in data[0]:
                    id_mappings[col] = {d[pk_name]: id_
                                        for d, id_ in zip(data, result.inserted_ids)}
                    logger.info(f"Created ID mapping for {collection_name} ({len(data)} records)")
                else:
                    logger.warning(f"Skipped ID mapping for {col} - primary key not found")

        collection_name = f"{prefix}products"
        logger.info(f"Migrating {collection_name} with nested data")
        products = []

        mongo_db[collection_name].delete_many({})

        base_products = list(fetch_data(pg_cur, "SELECT * FROM production.product"))
        for prod in base_products:
            product_id = prod["product_id"]

            product_photos = []
            for ppp in fetch_data(
                    pg_cur,
                    """
                    SELECT ppp.*, pp.large_photo, pp.thumb_nail_photo
                    FROM production.product_product_photo ppp
                    JOIN production.product_photo pp ON ppp.product_photo_id = pp.product_photo_id
                    WHERE ppp.product_id = %s
                    """,
                    (product_id,)
            ):
                photo_doc = {
                    "product_photo": {
                        "product_photo_id": ppp["product_photo_id"],
                        "photos": {
                            "large": ppp["large_photo"],
                            "thumbnail": ppp["thumb_nail_photo"]
                        }
                    },
                    "primary": ppp["primary"],
                    "modified_date": ppp["modified_date"]
                }
                product_photos.append(photo_doc)
            prod["photos"] = product_photos

            cost_history = list(fetch_data(
                pg_cur,
                "SELECT * FROM production.product_cost_history WHERE product_id = %s",
                (product_id,)
            ))
            prod["cost_history"] = cost_history

            price_history = list(fetch_data(
                pg_cur,
                "SELECT * FROM production.product_list_price_history WHERE product_id = %s",
                (product_id,)
            ))
            prod["price_history"] = price_history

            reviews = list(fetch_data(
                pg_cur,
                "SELECT * FROM production.product_review WHERE product_id = %s",
                (product_id,)
            ))
            prod["reviews"] = reviews

            if "product_model" in id_mappings:
                prod["product_model_ref"] = id_mappings["product_model"].get(prod["product_model_id"])

            if "product_subcategory" in id_mappings:
                prod["product_subcategory_ref"] = id_mappings["product_subcategory"].get(prod["product_subcategory_id"])

            products.append(prod)

        if products:
            result = mongo_db[collection_name].insert_many(products)
            id_mappings["product"] = {
                p["product_id"]: id_ for p, id_ in zip(products, result.inserted_ids)
            }
            logger.info(f"Migrated {len(products)} products to {collection_name}")

        collection_name = f"{prefix}work_orders"
        logger.info(f"Migrating {collection_name} with routing")
        work_orders = []

        mongo_db[collection_name].delete_many({})

        for wo in fetch_data(pg_cur, "SELECT * FROM production.work_order"):
            order_id = wo["work_order_id"]

            routings = list(fetch_data(
                pg_cur,
                """
                SELECT wor.*, l.name as location_name
                FROM production.work_order_routing wor
                JOIN production.location l ON wor.location_id = l.location_id
                WHERE wor.work_order_id = %s
                """,
                (order_id,)
            ))

            for routing in routings:
                routing["location"] = {
                    "location_id": routing["location_id"],
                    "name": routing.pop("location_name")
                }

            wo["routings"] = routings

            if "product" in id_mappings:
                wo["product_ref"] = id_mappings["product"].get(wo["product_id"])

            work_orders.append(wo)

        if work_orders:
            mongo_db[collection_name].insert_many(work_orders)
            logger.info(f"Migrated {len(work_orders)} work orders to {collection_name}")

        collection_name = f"{prefix}bill_of_materials"
        logger.info(f"Migrating {collection_name}")
        bom_list = []

        mongo_db[collection_name].delete_many({})

        for bom in fetch_data(pg_cur, "SELECT * FROM production.bill_of_materials"):
            if "product" in id_mappings:
                bom["product_assembly_ref"] = id_mappings["product"].get(bom["product_assembly_id"])
                bom["component_ref"] = id_mappings["product"].get(bom["component_id"])
            bom_list.append(bom)

        if bom_list:
            mongo_db[collection_name].insert_many(bom_list)
            logger.info(f"Migrated {len(bom_list)} bill of materials records to {collection_name}")

        logger.info("Production module completed")


def migrate_purchasing(pg_conn, mongo_db):
    """Миграция модуля Purchasing"""
    prefix = MODULE_PREFIXES["purchasing"]
    logger.info(f"Starting Purchasing module migration with prefix: {prefix}")

    with pg_conn.cursor() as pg_cur:
        collections = ["ship_method", "vendor", "product_vendor"]
        for col in collections:
            collection_name = f"{prefix}{col}"
            logger.info(f"Migrating {collection_name}")

            mongo_db[collection_name].delete_many({})

            query = f"SELECT * FROM purchasing.{col}"
            data = list(fetch_data(pg_cur, query))

            if data:
                result = mongo_db[collection_name].insert_many(data)
                pk_name = PRIMARY_KEYS.get(col)

                if pk_name and pk_name in data[0]:
                    id_mappings[col] = {d[pk_name]: id_
                                        for d, id_ in zip(data, result.inserted_ids)}
                    logger.info(f"Created ID mapping for {collection_name} ({len(data)} records)")
                else:
                    logger.warning(f"Skipped ID mapping for {col} - primary key not found")

        collection_name = f"{prefix}purchase_orders"
        logger.info(f"Migrating {collection_name} with nested data")
        orders = []

        mongo_db[collection_name].delete_many({})

        for order in fetch_data(pg_cur, "SELECT * FROM purchasing.purchase_order_header"):
            if "ship_method" in id_mappings and "ship_method_id" in order:
                order["ship_method"] = id_mappings["ship_method"].get(order["ship_method_id"])

            if "employee" in id_mappings:
                order["employee_ref"] = id_mappings["employee"].get(order["employee_id"])
            if "vendor" in id_mappings:
                order["vendor_ref"] = id_mappings["vendor"].get(order["vendor_id"])

            details = list(fetch_data(
                pg_cur,
                "SELECT * FROM purchasing.purchase_order_detail WHERE purchase_order_id = %s",
                (order["purchase_order_id"],)
            ))
            for detail in details:
                if "product" in id_mappings:
                    detail["product_ref"] = id_mappings["product"].get(detail["product_id"])

            order["details"] = details
            orders.append(order)

        if orders:
            mongo_db[collection_name].insert_many(orders)
            logger.info(f"Migrated {len(orders)} purchase orders to {collection_name}")

        logger.info("Purchasing module completed")


def migrate_sales(pg_conn, mongo_db):
    """Миграция модуля Sales"""
    prefix = MODULE_PREFIXES["sales"]
    logger.info(f"Starting Sales module migration with prefix: {prefix}")
    with pg_conn.cursor() as pg_cur:
        collections = [
            "credit_card", "currency", "sales_reason", "sales_tax_rate",
            "sales_territory", "special_offer", "country_region_currency",
            "currency_rate", "shopping_cart_item", "store", "sales_person",
            "sales_person_quota_history", "sales_territory_history",
            "special_offer_product", "sales_order_header_sales_reason"
        ]

        for col in collections:
            collection_name = f"{prefix}{col}"
            logger.info(f"Migrating {collection_name}")

            mongo_db[collection_name].delete_many({})

            query = f"SELECT * FROM sales.{col}"
            data = list(fetch_data(pg_cur, query))

            if data:
                result = mongo_db[collection_name].insert_many(data)
                pk_name = PRIMARY_KEYS.get(col)

                if pk_name and pk_name in data[0]:
                    id_mappings[col] = {d[pk_name]: id_
                                        for d, id_ in zip(data, result.inserted_ids)}
                    logger.info(f"Created ID mapping for {collection_name} ({len(data)} records)")
                else:
                    logger.warning(f"Skipped ID mapping for {col} - primary key not found")

        collection_name = f"{prefix}sales_persons"
        logger.info(f"Migrating {collection_name} with nested data")
        sales_persons = []

        mongo_db[collection_name].delete_many({})

        for sp in fetch_data(pg_cur, "SELECT * FROM sales.sales_person"):
            entity_id = sp["business_entity_id"]

            quota_history = list(fetch_data(
                pg_cur,
                "SELECT * FROM sales.sales_person_quota_history WHERE business_entity_id = %s",
                (entity_id,)
            ))
            sp["quota_history"] = quota_history

            territory_history = list(fetch_data(
                pg_cur,
                """
                SELECT sth.*, st.name as territory_name
                FROM sales.sales_territory_history sth
                JOIN sales.sales_territory st ON sth.territory_id = st.territory_id
                WHERE sth.business_entity_id = %s
                """,
                (entity_id,)
            ))
            for th in territory_history:
                th["territory"] = {
                    "territory_id": th["territory_id"],
                    "name": th.pop("territory_name")
                }
            sp["territory_history"] = territory_history

            sales_persons.append(sp)

        if sales_persons:
            result = mongo_db[collection_name].insert_many(sales_persons)
            id_mappings["sales_person"] = {
                sp["business_entity_id"]: id_ for sp, id_ in zip(sales_persons, result.inserted_ids)
            }
            logger.info(f"Migrated {len(sales_persons)} sales persons to {collection_name}")

        collection_name = f"{prefix}sales_orders"
        logger.info(f"Migrating {collection_name} with nested data")
        sales_orders = []

        mongo_db[collection_name].delete_many({})

        for order in fetch_data(pg_cur, "SELECT * FROM sales.sales_order_header"):
            if "person" in id_mappings:
                order["customer_ref"] = id_mappings["person"].get(order["customer_id"])
            if "sales_person" in id_mappings:
                order["sales_person_ref"] = id_mappings["sales_person"].get(order["sales_person_id"])
            if "credit_card" in id_mappings:
                order["credit_card_ref"] = id_mappings["credit_card"].get(order["credit_card_id"])

            details = list(fetch_data(
                pg_cur,
                "SELECT * FROM sales.sales_order_detail WHERE sales_order_id = %s",
                (order["sales_order_id"],)
            ))
            for detail in details:
                if "product" in id_mappings:
                    detail["product_ref"] = id_mappings["product"].get(detail["product_id"])
                if "special_offer" in id_mappings:
                    detail["special_offer_ref"] = id_mappings["special_offer"].get(detail["special_offer_id"])

            order["details"] = details

            sales_reasons = list(fetch_data(
                pg_cur,
                """
                SELECT sohr.*, sr.name as reason_name
                FROM sales.sales_order_header_sales_reason sohr
                JOIN sales.sales_reason sr ON sohr.sales_reason_id = sr.sales_reason_id
                WHERE sohr.sales_order_id = %s
                """,
                (order["sales_order_id"],)
            ))
            for reason in sales_reasons:
                reason["reason"] = {
                    "sales_reason_id": reason["sales_reason_id"],
                    "name": reason.pop("reason_name")
                }
            order["sales_reasons"] = sales_reasons

            sales_orders.append(order)

        if sales_orders:
            mongo_db[collection_name].insert_many(sales_orders)
            logger.info(f"Migrated {len(sales_orders)} sales orders to {collection_name}")

        collection_name = f"{prefix}customers"
        logger.info(f"Migrating {collection_name}")
        customers = []

        mongo_db[collection_name].delete_many({})

        for cust in fetch_data(pg_cur, "SELECT * FROM sales.customer"):
            if "person" in id_mappings:
                cust["person_ref"] = id_mappings["person"].get(cust["person_id"])
            if "store" in id_mappings:
                cust["store_ref"] = id_mappings["store"].get(cust["store_id"])
            if "sales_territory" in id_mappings:
                cust["territory_ref"] = id_mappings["sales_territory"].get(cust["territory_id"])
            customers.append(cust)

        if customers:
            mongo_db[collection_name].insert_many(customers)
            logger.info(f"Migrated {len(customers)} customers to {collection_name}")

        logger.info("Sales module completed")


def create_indexes(mongo_db):
    """Создание индексов после миграции"""
    logger.info("Creating indexes with prefixes")

    try:
        # Person
        mongo_db["person_persons"].create_index("business_entity_id")
        mongo_db["person_business_entity_addresses"].create_index("address_id")
        mongo_db["person_addresses"].create_index("state_province_id")
        mongo_db["person_persons"].create_index("emails.email_address")

        # HR
        mongo_db["hr_employees"].create_index("business_entity_id")
        mongo_db["hr_employees"].create_index("department_history.department.id")
        mongo_db["hr_employees"].create_index("pay_history.rate_change_date")

        # Production
        mongo_db["prod_products"].create_index("product_id")
        mongo_db["prod_products"].create_index("product_subcategory_id")
        mongo_db["prod_products"].create_index("product_model_id")
        mongo_db["prod_work_orders"].create_index("product_id")
        mongo_db["prod_work_orders"].create_index("routings.location.location_id")

        # Purchasing
        mongo_db["purch_purchase_orders"].create_index("vendor_id")
        mongo_db["purch_purchase_orders"].create_index("employee_id")
        mongo_db["purch_purchase_orders"].create_index("details.product_id")

        # Sales
        mongo_db["sales_sales_orders"].create_index("customer_id")
        mongo_db["sales_sales_orders"].create_index("sales_person_id")
        mongo_db["sales_sales_orders"].create_index("details.product_id")
        mongo_db["sales_sales_orders"].create_index("credit_card_id")
        mongo_db["sales_customers"].create_index("person_id")
        mongo_db["sales_sales_persons"].create_index("territory_id")
        mongo_db["sales_sales_persons"].create_index("quota_history.quota_date")

        logger.info("Indexes created successfully")
    except Exception as e:
        logger.error(f"Error creating indexes: {str(e)}")


def main():
    try:
        logger.info("Starting migration process")

        pg_conn = psycopg2.connect(**PG_CONFIG)

        mongo_client = MongoClient(MONGO_URI)
        mongo_db = mongo_client[MONGO_DB_NAME]

        # Сброс счетчика ошибок конвертации
        convert_value.error_count = 0

        migrate_person(pg_conn, mongo_db)
        migrate_hr(pg_conn, mongo_db)
        migrate_production(pg_conn, mongo_db)
        migrate_purchasing(pg_conn, mongo_db)
        migrate_sales(pg_conn, mongo_db)

        create_indexes(mongo_db)
        logger.info("Migration completed successfully")

        return 0
    except Exception as e:
        logger.critical(f"Migration failed: {str(e)}", exc_info=True)
        return 1
    finally:
        if 'pg_conn' in locals(): pg_conn.close()
        if 'mongo_client' in locals(): mongo_client.close()
        logger.info("Database connections closed")


if __name__ == "__main__":
    sys.exit(main())
