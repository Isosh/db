from collections import defaultdict

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
    "sales_person": "business_entity_id",
    "currency_rate": "currency_rate_id",
    "shopping_cart_item": "shopping_cart_item_id",
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
    prefix = MODULE_PREFIXES["person"]
    logger.info(f"Starting Person module migration with prefix: {prefix}")

    collections = ["country_region", "state_province", "address", "business_entity"]
    with pg_conn.cursor() as pg_cur:
        collection_name = f"{prefix}country_region"
        logger.info(f"Migrating {collection_name}")
        mongo_db[collection_name].delete_many({})
        country_data = []
        for country in fetch_data(pg_cur, "SELECT * FROM person.country_region"):
            currencies = list(fetch_data(
                pg_cur,
                """
                SELECT crc.currency_code, crc.modified_date
                FROM sales.country_region_currency crc
                WHERE crc.country_region_code = %s
                """,
                (country["country_region_code"],)
            ))
            country["currency"] = currencies[0] if currencies else None
            country_data.append(country)

        if country_data:
            result = mongo_db[collection_name].insert_many(country_data)
            id_mappings["country_region"] = {
                d["country_region_code"]: id_ for d, id_ in zip(country_data, result.inserted_ids)
            }
            logger.info(f"Created ID mapping for country_region ({len(country_data)} records)")

        collection_name = f"{prefix}state_province"
        logger.info(f"Migrating {collection_name}")
        mongo_db[collection_name].delete_many({})
        state_data = []
        for state in fetch_data(pg_cur, "SELECT * FROM person.state_province"):
            state["country_region_ref"] = id_mappings["country_region"].get(state["country_region_code"])

            tax_rates = list(fetch_data(
                pg_cur,
                """
                SELECT str.tax_rate, str.name, str.tax_type, str.modified_date
                FROM sales.sales_tax_rate str
                WHERE str.state_province_id = %s
                """,
                (state["state_province_id"],)
            ))
            state["sales_tax_rate"] = tax_rates
            state_data.append(state)

        if state_data:
            result = mongo_db[collection_name].insert_many(state_data)
            id_mappings["state_province"] = {
                d["state_province_id"]: id_ for d, id_ in zip(state_data, result.inserted_ids)
            }
            logger.info(f"Created ID mapping for state_province ({len(state_data)} records)")

        for col in ["address", "business_entity"]:
            collection_name = f"{prefix}{col}"
            logger.info(f"Migrating {collection_name}")
            mongo_db[collection_name].delete_many({})
            query = f"SELECT * FROM person.{col}"
            data = list(fetch_data(pg_cur, query))

            if data:
                result = mongo_db[collection_name].insert_many(data)
                pk_name = PRIMARY_KEYS.get(col)
                if pk_name and pk_name in data[0]:
                    id_mappings[col] = {d[pk_name]: id_ for d, id_ in zip(data, result.inserted_ids)}
                    logger.info(f"Created ID mapping for {collection_name} ({len(data)} records)")
                else:
                    logger.warning(f"Skipped ID mapping for {col} - primary key not found")

        collection_name = f"{prefix}persons"
        logger.info(f"Migrating {collection_name} with nested data")
        persons = []
        mongo_db[collection_name].delete_many({})

        for person in fetch_data(pg_cur, "SELECT * FROM person.person"):
            entity_id = person["business_entity_id"]
            person["business_entity_ref"] = id_mappings["business_entity"].get(entity_id)

            person["emails"] = list(fetch_data(
                pg_cur,
                "SELECT * FROM person.email_address WHERE business_entity_id = %s",
                (entity_id,)
            ))

            person["passwords"] = list(fetch_data(
                pg_cur,
                "SELECT * FROM person.password WHERE business_entity_id = %s",
                (entity_id,)
            ))

            phones = list(fetch_data(
                pg_cur,
                """
                SELECT pp.phone_number, pp.modified_date, pnt.name as phone_type_name
                FROM person.person_phone pp
                         JOIN person.phone_number_type pnt ON pp.phone_number_type_id = pnt.phone_number_type_id
                WHERE pp.business_entity_id = %s
                """,
                (entity_id,)
            ))
            for phone in phones:
                phone["phone_type"] = {"name": phone.pop("phone_type_name")}
            person["phones"] = phones

            credit_cards = list(fetch_data(
                pg_cur,
                """
                SELECT cc.card_type, cc.card_number, pcc.modified_date
                FROM sales.person_credit_card pcc
                         JOIN sales.credit_card cc ON pcc.credit_card_id = cc.credit_card_id
                WHERE pcc.business_entity_id = %s
                """,
                (entity_id,)
            ))
            person["credit_cards"] = credit_cards
            persons.append(person)

        if persons:
            result = mongo_db[collection_name].insert_many(persons)
            id_mappings["person"] = {
                p["business_entity_id"]: id_ for p, id_ in zip(persons, result.inserted_ids)
            }
            logger.info(f"Migrated {len(persons)} persons to {collection_name}")

        collection_name = f"{prefix}business_entity_addresses"
        logger.info(f"Migrating {collection_name}")
        entity_addresses = []
        mongo_db[collection_name].delete_many({})

        for addr in fetch_data(
                pg_cur,
                """
                SELECT bea.*, at.name as address_type_name
                FROM person.business_entity_address bea
                         JOIN person.address_type at ON bea.address_type_id = at.address_type_id
                """
        ):
            addr["address_type"] = {"name": addr.pop("address_type_name")}

            addr["address_ref"] = id_mappings["address"].get(addr["address_id"])
            addr["business_entity_ref"] = id_mappings["business_entity"].get(addr["business_entity_id"])
            entity_addresses.append(addr)

        if entity_addresses:
            mongo_db[collection_name].insert_many(entity_addresses)
            logger.info(f"Migrated {len(entity_addresses)} entity addresses to {collection_name}")

        collection_name = f"{prefix}business_entity_contacts"
        logger.info(f"Migrating {collection_name}")
        entity_contacts = []
        mongo_db[collection_name].delete_many({})

        for contact in fetch_data(
                pg_cur,
                """
                SELECT bec.*, ct.name as contact_type_name
                FROM person.business_entity_contact bec
                         JOIN person.contact_type ct ON bec.contact_type_id = ct.contact_type_id
                """
        ):
            contact["contact_type"] = {"name": contact.pop("contact_type_name")}

            contact["person_ref"] = id_mappings["person"].get(contact["person_id"])
            contact["business_entity_ref"] = id_mappings["business_entity"].get(contact["business_entity_id"])
            entity_contacts.append(contact)

        if entity_contacts:
            mongo_db[collection_name].insert_many(entity_contacts)
            logger.info(f"Migrated {len(entity_contacts)} entity contacts to {collection_name}")

        logger.info("Person module completed")


def migrate_hr(pg_conn, mongo_db):
    prefix = MODULE_PREFIXES["human_resources"]
    logger.info(f"Starting HR module migration with prefix: {prefix}")

    shifts = {}
    with pg_conn.cursor() as pg_cur:
        for shift in fetch_data(pg_cur, "SELECT * FROM human_resources.shift"):
            shifts[shift['shift_id']] = {
                "name": shift["name"],
                "start_time": shift["start_time"],
                "end_time": shift["end_time"],
                "modified_date": shift["modified_date"]
            }

    collections = ["department"]
    with pg_conn.cursor() as pg_cur:
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
                    id_mappings[col] = {d[pk_name]: id_ for d, id_ in zip(data, result.inserted_ids)}
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
                "SELECT rate_change_date, rate, pay_frequency, modified_date "
                "FROM human_resources.employee_pay_history "
                "WHERE business_entity_id = %s",
                (entity_id,)
            ))
            emp["employee_pay_history"] = pay_history

            dept_history = []
            for hist in fetch_data(
                    pg_cur,
                    """
                    SELECT edh.start_date,
                           edh.end_date,
                           edh.modified_date,
                           edh.department_id,
                           edh.shift_id
                    FROM human_resources.employee_department_history edh
                    WHERE edh.business_entity_id = %s
                    """,
                    (entity_id,)
            ):
                if "department" in id_mappings:
                    hist["department_ref"] = id_mappings["department"].get(hist["department_id"])

                shift_id = hist["shift_id"]
                if shift_id in shifts:
                    hist["shift"] = shifts[shift_id]

                for field in ["department_id", "shift_id"]:
                    hist.pop(field, None)

                dept_history.append(hist)

            emp["employee_department_history"] = dept_history
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

def migrate_purchasing(pg_conn, mongo_db):
    prefix = MODULE_PREFIXES["purchasing"]
    logger.info(f"Starting Purchasing module migration with prefix: {prefix}")

    ship_methods = {}
    with pg_conn.cursor() as pg_cur:
        for sm in fetch_data(pg_cur, "SELECT * FROM purchasing.ship_method"):
            ship_methods[sm['ship_method_id']] = sm

    base_collections = ["vendor"]
    with pg_conn.cursor() as pg_cur:
        for col in base_collections:
            collection_name = f"{prefix}{col}"
            logger.info(f"Migrating base collection: {collection_name}")
            mongo_db[collection_name].delete_many({})
            query = f"SELECT * FROM purchasing.{col}"
            data = list(fetch_data(pg_cur, query))

            if data:
                result = mongo_db[collection_name].insert_many(data)
                pk_name = PRIMARY_KEYS.get(col)
                if pk_name and pk_name in data[0]:
                    id_mappings[col] = {d[pk_name]: id_ for d, id_ in zip(data, result.inserted_ids)}
                    logger.info(f"Created ID mapping for {collection_name} ({len(data)} records)")
                else:
                    logger.warning(f"Skipped ID mapping for {col} - primary key not found")

        collection_name = f"{prefix}product_vendor"
        logger.info(f"Migrating {collection_name} with references")
        mongo_db[collection_name].delete_many({})
        product_vendors = []

        for pv in fetch_data(pg_cur, "SELECT * FROM purchasing.product_vendor"):
            if "product" in id_mappings:
                pv["product_ref"] = id_mappings["product"].get(pv["product_id"])
            if "vendor" in id_mappings:
                pv["vendor_ref"] = id_mappings["vendor"].get(pv["business_entity_id"])
            if "unit_measure" in id_mappings:
                pv["unit_measure_ref"] = id_mappings["unit_measure"].get(pv["unit_measure_code"])

            product_vendors.append(pv)

        if product_vendors:
            result = mongo_db[collection_name].insert_many(product_vendors)
            logger.info(f"Migrated {len(product_vendors)} product-vendor relationships")

        collection_name = f"{prefix}purchase_orders"
        logger.info(f"Migrating {collection_name} with nested details and references")
        orders = []
        mongo_db[collection_name].delete_many({})

        for order in fetch_data(pg_cur, "SELECT * FROM purchasing.purchase_order_header"):
            if "employee" in id_mappings:
                employee_ref = id_mappings["employee"].get(order["employee_id"])
                order["employee_ref"] = employee_ref

            if "vendor" in id_mappings:
                order["vendor_ref"] = id_mappings["vendor"].get(order["vendor_id"])

            ship_method_id = order.get("ship_method_id")
            if ship_method_id and ship_method_id in ship_methods:
                ship_method_data = dict(ship_methods[ship_method_id])
                ship_method_data.pop("ship_method_id", None)
                order["ship_method"] = ship_method_data

            for field in ["ship_method_id", "employee_id"]:
                order.pop(field, None)

            details = []
            for detail in fetch_data(
                    pg_cur,
                    "SELECT * FROM purchasing.purchase_order_detail WHERE purchase_order_id = %s",
                    (order["purchase_order_id"],)
            ):
                if "product" in id_mappings:
                    detail["product_ref"] = id_mappings["product"].get(detail["product_id"])
                details.append(detail)

            order["details"] = details
            orders.append(order)

        if orders:
            result = mongo_db[collection_name].insert_many(orders)
            logger.info(f"Migrated {len(orders)} purchase orders with {sum(len(o['details']) for o in orders)} details")

        logger.info("Purchasing module completed")


def migrate_production(pg_conn, mongo_db):
    prefix = MODULE_PREFIXES["production"]
    logger.info(f"Starting Production module migration with prefix: {prefix}")

    scrap_reasons = {}
    product_photos = {}
    with pg_conn.cursor() as pg_cur:
        for sr in fetch_data(pg_cur, "SELECT * FROM production.scrap_reason"):
            scrap_reasons[sr['scrap_reason_id']] = {
                "name": sr["name"],
                "modified_date": sr["modified_date"]
            }

        for pp in fetch_data(pg_cur, "SELECT * FROM production.product_photo"):
            product_photos[pp['product_photo_id']] = {
                "thumb_nail_photo": pp["thumb_nail_photo"],
                "large_photo": pp["large_photo"],
                "modified_date": pp["modified_date"]
            }

    base_collections = [
        "culture", "location", "product_category",
        "product_description", "product_model",
        "unit_measure", "illustration", "scrap_reason"
    ]

    with pg_conn.cursor() as pg_cur:
        for col in base_collections:
            collection_name = f"{prefix}{col}"
            logger.info(f"Migrating base collection: {collection_name}")
            mongo_db[collection_name].delete_many({})
            query = f"SELECT * FROM production.{col}"
            data = list(fetch_data(pg_cur, query))

            if data:
                result = mongo_db[collection_name].insert_many(data)
                pk_name = PRIMARY_KEYS.get(col, f"{col}_id")
                if pk_name in data[0]:
                    id_mappings[col] = {d[pk_name]: id_ for d, id_ in zip(data, result.inserted_ids)}
                    logger.info(f"Created ID mapping for {collection_name} ({len(data)} records)")
                else:
                    logger.warning(f"Skipped ID mapping for {col} - primary key not found")

        collection_name = f"{prefix}document"
        logger.info(f"Migrating {collection_name} with employee references")
        mongo_db[collection_name].delete_many({})
        documents = []

        for doc in fetch_data(pg_cur, "SELECT * FROM production.document"):
            if "employee" in id_mappings and doc["owner"]:
                doc["owner_ref"] = id_mappings["employee"].get(doc["owner"])
            documents.append(doc)

        if documents:
            result = mongo_db[collection_name].insert_many(documents)
            pk_name = PRIMARY_KEYS.get("document", "document_node")
            if pk_name in documents[0]:
                id_mappings["document"] = {
                    d[pk_name]: id_ for d, id_ in zip(documents, result.inserted_ids)
                }
                logger.info(f"Created ID mapping for {collection_name} ({len(documents)} records)")
            else:
                logger.warning(f"Skipped ID mapping for document - primary key '{pk_name}' not found")

        collection_name = f"{prefix}product_subcategories"
        logger.info(f"Migrating {collection_name} with references")
        mongo_db[collection_name].delete_many({})
        subcategories = []

        for subcat in fetch_data(pg_cur, "SELECT * FROM production.product_subcategory"):
            if "product_category" in id_mappings:
                subcat["product_category_ref"] = id_mappings["product_category"].get(subcat["product_category_id"])
            subcategories.append(subcat)

        if subcategories:
            result = mongo_db[collection_name].insert_many(subcategories)
            id_mappings["product_subcategory"] = {
                s["product_subcategory_id"]: id_ for s, id_ in zip(subcategories, result.inserted_ids)
            }
            logger.info(f"Migrated {len(subcategories)} product subcategories")

        collection_name = f"{prefix}products"
        logger.info(f"Migrating {collection_name} with nested data")
        products = []
        mongo_db[collection_name].delete_many({})

        for prod in fetch_data(pg_cur, "SELECT * FROM production.product"):
            if "product_subcategory" in id_mappings and prod["product_subcategory_id"]:
                prod["product_subcategory_ref"] = id_mappings["product_subcategory"].get(prod["product_subcategory_id"])
            if "unit_measure" in id_mappings:
                if prod["size_unit_measure_code"]:
                    prod["size_unit_measure_ref"] = id_mappings["unit_measure"].get(prod["size_unit_measure_code"])
                if prod["weight_unit_measure_code"]:
                    prod["weight_unit_measure_ref"] = id_mappings["unit_measure"].get(prod["weight_unit_measure_code"])
            if "product_model" in id_mappings and prod["product_model_id"]:
                prod["product_model_ref"] = id_mappings["product_model"].get(prod["product_model_id"])

            prod_photos = []
            for ppp in fetch_data(
                    pg_cur,
                    "SELECT * FROM production.product_product_photo WHERE product_id = %s",
                    (prod["product_id"],)
            ):
                photo_id = ppp["product_photo_id"]
                if photo_id in product_photos:
                    photo_data = product_photos[photo_id].copy()
                    photo_data["primary"] = ppp["primary"]
                    prod_photos.append(photo_data)

            prod["photos"] = prod_photos

            cost_history = list(fetch_data(
                pg_cur,
                "SELECT start_date, end_date, standard_cost, modified_date "
                "FROM production.product_cost_history WHERE product_id = %s",
                (prod["product_id"],)
            ))
            prod["product_cost_history"] = cost_history

            products.append(prod)

        if products:
            result = mongo_db[collection_name].insert_many(products)
            id_mappings["product"] = {
                p["product_id"]: id_ for p, id_ in zip(products, result.inserted_ids)
            }
            logger.info(f"Migrated {len(products)} products")

        collection_name = f"{prefix}product_reviews"
        logger.info(f"Migrating {collection_name} with references")
        reviews = []
        mongo_db[collection_name].delete_many({})

        for rev in fetch_data(pg_cur, "SELECT * FROM production.product_review"):
            if "product" in id_mappings:
                rev["product_ref"] = id_mappings["product"].get(rev["product_id"])
            reviews.append(rev)

        if reviews:
            mongo_db[collection_name].insert_many(reviews)
            logger.info(f"Migrated {len(reviews)} product reviews")

        header_collection = f"{prefix}work_order_headers"
        logger.info(f"Migrating {header_collection} with references and nested data")
        work_order_headers = []
        mongo_db[header_collection].delete_many({})

        for wo in fetch_data(pg_cur, "SELECT * FROM production.work_order"):
            if "product" in id_mappings:
                wo["product_ref"] = id_mappings["product"].get(wo["product_id"])

            scrap_reason_id = wo.get("scrap_reason_id")
            if scrap_reason_id and scrap_reason_id in scrap_reasons:
                wo["scrap_reason"] = scrap_reasons[scrap_reason_id]

            work_order_headers.append(wo)

        if work_order_headers:
            result = mongo_db[header_collection].insert_many(work_order_headers)
            id_mappings["work_order"] = {
                w["work_order_id"]: id_ for w, id_ in zip(work_order_headers, result.inserted_ids)
            }
            logger.info(f"Migrated {len(work_order_headers)} work order headers")

        detail_collection = f"{prefix}work_order_routings"
        logger.info(f"Migrating {detail_collection} with references")
        work_order_routings = []
        mongo_db[detail_collection].delete_many({})

        for wor in fetch_data(pg_cur, "SELECT * FROM production.work_order_routing"):
            if "work_order" in id_mappings:
                wor["work_order_ref"] = id_mappings["work_order"].get(wor["work_order_id"])
            if "location" in id_mappings:
                wor["location_ref"] = id_mappings["location"].get(wor["location_id"])
            work_order_routings.append(wor)

        if work_order_routings:
            mongo_db[detail_collection].insert_many(work_order_routings)
            logger.info(f"Migrated {len(work_order_routings)} work order routings")

        collection_name = f"{prefix}bill_of_materials"
        logger.info(f"Migrating {collection_name} with references")
        bom_list = []
        mongo_db[collection_name].delete_many({})

        for bom in fetch_data(pg_cur, "SELECT * FROM production.bill_of_materials"):
            if "product" in id_mappings:
                if bom["product_assembly_id"]:
                    bom["product_assembly_ref"] = id_mappings["product"].get(bom["product_assembly_id"])
                bom["component_ref"] = id_mappings["product"].get(bom["component_id"])
            if "unit_measure" in id_mappings:
                bom["unit_measure_ref"] = id_mappings["unit_measure"].get(bom["unit_measure_code"])
            bom_list.append(bom)

        if bom_list:
            mongo_db[collection_name].insert_many(bom_list)
            logger.info(f"Migrated {len(bom_list)} bill of materials records")

        collection_name = f"{prefix}transaction_histories"
        logger.info(f"Migrating {collection_name} with references")
        transactions = []
        mongo_db[collection_name].delete_many({})

        for th in fetch_data(pg_cur, "SELECT * FROM production.transaction_history"):
            if "product" in id_mappings:
                th["product_ref"] = id_mappings["product"].get(th["product_id"])
            transactions.append(th)

        if transactions:
            mongo_db[collection_name].insert_many(transactions)
            logger.info(f"Migrated {len(transactions)} transaction history records")

        collection_name = f"{prefix}product_inventories"
        logger.info(f"Migrating {collection_name} with references")
        inventories = []
        mongo_db[collection_name].delete_many({})

        for inv in fetch_data(pg_cur, "SELECT * FROM production.product_inventory"):
            if "product" in id_mappings:
                inv["product_ref"] = id_mappings["product"].get(inv["product_id"])
            if "location" in id_mappings:
                inv["location_ref"] = id_mappings["location"].get(inv["location_id"])
            inventories.append(inv)

        if inventories:
            mongo_db[collection_name].insert_many(inventories)
            logger.info(f"Migrated {len(inventories)} product inventory records")

        collection_name = f"{prefix}product_list_price_histories"
        logger.info(f"Migrating {collection_name} with references")
        price_histories = []
        mongo_db[collection_name].delete_many({})

        for pl in fetch_data(pg_cur, "SELECT * FROM production.product_list_price_history"):
            if "product" in id_mappings:
                pl["product_ref"] = id_mappings["product"].get(pl["product_id"])
            price_histories.append(pl)

        if price_histories:
            mongo_db[collection_name].insert_many(price_histories)
            logger.info(f"Migrated {len(price_histories)} product price history records")

        collection_name = f"{prefix}product_model_illustrations"
        logger.info(f"Migrating {collection_name} with references")
        model_illustrations = []
        mongo_db[collection_name].delete_many({})

        for mi in fetch_data(pg_cur, "SELECT * FROM production.product_model_illustration"):
            if "product_model" in id_mappings:
                mi["product_model_ref"] = id_mappings["product_model"].get(mi["product_model_id"])
            if "illustration" in id_mappings:
                mi["illustration_ref"] = id_mappings["illustration"].get(mi["illustration_id"])
            model_illustrations.append(mi)

        if model_illustrations:
            mongo_db[collection_name].insert_many(model_illustrations)
            logger.info(f"Migrated {len(model_illustrations)} model-illustration relationships")

        collection_name = f"{prefix}product_model_description_cultures"
        logger.info(f"Migrating {collection_name} with references")
        model_descriptions = []
        mongo_db[collection_name].delete_many({})

        for mdc in fetch_data(pg_cur, "SELECT * FROM production.product_model_product_description_culture"):
            if "product_model" in id_mappings:
                mdc["product_model_ref"] = id_mappings["product_model"].get(mdc["product_model_id"])
            if "product_description" in id_mappings:
                mdc["product_description_ref"] = id_mappings["product_description"].get(mdc["product_description_id"])
            if "culture" in id_mappings:
                mdc["culture_ref"] = id_mappings["culture"].get(mdc["culture_id"])
            model_descriptions.append(mdc)

        if model_descriptions:
            mongo_db[collection_name].insert_many(model_descriptions)
            logger.info(f"Migrated {len(model_descriptions)} model-description-culture relationships")

        collection_name = f"{prefix}product_documents"
        logger.info(f"Migrating {collection_name} with references")
        product_docs = []
        mongo_db[collection_name].delete_many({})

        for pd in fetch_data(pg_cur, "SELECT * FROM production.product_document"):
            if "product" in id_mappings:
                pd["product_ref"] = id_mappings["product"].get(pd["product_id"])
            if "document" in id_mappings:
                pd["document_ref"] = id_mappings["document"].get(pd["document_node"])
            product_docs.append(pd)

        if product_docs:
            mongo_db[collection_name].insert_many(product_docs)
            logger.info(f"Migrated {len(product_docs)} product-document relationships")

        logger.info("Production module completed")


def migrate_sales(pg_conn, mongo_db):
    prefix = MODULE_PREFIXES["sales"]
    logger.info(f"Starting Sales module migration with prefix: {prefix}")

    special_offer_products = defaultdict(list)
    with pg_conn.cursor() as pg_cur:
        for sop in fetch_data(pg_cur, "SELECT * FROM sales.special_offer_product"):
            special_offer_products[sop['special_offer_id']].append(sop)

    base_collections = [
        "credit_card", "currency", "currency_rate",
        "sales_reason",
        "sales_territory", "special_offer"
    ]

    with pg_conn.cursor() as pg_cur:
        for col in base_collections:
            collection_name = f"{prefix}{col}"
            logger.info(f"Migrating base collection: {collection_name}")
            mongo_db[collection_name].delete_many({})
            query = f"SELECT * FROM sales.{col}"
            data = list(fetch_data(pg_cur, query))

            if data:
                if col == "currency_rate":
                    for rate in data:
                        if "currency" in id_mappings:
                            rate["from_currency_ref"] = id_mappings["currency"].get(rate["from_currency_code"])
                            rate["to_currency_ref"] = id_mappings["currency"].get(rate["to_currency_code"])

                if col == "special_offer":
                    for offer in data:
                        offer_id = offer["special_offer_id"]
                        offer["special_offer_products"] = special_offer_products.get(offer_id, [])

                result = mongo_db[collection_name].insert_many(data)
                pk_name = PRIMARY_KEYS.get(col)
                if pk_name and pk_name in data[0]:
                    id_mappings[col] = {d[pk_name]: id_ for d, id_ in zip(data, result.inserted_ids)}
                    logger.info(f"Created ID mapping for {collection_name} ({len(data)} records)")
                else:
                    logger.warning(f"Skipped ID mapping for {col} - primary key not found")

        if "sales_territory" in id_mappings and "country_region" in id_mappings:
            logger.info("Updating sales territories with country region references")
            for territory in fetch_data(pg_cur, "SELECT territory_id, country_region_code FROM sales.sales_territory"):
                territory_id = territory["territory_id"]
                country_code = territory["country_region_code"]
                if territory_id in id_mappings["sales_territory"] and country_code in id_mappings["country_region"]:
                    mongo_db["sales_sales_territory"].update_one(
                        {"_id": id_mappings["sales_territory"][territory_id]},
                        {"$set": {"country_region_ref": id_mappings["country_region"][country_code]}}
                    )
        collection_name = f"{prefix}shopping_cart_item"
        logger.info(f"Migrating {collection_name} with product references")
        mongo_db[collection_name].delete_many({})
        shopping_cart_items = []

        for item in fetch_data(pg_cur, "SELECT * FROM sales.shopping_cart_item"):
            if "product" in id_mappings and item.get("product_id"):
                item["product_ref"] = id_mappings["product"].get(item["product_id"])
            shopping_cart_items.append(item)

        if shopping_cart_items:
            result = mongo_db[collection_name].insert_many(shopping_cart_items)
            pk_name = PRIMARY_KEYS.get("shopping_cart_item")
            if pk_name and pk_name in shopping_cart_items[0]:
                id_mappings["shopping_cart_item"] = {
                    d[pk_name]: id_ for d, id_ in zip(shopping_cart_items, result.inserted_ids)
                }
                logger.info(f"Created ID mapping for {collection_name} ({len(shopping_cart_items)} records)")
            else:
                logger.warning("Skipped ID mapping for shopping_cart_item - primary key not found")
        collection_name = f"{prefix}stores"
        logger.info(f"Migrating {collection_name} with references")
        stores = []
        mongo_db[collection_name].delete_many({})

        for store in fetch_data(pg_cur, "SELECT * FROM sales.store"):
            if "sales_person" in id_mappings and store["sales_person_id"]:
                store["sales_person_ref"] = id_mappings["sales_person"].get(store["sales_person_id"])
            stores.append(store)

        if stores:
            result = mongo_db[collection_name].insert_many(stores)
            id_mappings["store"] = {
                s["business_entity_id"]: id_ for s, id_ in zip(stores, result.inserted_ids)
            }
            logger.info(f"Migrated {len(stores)} stores")

        collection_name = f"{prefix}sales_persons"
        logger.info(f"Migrating {collection_name} with references and nested data")
        sales_persons = []
        mongo_db[collection_name].delete_many({})

        quotas_by_person = defaultdict(list)
        territory_history_by_person = defaultdict(list)

        for quota in fetch_data(pg_cur, "SELECT * FROM sales.sales_person_quota_history"):
            quotas_by_person[quota["business_entity_id"]].append(quota)

        for th in fetch_data(pg_cur, "SELECT * FROM sales.sales_territory_history"):
            territory_history_by_person[th["business_entity_id"]].append(th)

        for sp in fetch_data(pg_cur, "SELECT * FROM sales.sales_person"):
            entity_id = sp["business_entity_id"]

            if "employee" in id_mappings:
                sp["employee_ref"] = id_mappings["employee"].get(entity_id)
            if "sales_territory" in id_mappings and sp["territory_id"]:
                sp["territory_ref"] = id_mappings["sales_territory"].get(sp["territory_id"])

            sp["quota_history"] = quotas_by_person.get(entity_id, [])
            sp["territory_history"] = territory_history_by_person.get(entity_id, [])

            sales_persons.append(sp)

        if sales_persons:
            result = mongo_db[collection_name].insert_many(sales_persons)
            id_mappings["sales_person"] = {
                sp["business_entity_id"]: id_ for sp, id_ in zip(sales_persons, result.inserted_ids)
            }
            logger.info(f"Migrated {len(sales_persons)} sales persons")

        collection_name = f"{prefix}customers"
        logger.info(f"Migrating {collection_name} with references")
        customers = []
        mongo_db[collection_name].delete_many({})

        for cust in fetch_data(pg_cur, "SELECT * FROM sales.customer"):
            if "person" in id_mappings and cust["person_id"]:
                cust["person_ref"] = id_mappings["person"].get(cust["person_id"])
            if "store" in id_mappings and cust["store_id"]:
                cust["store_ref"] = id_mappings["store"].get(cust["store_id"])
            if "sales_territory" in id_mappings and cust["territory_id"]:
                cust["territory_ref"] = id_mappings["sales_territory"].get(cust["territory_id"])
            customers.append(cust)

        if customers:
            result = mongo_db[collection_name].insert_many(customers)
            id_mappings["customer"] = {
                c["customer_id"]: id_ for c, id_ in zip(customers, result.inserted_ids)
            }
            logger.info(f"Migrated {len(customers)} customers")

        header_collection = f"{prefix}sales_order_headers"
        logger.info(f"Migrating {header_collection} with references and nested reasons")
        order_headers = []
        mongo_db[header_collection].delete_many({})

        reasons_by_order = defaultdict(list)
        for reason in fetch_data(pg_cur, "SELECT * FROM sales.sales_order_header_sales_reason"):
            reasons_by_order[reason["sales_order_id"]].append(reason)

        for header in fetch_data(pg_cur, "SELECT * FROM sales.sales_order_header"):
            order_id = header["sales_order_id"]

            if "customer" in id_mappings:
                header["customer_ref"] = id_mappings["customer"].get(header["customer_id"])
            if "sales_person" in id_mappings and header["sales_person_id"]:
                header["sales_person_ref"] = id_mappings["sales_person"].get(header["sales_person_id"])
            if "sales_territory" in id_mappings and header["territory_id"]:
                header["territory_ref"] = id_mappings["sales_territory"].get(header["territory_id"])
            if "ship_method" in id_mappings:
                header["ship_method_ref"] = id_mappings["ship_method"].get(header["ship_method_id"])
            if "credit_card" in id_mappings and header["credit_card_id"]:
                header["credit_card_ref"] = id_mappings["credit_card"].get(header["credit_card_id"])
            if "address" in id_mappings:
                if header["bill_to_address_id"]:
                    header["bill_to_address_ref"] = id_mappings["address"].get(header["bill_to_address_id"])
                if header["ship_to_address_id"]:
                    header["ship_to_address_ref"] = id_mappings["address"].get(header["ship_to_address_id"])
            if "currency_rate" in id_mappings and header["currency_rate_id"]:
                header["currency_rate_ref"] = id_mappings["currency_rate"].get(header["currency_rate_id"])

            header["sales_reasons"] = []
            for reason in reasons_by_order.get(order_id, []):
                if "sales_reason" in id_mappings:
                    reason_data = {
                        "sales_reason_ref": id_mappings["sales_reason"].get(reason["sales_reason_id"]),
                        "modified_date": reason["modified_date"]
                    }
                    header["sales_reasons"].append(reason_data)

            order_headers.append(header)

        if order_headers:
            result = mongo_db[header_collection].insert_many(order_headers)
            id_mappings["sales_order_header"] = {
                h["sales_order_id"]: id_ for h, id_ in zip(order_headers, result.inserted_ids)
            }
            logger.info(f"Migrated {len(order_headers)} sales order headers")

        detail_collection = f"{prefix}sales_order_details"
        logger.info(f"Migrating {detail_collection} with references")
        order_details = []
        mongo_db[detail_collection].delete_many({})

        for detail in fetch_data(pg_cur, "SELECT * FROM sales.sales_order_detail"):
            if "sales_order_header" in id_mappings:
                detail["sales_order_header_ref"] = id_mappings["sales_order_header"].get(detail["sales_order_id"])
            if "product" in id_mappings:
                detail["product_ref"] = id_mappings["product"].get(detail["product_id"])
            if "special_offer" in id_mappings:
                detail["special_offer_ref"] = id_mappings["special_offer"].get(detail["special_offer_id"])
            order_details.append(detail)

        if order_details:
            mongo_db[detail_collection].insert_many(order_details)
            logger.info(f"Migrated {len(order_details)} sales order details")

        logger.info("Sales module completed")

def create_indexes(mongo_db):
    """Создание индексов после миграции"""
    logger.info("Creating indexes with prefixes")

    try:
        # Person
        mongo_db["person_persons"].create_index("business_entity_id")
        mongo_db["person_business_entity_addresses"].create_index("address_id")
        mongo_db["person_address"].create_index("state_province_id")
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
        pg_cur = pg_conn.cursor()

        mongo_client = MongoClient(MONGO_URI)
        mongo_db = mongo_client[MONGO_DB_NAME]

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
        if 'pg_cur' in locals(): pg_cur.close()
        if 'pg_conn' in locals(): pg_conn.close()
        if 'mongo_client' in locals(): mongo_client.close()
        logger.info("Database connections closed")


if __name__ == "__main__":
    sys.exit(main())