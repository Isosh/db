import psycopg2
from pymongo import MongoClient
from bson import Binary
import datetime
from datetime import date
import uuid
import logging
import sys
from decimal import Decimal  # Добавлен импорт Decimal

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Конфигурация
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

# Словарь соответствия имен первичных ключей
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
    "special_offer": "special_offer_id"
}

# Глобальные переменные для маппинга ID
id_mappings = {}


def convert_value(value):
    """Конвертация типов PostgreSQL в MongoDB-совместимые"""
    try:
        # Пропускаем None значения
        if value is None:
            return None

        # Обработка даты и времени
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, date):
            return datetime.datetime(value.year, value.month, value.day)
        if isinstance(value, datetime.time):
            return value.isoformat()

        # Обработка бинарных данных
        if isinstance(value, (bytes, bytearray, memoryview)):
            return Binary(bytes(value))

        # Обработка специальных типов
        if isinstance(value, uuid.UUID):
            return str(value)
        if isinstance(value, Decimal):
            return float(value)

        return value

    except Exception as e:
        # Логируем только первые 10 ошибок каждого типа
        if convert_value.error_count < 10:
            logger.error(f"Error converting value: {value} ({type(value)}) - {str(e)}")
            convert_value.error_count += 1
        return value


# Инициализация счетчика ошибок для функции
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


def migrate_person(pg_cur, mongo_db):
    """Миграция модуля Person"""
    logger.info("Starting Person module migration")

    # Миграция основных сущностей
    collections = [
        "address_type", "contact_type", "country_region",
        "state_province", "address", "business_entity"
    ]

    for col in collections:
        logger.info(f"Migrating {col}")
        query = f"SELECT * FROM person.{col}"
        data = list(fetch_data(pg_cur, query))

        if data:
            result = mongo_db[col].insert_many(data)
            pk_name = PRIMARY_KEYS.get(col)

            if pk_name and pk_name in data[0]:
                id_mappings[col] = {d[pk_name]: id_
                                    for d, id_ in zip(data, result.inserted_ids)}
                logger.info(f"Created ID mapping for {col} ({len(data)} records)")
            else:
                logger.warning(f"Skipped ID mapping for {col} - primary key not found")

    # Миграция Person с вложенными документами
    logger.info("Migrating persons with nested data")
    persons = []

    for person in fetch_data(pg_cur, "SELECT * FROM person.person"):
        entity_id = person["business_entity_id"]

        # Вложенные email
        emails = list(fetch_data(
            pg_cur,
            "SELECT * FROM person.email_address WHERE business_entity_id = %s",
            (entity_id,)
        ))
        person["emails"] = emails

        # Вложенные пароли
        passwords = list(fetch_data(
            pg_cur,
            "SELECT * FROM person.password WHERE business_entity_id = %s",
            (entity_id,)
        ))
        if passwords:
            person["password"] = passwords[0]

        # Вложенные телефоны
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
            # Убираем поле phone_type, если оно есть
            if "phone_type" in phone:
                phone["phone_type"] = {"name": phone.pop("phone_type")}
            else:
                phone["phone_type"] = {"name": "Unknown"}
        person["phones"] = phones

        persons.append(person)

    if persons:
        result = mongo_db["persons"].insert_many(persons)
        id_mappings["person"] = {
            p["business_entity_id"]: id_ for p, id_ in zip(persons, result.inserted_ids)
        }
        logger.info(f"Migrated {len(persons)} persons")

    # Миграция связующих таблиц
    logger.info("Migrating entity addresses and contacts")

    # BusinessEntityAddress
    entity_addresses = []
    for addr in fetch_data(pg_cur, "SELECT * FROM person.business_entity_address"):
        if "address" in id_mappings:
            addr["address_ref"] = id_mappings["address"].get(addr["address_id"])
        entity_addresses.append(addr)

    if entity_addresses:
        mongo_db["business_entity_addresses"].insert_many(entity_addresses)
        logger.info(f"Migrated {len(entity_addresses)} entity addresses")

    # BusinessEntityContact
    entity_contacts = []
    for contact in fetch_data(pg_cur, "SELECT * FROM person.business_entity_contact"):
        if "person" in id_mappings:
            contact["person_ref"] = id_mappings["person"].get(contact["person_id"])
        entity_contacts.append(contact)

    if entity_contacts:
        mongo_db["business_entity_contacts"].insert_many(entity_contacts)
        logger.info(f"Migrated {len(entity_contacts)} entity contacts")

    logger.info("Person module completed")


def migrate_hr(pg_cur, mongo_db):
    """Миграция модуля Human Resources"""
    logger.info("Starting HR module migration")

    # Миграция справочников
    collections = ["department", "shift"]
    for col in collections:
        logger.info(f"Migrating {col}")
        query = f"SELECT * FROM human_resources.{col}"
        data = list(fetch_data(pg_cur, query))

        if data:
            result = mongo_db[col].insert_many(data)
            pk_name = PRIMARY_KEYS.get(col)

            if pk_name and pk_name in data[0]:
                id_mappings[col] = {d[pk_name]: id_
                                    for d, id_ in zip(data, result.inserted_ids)}
                logger.info(f"Created ID mapping for {col} ({len(data)} records)")
            else:
                logger.warning(f"Skipped ID mapping for {col} - primary key not found")

    # Миграция сотрудников
    logger.info("Migrating employees with nested data")
    employees = []

    for emp in fetch_data(pg_cur, "SELECT * FROM human_resources.employee"):
        entity_id = emp["business_entity_id"]

        # Ссылка на Person
        if "person" in id_mappings:
            emp["person_ref"] = id_mappings["person"].get(entity_id)

        # Вложенная история зарплат
        pay_history = list(fetch_data(
            pg_cur,
            "SELECT * FROM human_resources.employee_pay_history WHERE business_entity_id = %s",
            (entity_id,)
        ))
        emp["pay_history"] = pay_history

        # Вложенная история департаментов
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
            # Формируем вложенный документ для департамента
            dept_doc = {
                "id": hist["department_id"],
                "name": hist.pop("department_name")
            }
            # Формируем вложенный документ для смены
            shift_doc = {
                "name": hist.pop("shift_name")
            }
            hist["department"] = dept_doc
            hist["shift"] = shift_doc
            dept_history.append(hist)

        emp["department_history"] = dept_history
        employees.append(emp)

    if employees:
        result = mongo_db["employees"].insert_many(employees)
        id_mappings["employee"] = {
            e["business_entity_id"]: id_ for e, id_ in zip(employees, result.inserted_ids)
        }
        logger.info(f"Migrated {len(employees)} employees")

    # Миграция кандидатов
    logger.info("Migrating job candidates")
    candidates = []

    for cand in fetch_data(pg_cur, "SELECT * FROM human_resources.job_candidate"):
        # Ссылка на сотрудника (если есть)
        if "employee" in id_mappings:
            cand["employee_ref"] = id_mappings["employee"].get(cand["business_entity_id"])
        candidates.append(cand)

    if candidates:
        mongo_db["job_candidates"].insert_many(candidates)
        logger.info(f"Migrated {len(candidates)} job candidates")

    logger.info("HR module completed")


def migrate_production(pg_cur, mongo_db):
    """Миграция модуля Production с полной поддержкой вложенных отношений"""
    logger.info("Starting Production module migration")

    # 1. Миграция справочников и независимых коллекций
    collections = [
        "culture", "location", "product_category",
        "product_description", "product_model", "product_photo",
        "scrap_reason", "unit_measure", "illustration", "document"
    ]

    for col in collections:
        logger.info(f"Migrating {col}")
        query = f"SELECT * FROM production.{col}"
        data = list(fetch_data(pg_cur, query))

        if data:
            result = mongo_db[col].insert_many(data)
            pk_name = PRIMARY_KEYS.get(col, f"{col}_id")

            if pk_name in data[0]:
                id_mappings[col] = {d[pk_name]: id_
                                    for d, id_ in zip(data, result.inserted_ids)}
                logger.info(f"Created ID mapping for {col} ({len(data)} records)")
            else:
                logger.warning(f"Skipped ID mapping for {col} - primary key not found")

    # 2. Миграция продуктов с полной вложенной структурой
    logger.info("Migrating products with complete nested structure")
    products = []

    # Сначала получаем все продукты без вложенных данных
    base_products = list(fetch_data(pg_cur, "SELECT * FROM production.product"))

    for prod in base_products:
        product_id = prod["product_id"]

        # Вложенные фото продуктов (embedded как в ERD)
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
            # Преобразуем в структуру как в ERD
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

        # Вложенная история цен (embedded)
        cost_history = list(fetch_data(
            pg_cur,
            "SELECT * FROM production.product_cost_history WHERE product_id = %s",
            (product_id,)
        ))
        prod["cost_history"] = cost_history

        # Вложенная история цен (embedded)
        price_history = list(fetch_data(
            pg_cur,
            "SELECT * FROM production.product_list_price_history WHERE product_id = %s",
            (product_id,)
        ))
        prod["price_history"] = price_history

        # Вложенные отзывы (embedded)
        reviews = list(fetch_data(
            pg_cur,
            "SELECT * FROM production.product_review WHERE product_id = %s",
            (product_id,)
        ))
        prod["reviews"] = reviews

        # Ссылки на связанные документы
        if "product_model" in id_mappings:
            prod["product_model_ref"] = id_mappings["product_model"].get(prod["product_model_id"])

        if "product_subcategory" in id_mappings:
            prod["product_subcategory_ref"] = id_mappings["product_subcategory"].get(prod["product_subcategory_id"])

        products.append(prod)

    if products:
        result = mongo_db["products"].insert_many(products)
        id_mappings["product"] = {
            p["product_id"]: id_ for p, id_ in zip(products, result.inserted_ids)
        }
        logger.info(f"Migrated {len(products)} products with nested data")

    # 3. Миграция WorkOrder с вложенными маршрутами
    logger.info("Migrating work orders with routing")
    work_orders = []

    for wo in fetch_data(pg_cur, "SELECT * FROM production.work_order"):
        order_id = wo["work_order_id"]

        # Вложенные маршруты (embedded как в ERD)
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

        # Преобразуем маршруты согласно ERD
        for routing in routings:
            routing["location"] = {
                "location_id": routing["location_id"],
                "name": routing.pop("location_name")
            }

        wo["routings"] = routings

        # Ссылка на продукт
        if "product" in id_mappings:
            wo["product_ref"] = id_mappings["product"].get(wo["product_id"])

        work_orders.append(wo)

    if work_orders:
        mongo_db["work_orders"].insert_many(work_orders)
        logger.info(f"Migrated {len(work_orders)} work orders with routings")

    # 4. Миграция BillOfMaterials
    logger.info("Migrating bill of materials")
    bom_list = []

    for bom in fetch_data(pg_cur, "SELECT * FROM production.bill_of_materials"):
        # Ссылки на продукты
        if "product" in id_mappings:
            bom["product_assembly_ref"] = id_mappings["product"].get(bom["product_assembly_id"])
            bom["component_ref"] = id_mappings["product"].get(bom["component_id"])

        bom_list.append(bom)

    if bom_list:
        mongo_db["bill_of_materials"].insert_many(bom_list)
        logger.info(f"Migrated {len(bom_list)} bill of materials records")

    logger.info("Production module completed")


def migrate_purchasing(pg_cur, mongo_db):
    """Миграция модуля Purchasing"""
    logger.info("Starting Purchasing module migration")

    # Миграция справочников
    collections = ["ship_method", "vendor"]
    for col in collections:
        logger.info(f"Migrating {col}")
        query = f"SELECT * FROM purchasing.{col}"
        data = list(fetch_data(pg_cur, query))

        if data:
            result = mongo_db[col].insert_many(data)
            pk_name = PRIMARY_KEYS.get(col)

            if pk_name and pk_name in data[0]:
                id_mappings[col] = {d[pk_name]: id_
                                    for d, id_ in zip(data, result.inserted_ids)}
                logger.info(f"Created ID mapping for {col} ({len(data)} records)")
            else:
                logger.warning(f"Skipped ID mapping for {col} - primary key not found")

    # Миграция заказов
    logger.info("Migrating purchase orders with nested data")
    orders = []

    for order in fetch_data(pg_cur, "SELECT * FROM purchasing.purchase_order_header"):
        # Встроенный метод доставки
        if "ship_method" in id_mappings and "ship_method_id" in order:
            order["ship_method"] = id_mappings["ship_method"].get(order["ship_method_id"])

        # Ссылки
        if "employee" in id_mappings:
            order["employee_ref"] = id_mappings["employee"].get(order["employee_id"])
        if "vendor" in id_mappings:
            order["vendor_ref"] = id_mappings["vendor"].get(order["vendor_id"])

        # Вложенные детали заказа
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
        mongo_db["purchase_orders"].insert_many(orders)
        logger.info(f"Migrated {len(orders)} purchase orders")

    # Миграция ProductVendor
    logger.info("Migrating product vendors")
    product_vendors = []

    for pv in fetch_data(pg_cur, "SELECT * FROM purchasing.product_vendor"):
        if "product" in id_mappings:
            pv["product_ref"] = id_mappings["product"].get(pv["product_id"])
        if "vendor" in id_mappings:
            pv["vendor_ref"] = id_mappings["vendor"].get(pv["business_entity_id"])
        product_vendors.append(pv)

    if product_vendors:
        mongo_db["product_vendors"].insert_many(product_vendors)
        logger.info(f"Migrated {len(product_vendors)} product vendors")

    logger.info("Purchasing module completed")


def migrate_sales(pg_cur, mongo_db):
    """Миграция модуля Sales"""
    logger.info("Starting Sales module migration")

    # Миграция справочников
    collections = [
        "credit_card", "currency", "sales_reason",
        "sales_tax_rate", "sales_territory", "special_offer"
    ]

    for col in collections:
        logger.info(f"Migrating {col}")
        query = f"SELECT * FROM sales.{col}"
        data = list(fetch_data(pg_cur, query))

        if data:
            result = mongo_db[col].insert_many(data)
            pk_name = PRIMARY_KEYS.get(col)

            if pk_name and pk_name in data[0]:
                id_mappings[col] = {d[pk_name]: id_
                                    for d, id_ in zip(data, result.inserted_ids)}
                logger.info(f"Created ID mapping for {col} ({len(data)} records)")
            else:
                logger.warning(f"Skipped ID mapping for {col} - primary key not found")

    # Миграция заказов
    logger.info("Migrating sales orders with nested data")
    sales_orders = []

    for order in fetch_data(pg_cur, "SELECT * FROM sales.sales_order_header"):
        # Ссылки
        if "person" in id_mappings:
            order["customer_ref"] = id_mappings["person"].get(order["customer_id"])
        if "employee" in id_mappings:
            order["sales_person_ref"] = id_mappings["employee"].get(order["sales_person_id"])
        if "credit_card" in id_mappings:
            order["credit_card_ref"] = id_mappings["credit_card"].get(order["credit_card_id"])

        # Вложенные детали
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
        sales_orders.append(order)

    if sales_orders:
        mongo_db["sales_orders"].insert_many(sales_orders)
        logger.info(f"Migrated {len(sales_orders)} sales orders")

    # Миграция клиентов
    logger.info("Migrating customers")
    customers = []

    for cust in fetch_data(pg_cur, "SELECT * FROM sales.customer"):
        if "person" in id_mappings:
            cust["person_ref"] = id_mappings["person"].get(cust["person_id"])
        customers.append(cust)

    if customers:
        mongo_db["customers"].insert_many(customers)
        logger.info(f"Migrated {len(customers)} customers")

    logger.info("Sales module completed")


def create_indexes(mongo_db):
    """Создание индексов после миграции"""
    logger.info("Creating indexes")

    try:
        # Person
        mongo_db.persons.create_index("business_entity_id")
        mongo_db.business_entity_addresses.create_index("address_id")

        # HR
        mongo_db.employees.create_index("business_entity_id")
        mongo_db.employees.create_index("department_history.department.id")

        # Production
        mongo_db.products.create_index("product_id")
        mongo_db.products.create_index("product_subcategory_id")

        # Purchasing
        mongo_db.purchase_orders.create_index("vendor_id")
        mongo_db.purchase_orders.create_index("details.product_id")

        # Sales
        mongo_db.sales_orders.create_index("customer_id")
        mongo_db.sales_orders.create_index("sales_person_id")
        mongo_db.sales_orders.create_index("details.product_id")

        logger.info("Indexes created successfully")
    except Exception as e:
        logger.error(f"Error creating indexes: {str(e)}")

def main():
    try:
        logger.info("Starting migration process")

        # Подключение к PostgreSQL
        pg_conn = psycopg2.connect(**PG_CONFIG)
        pg_cur = pg_conn.cursor()

        # Подключение к MongoDB
        mongo_client = MongoClient(MONGO_URI)
        mongo_db = mongo_client[MONGO_DB_NAME]

        # Сброс счетчика ошибок конвертации
        convert_value.error_count = 0

        # Последовательная миграция модулей
        migrate_person(pg_cur, mongo_db)
        migrate_hr(pg_cur, mongo_db)
        migrate_production(pg_cur, mongo_db)
        migrate_purchasing(pg_cur, mongo_db)
        migrate_sales(pg_cur, mongo_db)

        # Финализация
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