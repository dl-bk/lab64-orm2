from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import insert, update, delete, between, not_, distinct, func
from datetime import date
import psycopg2
import json

# Зчитування конфігураційних даних з файлу
with open('config.json') as f:
    config = json.load(f)

# Отримання логіну та паролю з об'єкта конфігурації
db_user = config['database']['user']
db_password = config['database']['password']

db_url = f'postgresql+psycopg2://{db_user}:{db_password}@localhost:5432/Hospital'
engine = create_engine(db_url)

#Соединение с БД
connection = engine.connect()
metadata = MetaData()

Session = sessionmaker(bind=engine)
session = Session()
#Загрузка таблиц
#автозагрузка
metadata.reflect(bind=engine)
# или одна таблица
# 
# departments = Table('departments', metadata, autoload=True, autoload_with=engine)

def insert_row(table: Table):
    columns = table.columns.keys()
    values = {}
    for column in columns:
        value = input(f"Enter value for column {column}: ")
        values[column] = value
    query = insert(table).values(values)
    connection.execute(query)
    connection.commit()
    print("Row successfully added")

def update_rows(table):
    columns = table.columns.keys()
    print("available columns to update: ")
    for idx, column in enumerate(columns, start=1):
        print(f"{idx}.{column}")
    selected_row_idx = int(input("Enetr row number to update: "))

    if 1 <= selected_row_idx <= len(columns):
        condition_column = columns[selected_row_idx - 1]
    else:
        print("Invalid row number!")
    
    condition_value = input(f"Enter value for condittion {condition_column}")
    new_values = {}
    for column in columns:
        value = input(f"Enter new value for column {column}: ")
        new_values[column] = value
    
    confirm_update = input("Update all rows? y/n: ")
    if confirm_update.lower() == 'y':
        query = update(table).where(getattr(table.c, condition_column) == condition_value).values(new_values)
        connection.execute(query)
        connection.commit()
        print("Data successfully updated!")

    else:
        print("Update cancelled")

def delete_rows(table):
    columns = table.columns.keys()
    print("available columns to delete: ")
    for idx, column in enumerate(columns, start=1):
        print(f"{idx}.{column}")
    selected_row_idx = int(input("Enetr row number to condition delete: "))

    if 1 <= selected_row_idx <= len(columns):
        condition_column = columns[selected_row_idx - 1]
    else:
        print("Invalid row number!")
    
    condition_value = input(f"Enter value for condittion {condition_column}")
    
    confirm_update = input("delete all rows? y/n: ")
    if confirm_update.lower() == 'y':
        query = delete(table).where(getattr(table.c, condition_column) == condition_value)
        connection.execute(query)
        connection.commit()
        print("Data successfully deleted!")

    else:
        print("deleting cancelled")

def execute_queries():
    print("Вивести прізвища лікарів та їх спеціалізації;")
    doctors_specializations = metadata.tables['doctorsspecializations']
    doctors = metadata.tables['doctors']
    specializations = metadata.tables['specializations']

    results = session.query(doctors.c.surname, specializations.c.name)\
                    .join(doctors_specializations, doctors_specializations.c.doctor_id == doctors.c.id)\
                    .join(specializations, doctors_specializations.c.specialization_id == specializations.c.id)\
                    .all()
    
    for result in results:
            doctor = result.surname
            spec = result.name
            print(f"""
            Doctor's surname: {doctor}
            Specialization name: {spec}
                """)
    print("--" * 20)

    print("Вивести прізвища та зарплати (сума ставки та надбавки) лікарів, які не перебувають у відпустці;")
    vacations = metadata.tables['vacations']
    results = session.query(doctors.c.surname, func.sum(doctors.c.salary + doctors.c.bonus).label("total"))\
                     .join(vacations, vacations.c.doctor_id == doctors.c.id)\
                     .filter(not_(between(date.today(), vacations.c.start_date, vacations.c.end_date)))\
                     .group_by(doctors.c.surname)
    
    for result in results:
            print(f"""
            Doctor's surname: {result.surname}
            total salary: {result.total}
                """)
    print("--" * 20) 

    print(" Вивести назви палат, які знаходяться у певному відділенні;")
    wards = metadata.tables['wards']
    department_id = 1
    results = session.query(wards.c.name).filter(wards.c.department_id == department_id)

    for result in results:
            print(f"""
            ward name: {result.name}
                """)
    print("--" * 20)

    print("Вивести усі пожертвування за вказаний місяць у вигляді: відділення, спонсор, сума пожертвування, дата пожертвування;")
    donations = metadata.tables['donations']
    departments = metadata.tables['departments']
    sponsors = metadata.tables['sponsors']

    results = session.query(departments.c.name.label("dep_name"), sponsors.c.name, donations.c.amount, donations.c.date)\
                     .join(departments, departments.c.id == donations.c.department_id)\
                     .join(sponsors, sponsors.c.id == donations.c.sponsor_id)
    
    for result in results:
        print(f"""
        Department: {result.dep_name}
        Sponsor: {result.name}
        Donation: {result.amount}
        Date: {result.date}
            """)

    print("Вивести назви відділень без повторень, які спонсоруються певною компанією.")
    sponsors_id = 1
    results = session.query(departments.c.name.distinct().label("dep_name"))\
                    .join(donations, donations.c.department_id == departments.c.id)\
                    .filter(donations.c.sponsor_id == sponsors_id)
                    
    for result in results:
        print(f"""
        Department: {result.dep_name}
            """)

def execute_queries2():
    print("відображати назви усіх таблиць;")
    for table_name in metadata.tables.keys():
        print(table_name)
    print("--" * 20)

    print("відображати назви стовпців певної таблиці;")
    table_name = "doctors"
    if table_name:
        table = metadata.tables[table_name]
        columns = table.columns.keys()
        for column in columns:
            print(column)
    else:
        print("Table is not found")
    print("--" * 20)

    print("відображати назви стовпців та їх типи для певної таблиці;")
    for column_name in columns:
        column = table.columns[column_name]
        print(f"{column_name}: {column.type}")
    print("--" * 20)

    print(" відображати зв’язки між таблицями;")
    for table_name, table in metadata.tables.items():
        print(f"Таблица: {table_name}")
        for fk in table.foreign_keys:
            referred_column = fk.column
            print(f"Таблица: {table_name}, Внешний ключ: {fk.parent} -> {referred_column}")
    print("--" * 20)
    
    print("вміти створювати таблиці;")
    try:
        new_table = Table('new_table', metadata,
                        Column('id', Integer, primary_key=True),
                        Column('name', String(255), nullable=False),
                        Column('age', Integer))
        metadata.create_all(engine)
        print("table created successfull")
    except Exception as e:
        print(f"{str(e)}")
    print("--" * 20)

    print("видаляти таблиці;")
    table_name = 'new_table'
    table_to_drop = metadata.tables.get(table_name)

    if table_to_drop is not None:
        choice = input(f"Do you want to drop {table_name}? y/n: ")
        if choice.lower() == 'y':
            table_to_drop.drop(bind=engine)
            print(f"Таблица {table_name} удалена.")
        else:
            print(f"Отменено. Таблица {table_name} не удалена.")
    else:
        print("Таблица не найдена.")

    print("додавати стовпці;")
    table_name = 'new_table'
    table = metadata.tables.get(table_name)

    new_column = Column('d', Integer)
    
    new_table = Table(
                table_name,
                metadata,
                Column('id', Integer, primary_key=True),
                *table.columns,
                new_column,
                extend_existing=True  
            )

            
    table.drop(bind=engine, checkfirst=True)
    new_table.create(bind=engine, checkfirst=True)
    session.commit()


while True:
    print("Choose Table: ")
    for table_name in metadata.tables.keys():
        print(table_name)
    table_name = input("\nEnter table name or 0 to exit: ")

    if table_name == '0':
        break

    if table_name in metadata.tables:
        table = metadata.tables[table_name]
        print(f"\n{table_name}\n")

        print("1. Вставити рядки")
        print("2. Оновити рядки")
        print("3. Видалити рядки")
        print("4. execute all queries from task 2")
        print("5. execute all queries from task 3")
        print("0. Вийти")

        choice = input("Оберіть опцію: ")
        if choice == "0":
            break
        elif choice == "1":
            insert_row(table)
        elif choice == "2":
            update_rows(table)
        elif choice == "3":
            delete_rows(table)
        elif choice == "4":
            execute_queries()
        
        else:
            print("Невірний вибір. Будь ласка, оберіть знову.")
    else:
        print("Такої таблиці не існує. Будь ласка, введіть правильну назву.")

session.close()
