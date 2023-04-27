import sqlite3
import random
from faker import Faker

# Conectar a la base de datos SQLite (se creará si no existe)
conn = sqlite3.connect("ecommerce.db")
cursor = conn.cursor()

# Crear las tablas
cursor.execute("""
CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    price REAL NOT NULL
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users (id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS order_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders (id),
    FOREIGN KEY (product_id) REFERENCES products (id)
)
""")

# Simular datos
fake = Faker()

# Generar productos ficticios
categories = ["Electrónica", "Moda", "Hogar", "Deportes", "Libros"]

for _ in range(100):
    name = fake.sentence(nb_words=3)
    category = random.choice(categories)
    price = round(random.uniform(10, 500), 2)
    cursor.execute("INSERT INTO products (name, category, price) VALUES (?, ?, ?)", (name, category, price))

# Generar usuarios ficticios
for _ in range(50):
    name = fake.name()
    email = fake.email()
    cursor.execute("INSERT INTO users (name, email) VALUES (?, ?)", (name, email))

# Generar pedidos ficticios y sus items
for _ in range(200):
    user_id = random.randint(1, 50)
    cursor.execute("INSERT INTO orders (user_id) VALUES (?)", (user_id,))
    order_id = cursor.lastrowid

    num_items = random.randint(1, 5)
    for _ in range(num_items):
        product_id = random.randint(1, 100)
        quantity = random.randint(1, 3)
        cursor.execute("INSERT INTO order_items (order_id, product_id, quantity) VALUES (?, ?, ?)", (order_id, product_id, quantity))

# Guardar cambios en la base de datos
conn.commit()

# Cerrar conexión
conn.close()
