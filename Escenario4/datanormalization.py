import sqlite3
import random

# Conectar a la base de datos SQLite
conn = sqlite3.connect("ecommerce.db")
cursor = conn.cursor()

# Crear la tabla centralized_data
cursor.execute("""
CREATE TABLE IF NOT EXISTS centralized_data (
    product_id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    product_name TEXT NOT NULL,
    price REAL NOT NULL,
    category TEXT NOT NULL,
    feature_1 REAL NOT NULL,
    feature_2 REAL NOT NULL,
    FOREIGN KEY (product_id) REFERENCES products (id),
    FOREIGN KEY (user_id) REFERENCES users (id)
)
""")

# Obtener todos los order_items con información de usuario, producto y pedido
cursor.execute("""
SELECT users.id AS user_id, products.id AS product_id, products.name, products.price, products.category
FROM users, products, orders, order_items
WHERE users.id = orders.user_id
AND orders.id = order_items.order_id
AND order_items.product_id = products.id
""")
data = cursor.fetchall()

# Enriquecer la información con los atributos feature_1 y feature_2
enriched_data = []
for row in data:
    user_id, product_id, product_name, price, category = row
    feature_1 = round(random.uniform(0, 1), 2)  # Reemplazar con el cálculo real de la característica
    feature_2 = round(random.uniform(0, 1), 2)  # Reemplazar con el cálculo real de la característica
    enriched_data.append((product_id, user_id, product_name, price, category, feature_1, feature_2))

# Insertar los datos enriquecidos en la tabla centralized_data
cursor.executemany("INSERT OR IGNORE INTO centralized_data (product_id, user_id, product_name, price, category, feature_1, feature_2) VALUES (?, ?, ?, ?, ?, ?, ?)", enriched_data)

# Guardar cambios en la base de datos
conn.commit()

# Cerrar conexión
conn.close()
