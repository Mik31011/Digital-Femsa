import sqlite3
import tkinter as tk
import pickle
import numpy as np
from tkinter import messagebox


#Obtiene todos los productos dentro de la tabla products
def get_products():
    with sqlite3.connect("ecommerce.db") as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, price FROM products")
        products = cursor.fetchall()
    return products


#Utiliza el modelo creado en el archivo model.ipynb para generar sugerencias aprovechando la
#informacion enriquezida en el archivo datanormalization.py, en un caso real las caracteristicas deberian
#ser planteadas de acuerdo a las caracteristicas de un producto, asi como las reglas definidas por el negocio
def get_related_products(product_id):
    with open("knn_model.pkl", "rb") as f:
        knn_model = pickle.load(f)
    # Leer datos de la tabla centralized_data
    conn = sqlite3.connect("ecommerce.db")
    cursor = conn.cursor()
    cursor.execute("SELECT product_id, feature_1, feature_2 FROM centralized_data")
    data = cursor.fetchall()
    conn.close()
    product_ids = [row[0] for row in data]
    features = np.array([[row[1], row[2]] for row in data])

    if product_id not in product_ids:
        return []

    product_index = product_ids.index(product_id)
    feature = features[product_index]

    nearest_indices = knn_model.kneighbors([feature], return_distance=False)[0][1:]

    related_products = []
    for nearest_index in nearest_indices:
        related_product_id = product_ids[nearest_index]
        related_feature = features[nearest_index]

        conn = sqlite3.connect("ecommerce.db")
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, price FROM products WHERE id=?", (related_product_id,))
        related_product = cursor.fetchone()
        conn.close()

        related_products.append(related_product)
    return related_products


#Muestra la sugerencia de productos
def buy_product():
    selected_product = products_listbox.curselection()
    if not selected_product:
        messagebox.showerror("Error", "Debes seleccionar un producto para comprar.")
        return
    
    product_id, product_name, product_price = products[selected_product[0]]
    related_products = get_related_products(product_id)

    messagebox.showinfo("Compra realizada", f"Has comprado '{product_name}' por ${product_price:.2f}.")
    related_text = "\n".join([f"{name} - ${price:.2f}" for _, name, price in related_products])
    messagebox.showinfo("Productos sugeridos", f"Otros usuarios también compraron:\n\n{related_text}")

# Interfaz de usuario
window = tk.Tk()
window.title("Ejemplo eCommerce")

products_label = tk.Label(window, text="Productos disponibles:")
products_label.pack()

products = get_products()
products_listbox = tk.Listbox(window, width=50, height=10)
for _, name, price in products:
    products_listbox.insert(tk.END, f"{name} - ${price:.2f}")
products_listbox.pack()

buy_button = tk.Button(window, text="Comprar producto seleccionado", command=buy_product)
buy_button.pack()

window.mainloop()
