import requests
import json

URL = "https://dummyjson.com/products?limit=0"

def fetch_products():
    response = requests.get(URL)
    data = response.json()
    products = data["products"]

    return products

products = fetch_products()

for product in products :
    print(product) 



def create_product_catalog(products):

    catalog = []

    for product in products:
        print(product)

        item = {}

        item["product_id"] = f"P{product['id']:04d}"
        item["product_name"] = product["title"] 
        item["category"] = product["category"] 
        item["price"] = product["price"] 
        item["rating"] = product["rating"] 
        item["availability"] = product["availabilityStatus"]

        catalog.append(item)


    return catalog

products = fetch_products()

catalog = create_product_catalog(products)

for product in catalog:
    print(product)


with open("data/product_catalog.json", "w") as file:
    json.dump(catalog, file, indent=2)