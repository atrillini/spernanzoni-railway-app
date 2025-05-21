import pandas as pd
from sh_glcloud import Sh
import mysql.connector

def get_product_data(path):
    prod_df = read_import_file(path)
    return process_products(prod_df)

def read_import_file(file_path):
    return pd.read_csv(file_path, sep=';')

def db_connect(mysql_cfg):
    return mysql.connector.connect(
        user=mysql_cfg['db_user'],
        password=mysql_cfg['db_password'],
        host=mysql_cfg['host'],
        port=mysql_cfg['port'],
        database=mysql_cfg['db_name']
    )

def get_product_db(cursor, table, shid):
    query = "SELECT * FROM " + table + " WHERE shid = '" + str(shid) + "'"

    cursor.execute(query)
    res = cursor.fetchone()

    return res

def add_product_record(cursor, table, conn, sku, shid):
    query = ("INSERT INTO " + table + " (sku, shid) VALUES (%s, %s)")
    values = (sku, shid)

    cursor.execute(query, values)
    conn.commit()

def set_prod_data(cursor, table, conn, sku, title, tags, color, material, model, price):
    query = "UPDATE " + table + \
        " SET title = %s, tags = %s, color = %s, material = %s, model = %s, price = %s WHERE sku = '" + sku + "'"
    values = (title, tags, color, material, model, price)

    cursor.execute(query, values)
    conn.commit()

def process_products(product_df):
    prods = []
    for index, product_data in product_df.iterrows():
        prod = {
            'sku': product_data['Variant SKU'],
            'handle': product_data['Handle'],
            'title': product_data['Title'],
            'descr': product_data['descr'],
            'vendor': 'Gergo - storeonline',
            'bottom': product_data['Fondo'],
            'tags': product_data['Tags'],
            'color': product_data['Colore'],
            'material': product_data['Materiale'],
            'model': product_data['Modello'],
            'images': product_data['Image Src'],
            'price': float(product_data['Variant Price']),
            'price_eu': float(product_data['Price / Europa']),
            'price_wrld': float(product_data['Price / World'])
        }
        prods.append(prod)
    return prods

def import_products(cfg):
    response = []
    shopify = Sh()
    sh_prods = shopify.get_all_products()
    

    db_connection = db_connect(cfg['mysql'])
    db_cursor = db_connection.cursor()

    for p in sh_prods:
        print('sto valutando il prodotto '+p.id)  
        if('import' in p.tags):

            if(get_product_db(db_cursor, 'products', p.id)): continue

            variants = p.variants
            variant1 = variants[0]

            sku = variant1.sku.split(' - ')[0].strip()
            color = variant1.option2
            title = p.title
            shid = p.id
            price = variant1.price
            
            tags = p.tags.replace(' ', '').split(',')
            tags.remove('import')
            new_tags = ",".join(tags)

            p.tags = new_tags
            p.save()

            material = ''
            model = ''
            metafields = p.metafields()
            for m in metafields:
                if(m.key == 'Materiale'):
                    material = m.value
                if(m.key == 'Modello'):
                    model = m.value
            
            add_product_record(db_cursor, 'products', db_connection, sku, p.id)
            set_prod_data(db_cursor, 'products', db_connection, sku, title, new_tags, color, material, model, price)
            response.append(sku)
         
    return response
