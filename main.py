import requests
import pandas as pd
from time import sleep
from products import get_product_data, import_products
from orders import process_orders, process_day_orders, getorders
import json
import mysql.connector
from sh_glcloud import Sh
import base64
from datetime import datetime
from flask import Flask, request
import subprocess

BASE_COLUMN_SIZE = 4

def read_stock_file(file_path):
    return pd.read_csv(file_path, sep=';', header=None, encoding='latin1')

def db_connect(mysql_cfg):
    return mysql.connector.connect(
        user=mysql_cfg['db_user'], \
        password=mysql_cfg['db_password'], \
        host=mysql_cfg['host'], \
        port=mysql_cfg['port'],
        database=mysql_cfg['db_name']
    )

def add_stock_record(cursor, table, conn, code, variant, qty, shid, inv_id):
    query = (
        "INSERT INTO " + table + " (code, variant, qty, sync_date, shopify_id, inventory_item_id) VALUES (%s, %s, %s, %s, %s, %s)")
    values = (code, variant, qty, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), shid, inv_id)

    cursor.execute(query, values)
    conn.commit()

def get_current_stock(cursor, table, code, variant):
    query = "SELECT * FROM " + table + " WHERE code = %s AND variant = %s"
    values = (code, variant)

    cursor.execute(query, values)
    res = cursor.fetchone()

    return res

def get_product_record(cursor, table, sku):
    query = "SELECT * FROM " + table + " WHERE sku = '" + sku + "'"

    cursor.execute(query)
    res = cursor.fetchone()

    return res

def update_stock_record(cursor, table, conn, code, variant,  qty):
    query = "UPDATE " + table + " SET qty = %s, sync_date = %s WHERE code = %s AND variant = %s"
    values = (qty,datetime.now().strftime('%Y-%m-%d %H:%M:%S') ,code ,variant)

    cursor.execute(query, values)
    conn.commit()

def get_sizes(path, code):
    with open(path) as json_file:
        data = json.load(json_file)
        code = 'B' if code == ' ' else code
        if(data[code]):
            return data[code].split(';')
        else:
            return ''

def get_product_shipifyid(cursor, table, sku):
    query = ("SELECT shid FROM " + table + " WHERE sku = '" + sku + "'")
    
    cursor.execute(query)
    res = cursor.fetchone()
    
    return res

def notify_telegram(msg):
    payload = {
        "text": msg,
        "parse_mode": "Markdown",
        "disable_web_page_preview": False,
        "disable_notification": False,
        "reply_to_message_id": None,
        "chat_id": CHATID_TELEGRAM
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    requests.post(URL_TELEGRAM, json=payload, headers=headers)

def format_string_file(s):
    string_formatted = ''
    for r in s.split('\n'):
        if(not r or r == 'b'): continue
        string_formatted += r + '\n'
    return string_formatted

def process_stock(stock_df, size_guide_path):
    stocks = []
    skus = []
    for index, row in stock_df.iterrows():
        if(row[0] in skus): continue
        else: skus.append(row[0])
        prod = {
            'sku': row[0],
            'color': row[1],
            'variants': []
        }
        sizes_label = get_sizes(size_guide_path, row[3])
        for i in range(0, len(sizes_label)):
            prod['variants'].append({
                'size': sizes_label[i],
                'qty': row[BASE_COLUMN_SIZE + i]
            })
        stocks.append(prod)
    return stocks

def update_stocks(cfg):
    log_string = ''
    tel_string = ''
    tc = 0

    # stock file url
    file_path = cfg['stock_url']
    #mysql conf
    mysql_cfg = cfg['mysql']
    # db stock table name
    stock_table_name = mysql_cfg['stocks_table']
    products_table_name = mysql_cfg['products_table']

    product_data = get_product_data(cfg['products_url'])
    
    stock_df = read_stock_file(file_path)
    stock_data = process_stock(stock_df, cfg['size_guide_url'])

    # blob.upload_from_string(datetime.now().strftime('%Y-%m-%d %H:%M:%S') +' -> Stock processed: ' + str(len(stock_data)))
    
    # db connection
    db_connection = db_connect(mysql_cfg)
    db_cursor = db_connection.cursor()

    shopify = Sh()

    count = 0
    log_string += '\n\n'
  
    for stock in stock_data:
        count += 1

        # clean sku
        sku = stock['sku'].replace("'", "")
        print('sto facendo lo sku '+ sku)
        # retrieve shopify id on db
        shid_conf = get_product_shipifyid(db_cursor, products_table_name, sku)
        # product not added
        if(shid_conf == None): continue

        shid_conf = shid_conf[0]

        # get product data
        p = [p for p in product_data if p['sku'] == sku]
        
        #product not founded
        if(len(p) < 1):
            db_prod = get_product_record(db_cursor, products_table_name, sku)
            if(db_prod):
                p = {
                    'color': db_prod[6],
                    'price': db_prod[9]
                }
            else:
                continue
        else:
            p = p[0]

        var_update_data = []
        for var in stock['variants']:
            # get current stock 
            res = get_current_stock(db_cursor, stock_table_name, stock['sku'], var['size'])

            # stock never syncronized
            if(not res):
                sh_prod = shopify.get_prod(shid_conf)
                sh_variant = shopify.check_variant_exist(sh_prod, var['size'], p['color'])

                sleep(0.5)

                if(not sh_variant):
                    # create variant on shopify
                    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' - ' + sku + ' -> Create variant ' + var['size'] + '\n')
                    var_id = shopify.create_variant(shid_conf, sku, p['price'], var['size'], p['color'])

                    if not var_id:
                        continue
                    
                    inv_id = shopify.get_inventory_item_id(var_id)

                    sleep(1)
                else:
                    var_id = sh_variant.id
                    inv_id = sh_variant.inventory_item_id
                
                # check variant id
                if(not var_id):
                    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' - ' + sku + " -> Can't retrieve variant id from shopify  " + var['size'] + '\n')
                    #tel_string += sku + " -> Can't retrieve variant id from shopify  " + var['size'] + '\n'
                    continue

                # chekc inventory_item_id
                if(not inv_id):
                    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' - ' + sku + " -> Can't retrieve inventory item id from shopify  " + var['size'] + '\n')
                    #tel_string += sku + " -> Can't retrieve inventory item id from shopify  " + var['size'] + '\n'
                    continue

                # add stock to db
                add_stock_record(db_cursor, stock_table_name, db_connection, stock['sku'], var['size'], var['qty'], var_id, inv_id)

                if(shopify.update_stock(inv_id, var['qty'])):
                    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' - ' + sku + " -> Updated variant " + str(var['size']) + ' to -> ' + str(var['qty']) + '\n')
                    tc += 1
                else:
                    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' - ' + sku + ' Error while updating stock ' + var['size'] + '\n')
                    #tel_string += sku + ' Error while updating stock ' + var['size'] + '\n'
        
            # stock already syncronized
            else:
                new_qty = var['qty']
                qty = res[3]
                var_id = res[5]
                inv_id = res[6]

                if(qty == new_qty): continue

                if(shopify.update_stock(inv_id, new_qty)):
                    update_stock_record(db_cursor, stock_table_name, db_connection, sku, var['size'], new_qty)
                    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' - ' + sku + " -> Updated variant " + str(var['size']) + ' from ' + str(qty) + ' to -> ' + str(new_qty) + '\n')
                    tc += 1
                else:
                    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' - ' + sku + ' Error while updating stock ' + var['size'] + '\n')
                    #tel_string += sku + ' Error while updating stock ' + var['size'] + '\n'
    #string_to_push = format_string_file(old_string + log_string)
    #notify_telegram('products updated -> ' + str(tc) + '\n' + tel_string)
    #blob.upload_from_string(string_to_push)
    return "ok"




# define default gergo config
cfg = {
        'stock_url': 'https://spernanzoni.com/import/batch/stock.csv',
        'products_url': 'https://spernanzoni.com/import/batch/anagrafica.csv',
        'size_guide_url': 'gergo-sizes.json',
        'mysql': {
            'host': 'gondola.proxy.rlwy.net', 
            'db_user': 'root', 
            'db_password': 'RkNEPwSazUluXSUEngRYwJBbpoymBjDo',
            'db_name': 'railway', 
            'port':'48037',
            'stocks_table': 'stocks', 
            'products_table': 'products',
            'orders_table': 'orders'
        }
    }

app = Flask(__name__)
@app.route("/run", methods=["GET"])

def run_script():
    token = request.args.get("token")
    if token != "JVk02BmHoCaupThoxpERbKV7VXA1sB9EgzgzA1DrRBV1OMglutDk8eraUIXQVWCe":
        return "Unauthorized", 401

    # Esegui lo script desiderato (ad esempio uno script Python o shell)
    result = subprocess.run(["python3", "updatestocks.py"], capture_output=True, text=True)

    return result.stdout or result.stderr

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)

        # call update stocks
#update_stocks(cfg)
#import_products(cfg)
'''
    elif(function == 'orders'):
        storage_client = storage.Client()
        bucket = storage_client.get_bucket('gergo-backend-orders')

        blob = bucket.blob('orders-updates-' + datetime.now().strftime('%Y-%m-%d') + '.csv')
        blob.upload_from_string('')

        sleep(1)

        if(not request.args.get('day')):
            process_orders(cfg, blob, bucket)
        else:
            day = request.args.get('day')
            blob_day = bucket.blob('orders-download-' + day + '.csv')
            blob_day.upload_from_string('')
            res = process_day_orders(cfg, blob_day, bucket, day)
            if(res['status']):
                return res['message'], 200
            else:
                return res['message'], 200

    elif(function == 'import'):
        res = import_products(cfg)
        if(len(res) > 0):
            return 'Prodotti importati! -> ' + str(res), 200
        return  'Nessun prodotto da importare!', 200
    
    elif(function == 'getorders'):
        res = getorders()
        return res['message']

'''        
