import mysql.connector
from time import sleep
import pandas as pd
import csv
from sh import Sh
import io
from datetime import datetime
import sib_api_v3_sdk

def db_connect(mysql_cfg):
    return mysql.connector.connect(
        user=mysql_cfg['db_user'],
        password=mysql_cfg['db_password'],
        host=mysql_cfg['host'],
        database=mysql_cfg['db_name']
    )

def send_mail(blob, bucket):
    configuration = sib_api_v3_sdk.Configuration()
    configuration.api_key['api-key'] = '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'
    api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))

    senderSmtp = sib_api_v3_sdk.SendSmtpEmailSender(name="Pil Sessions",email="sessions@pilassociati.it")
    sendTo = sib_api_v3_sdk.SendSmtpEmailTo(email="info@ilgergo.it", name="Gergo")

    file_name = 'orders-updates-' + datetime.now().strftime('%Y-%m-%d') + '.txt'

    send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(
        sender=senderSmtp,
        to=[sendTo],
        html_content="<p></p>",
        subject="Order " + datetime.now().strftime('%Y-%m-%d'),
        attachment=[
            { 
                'url': 'https://storage.googleapis.com/' + bucket.name + '/' + blob.name
            }
        ]
    )

    api_response = api_instance.send_transac_email(send_smtp_email)

def get_order_db(cursor, table, oid):
    query = ("SELECT * FROM " + table + " WHERE shid = '" + str(oid) + "'")

    cursor.execute(query)
    res = cursor.fetchone()

    return res

def add_order_record(cursor, table, conn, shid, customer):
    query = ("INSERT INTO " + table + " (shid, customer, sync_date) VALUES (%s, %s, %s)")
    values = (shid, customer, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    cursor.execute(query, values)
    conn.commit()

def upload_blob(data, blob):
    blob.upload_from_string(data, content_type='text/csv')

def write_to_file(orders_data, blob):
    df = pd.DataFrame.from_dict(orders_data)
    csv_str = df.to_csv(index=False, header=True, sep=';')
    print(csv_str)
    upload_blob(csv_str, blob)

def create_order_file(order):
    shipping_address = order.shipping_address
    data = {
        "vabant": "",
        "vabccm": '521934',
        "vablnp": '52',
        "vabctr": 0,

        "vabcbo": 4 if 'Cash on Delivery (COD)' in order.payment_gateway_names else 1,

        "vabaas": datetime.fromisoformat(order.created_at).strftime("%Y"),
        "vabmgs":  datetime.fromisoformat(order.created_at).strftime("%m%d"),

        "vabrmn": order.name,

        "vabrsd": shipping_address.name[0:35].strip() if shipping_address.name else '',
        "vabind": shipping_address.address1[0:35].replace(',', '').strip() if shipping_address.address1 else '',
        "vablod": shipping_address.city[0:35].strip() if shipping_address.city else '',
        "vabcad": shipping_address.zip.strip() if shipping_address.zip else '',
        "vabprd": shipping_address.country_code[0:2].strip() if shipping_address.country_code else '',

        "vabnzd": '',
        "vabncl": 1,
        "vabpkb": order.total_weight / 1000,
        
        "vabcas": order.total_price if 'Cash on Delivery (COD)' in order.payment_gateway_names else "",
        "vabvca": "EUR" if 'Cash on Delivery (COD)' in order.payment_gateway_names else "",

        "vabnot": shipping_address.address1[35:35].strip() if shipping_address.address1 else '',
        "vabnt2": shipping_address.address1[70:35].strip() if shipping_address.address1 else '',

        "vabnrc": shipping_address.name[0:35].strip() if shipping_address.name else '',
        "vabtrc": shipping_address.phone[0:16].strip() if shipping_address.phone else '',
        "vabtic": '',
        "vabemd": order.customer.email[0:70].strip() if order.customer.email else '',
        "vabrma": order.id
    }
    return data

def process_day_orders(cfg, blob, bucket, day):
    mysql_cfg = cfg['mysql']

    db_connection = db_connect(mysql_cfg)
    db_cursor = db_connection.cursor()

    shopify = Sh()
    orders = shopify.get_all_orders()

    orders_data = []
    try:
        d = day.split('-')[0]
        m = day.split('-')[1]
        y = day.split('-')[2]
        selected_day = datetime(int(y), int(m), int(d))
    except:
        return {"status": False, "message": 'Malformed day'}
    
    for o in orders:
        if 'shipping_address' not in o:
            continue
        order_date = datetime.strptime(o.created_at, '%Y-%m-%dT%H:%M:%S%z')
        order_date = datetime(order_date.year, order_date.month, order_date.day)

        if(selected_day == order_date):
            order_data = create_order_file(o)
            orders_data.append(order_data)
        
    if(len(orders_data) > 0): 
        write_to_file(orders_data, blob)

        # set public access
        blob.acl.reload()
        acl = blob.acl
        acl.all().grant_read()
        acl.save()

        return {"status": True, "message": '<a href="https://storage.googleapis.com/' + bucket.name + '/' + blob.name + '">File ordini ' + str(selected_day) + '</a>'}
    else:
        return {"status": False, "message": 'No orders to process!'}

def getorders():
    shopify = Sh()
    orders = shopify.get_all_orders()
    message = []
    for o in orders:
        message.append({
            'id': o.id,
            'email': o.contact_email,
            'total': o.total_price,
            'created_at': datetime.fromisoformat(o.created_at).strftime("%d-%m-%y %H:%m")
        })
    return {"status": True, "message": message}

def process_orders(cfg, blob, bucket):
    mysql_cfg = cfg['mysql']

    db_connection = db_connect(mysql_cfg)
    db_cursor = db_connection.cursor()

    shopify = Sh()
    orders = shopify.get_all_orders()

    orders_data = []

    for o in orders:
        order_db = get_order_db(db_cursor, mysql_cfg['orders_table'], o.id)
        # order already saved
        if(order_db):
            continue
        
        if 'shipping_address' not in o:
            continue

        order_data = create_order_file(o)
        orders_data.append(order_data)

        add_order_record(db_cursor, 'orders', db_connection, o.id, o.customer.id)

    if(len(orders_data) > 0): 
        write_to_file(orders_data, blob)

        # set public access
        blob.acl.reload()
        acl = blob.acl
        acl.all().grant_read()
        acl.save()
        
        send_mail(blob, bucket)
    else:
        return 'No orders to process!'
