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

# define default gergo config
cfg = {
        'stock_url': 'https://spernanzoni.com/import/batch/stock.csv',
        'products_url': 'https://spernanzoni.com/import/batch/anagrafica.csv',
        'size_guide_url': 'spernanzoni/gergo-sizes.json',
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



        # call update stocks
#update_stocks(cfg)
import_products(cfg)
