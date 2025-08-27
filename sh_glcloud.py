import shopify

# GERGO config
API_KEY = 'f6f52cf1afb0f7a650e07802e7beb845'
API_PASSWORD = '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'
API_VERSION = '2025-01'
SHOP_URL = 'wttgrn-i5.myshopify.com'
LOCATION_ID = '106776428874'
ENDPOINT = 'https://' + API_KEY + ':' + API_PASSWORD + '@' + SHOP_URL + '/admin'


class Sh:
    def __init__(self):
        # set shopify site
        shopify.ShopifyResource.set_site(ENDPOINT)
    
    # retrieve main prod
    def get_prod(self, shid):
        p = shopify.Product.find(shid)
        return p
    
    # retrieve variant
    def get_var(self, varid):
        v = shopify.Variant.find(varid)
        return v
    
    # retrieve variant id
    def get_variant_id(self, conf_shid, variant_size):
        conf = shopify.Product.find(str(conf_shid))

        for var in conf.variants:
            variant = shopify.Variant.find(var.id)
            if(variant.option1 == variant_size):
                return variant.id
        return None
    
    # retrieve variant inventory item id
    def get_inventory_item_id(self, var_shid):
        var = shopify.Variant.find(var_shid)
        return var.inventory_item_id
    
    # check if variant exist
    def check_variant_exist(self, prod, option1, option2):
        for v in prod.variants:
            if(v.option1 == option1 and v.option2 == option2):
                return v
        return False
    
    # update variant stock available
    def update_stock(self, inv_id, qty):
        try:
            inv_l = shopify.InventoryLevel.set(LOCATION_ID, inv_id, qty)
            return inv_l.available == qty
        except:
            return False
    
    # retrieve all orders
    def get_all_orders(self):
        orders = shopify.Order.find(status='any')
        return orders
    
    # get all products
    def get_all_products(self, limit=100):
        get_next_page = True
        since_id = 0
        while get_next_page:
            products = shopify.Product.find(since_id=since_id, limit=limit)

            for product in products:
                yield product
                since_id = product.id

            if len(products) < limit:
                get_next_page = False
    
    # create variant
    def create_variant(self, shid, sku, price, option1, option2):

        try:
            # load main prod
            p = shopify.Product.find(shid)

            # create local variant
            var = shopify.Variant()
            var.option1 = option1
            var.option2 = option2
            var.sku = sku + ' - ' + option1
            var.price = price
            var.fullfilment_service = "manual"
            var.inventory_management = "shopify"
            var.requires_shipping = True

            # attach variant
            p.variants.append(var)
            p.save()

            # return variant id
            for x in p.variants:
                if(x.option1 == option1):
                    return x.id
        except:
            print('Could not create variant ' + str(option1) + ' for product -> ' + str(sku))
            return None
