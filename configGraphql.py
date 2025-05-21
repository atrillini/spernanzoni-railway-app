import requests
import yaml


with open("./config.yml", "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    


# Configurazione
SHOPIFY_STORE_URL = 'https://'+cfg['spernanzoni']['shopify']['shop_url']  # Sostituisci con il tuo URL
API_VERSION = cfg['spernanzoni']['shopify']['version_up']  # Usa la versione più recente dell'API
ACCESS_TOKEN = cfg['spernanzoni']['shopify']['token']  # Sostituisci con il tuo token
METAOBJECT_DEFINITION_ID = "gid://shopify/MetaobjectDefinition/17010622730"  # metaobjectfatture
GRAPHQL_ENDPOINT = f"{SHOPIFY_STORE_URL}/admin/api/{API_VERSION}/graphql.json"




# Intestazioni per la richiesta
headers = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": ACCESS_TOKEN
}

# Funzione per eseguire una query o mutation GraphQL
def execute_graphql(query, variables=None):
    payload = {"query": query, "variables": variables or {}}
    response = requests.post(GRAPHQL_ENDPOINT, headers=headers, json=payload)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Errore GraphQL: {response.status_code} - {response.text}")
