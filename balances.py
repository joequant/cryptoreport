import requests
from tenacity import retry, stop_after_attempt, wait_fixed

class CacheRequest:
    def __init__(self):
        self.cache = {}
    @retry(stop=stop_after_attempt(8), wait=wait_fixed(2))
    def get_json(self, url):
        url_cache = self.cache.get(url)
        try:
            if url_cache is not None:
                return url_cache
            response = requests.get(url)
            data = response.json()
            self.cache[url] = data
            return data
        except requests.exceptions.HTTPError as e:
            print(f"An error occurred: {e}")
            return None

cache = CacheRequest()

def get_btc_balance(wallet_address: str):
    try:
        data = cache.get_json(
            f'https://blockchain.info/rawaddr/{wallet_address}'
        )
        balance = float(data['final_balance'])/100000000.0

        data = cache.get_json(
            'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd'
        )
        return {
            'balance': balance,
            'price_usd': data['bitcoin']['usd']
        }
    except requests.exceptions.HTTPError as e:
        print(f"An error occurred: {e}")
        return None

def get_tron_balance(wallet_address):
    url = f'https://apilist.tronscanapi.com/api/account/wallet?address={wallet_address}'
    try:
        data = cache.get_json(url)
        balance = float(data['data'][0]['balance'])
        price_usd = float(data['data'][0]['token_price_in_usd'])
        return {
            'balance': balance,
            'price_usd': price_usd
        }
    except requests.exceptions.HTTPError as e:
        print(f"An error occurred: {e}")
        return None

def get_trc20_token_balance(wallet_address, token_contract_address):
    if token_contract_address is None or token_contract_address == '':
       return get_tron_balance(wallet_address)
    url = f'https://apilist.tronscan.org/api/token_trc20/holders?holder_address={wallet_address}&contract_address={token_contract_address}'
    try:
        data = cache.get_json(url)
        balance_token = int(data['trc20_tokens'][0]['balance']) / 10**6

        # Get the value in USD
        token_price_url = f'https://api.coingecko.com/api/v3/simple/token_price/tron?contract_addresses={token_contract_address}&vs_currencies=usd'
        data = cache.get_json(token_price_url)
        return {
            'balance': balance_token,
            'price_usd': data[token_contract_address]['usd']
        }

    except requests.exceptions.HTTPError as e:
        print(f"An error occurred: {e}")
        return None

def get_evm_balance(wallet_address, chain_url, coin, api_key):
    url = f'https://{chain_url}/api?module=account&action=balance&address={wallet_address}&tag=latest&apikey={api_key}'
    try:
        data = cache.get_json(url)
        balance_wei = int(data['result'])
        balance = balance_wei / 10**18

        # Get the value in USD
        data = cache.get_json(f'https://api.coingecko.com/api/v3/simple/price?ids={coin}&vs_currencies=usd')
        return {
            'balance': balance,
            'price_usd': data[coin]['usd']
        }
    except requests.exceptions.HTTPError as e:
        print(f"An error occurred: {e}")
        return None

def get_evm_token_balance(
        wallet_address, chain_url, coin,
        token_contract_address, api_key
):
    if token_contract_address is None or token_contract_address == '':
        return get_evm_balance(
            wallet_address,
            chain_url,
            coin,
            api_key
        )
    url = f'https://{chain_url}/api?module=account&action=tokenbalance&contractaddress={token_contract_address}&address={wallet_address}&tag=latest&apikey={api_key}'
    try:
        data = cache.get_json(url)
        balance_token = int(data['result']) / 10**18

        # Get the value in USD
        url = f'https://api.coingecko.com/api/v3/simple/token_price/{coin}?contract_addresses={token_contract_address}&vs_currencies=usd'
        data = cache.get_json(url)
        return {
            'balance': balance_token,
            'price_usd': data[token_contract_address.lower()]['usd']
        }
    except requests.exceptions.HTTPError as e:
        print(f"An error occurred: {e}")
        return None
