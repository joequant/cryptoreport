import requests
from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(3), wait=wait_exponential())
def get_btc_balance(wallet_address: str):
    try:
        response = requests.get(
            f'https://blockchain.info/rawaddr/{wallet_address}'
        )
        data = response.json()
        balance = float(data['final_balance'])/100000000.0

        price_url = 'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd'
        price_response = requests.get(price_url)
        price_data = price_response.json()
        price_usd = price_data['bitcoin']['usd']
        return {
            'balance': balance,
            'price_usd': price_usd
        }
    except requests.exceptions.HTTPError as e:
        print(f"An error occurred: {e}")
        return None

@retry(stop=stop_after_attempt(3), wait=wait_exponential())
def get_tron_balance(wallet_address):
    url = f'https://apilist.tronscanapi.com/api/account/wallet?address={wallet_address}'
    try:
        response = requests.get(url)
        data = response.json()
        balance = float(data['data'][0]['balance'])
        price_usd = float(data['data'][0]['token_price_in_usd'])
        return {
            'balance': balance,
            'price_usd': price_usd
        }
    except requests.exceptions.HTTPError as e:
        print(f"An error occurred: {e}")
        return None

@retry(stop=stop_after_attempt(3), wait=wait_exponential())
def get_trc20_token_balance(wallet_address, token_contract_address):
    url = f'https://apilist.tronscan.org/api/token_trc20/holders?holder_address={wallet_address}&contract_address={token_contract_address}'

    try:
        response = requests.get(url)
        data = response.json()
        balance_token = int(data['trc20_tokens'][0]['balance']) / 10**6

        # Get the value in USD
        token_price_url = f'https://api.coingecko.com/api/v3/simple/token_price/tron?contract_addresses={token_contract_address}&vs_currencies=usd'
        token_price_response = requests.get(token_price_url)
        token_price_data = token_price_response.json()
        token_usd_price = token_price_data[token_contract_address]['usd']

        return {
            'balance_token': balance_token,
            'price_usd': token_usd_price
        }

    except requests.exceptions.HTTPError as e:
        print(f"An error occurred: {e}")
        return None

@retry(stop=stop_after_attempt(3), wait=wait_exponential())
def get_evm_balance(wallet_address, chain_url, coin, api_key):
    url = f'https://{chain_url}/api?module=account&action=balance&address={wallet_address}&tag=latest&apikey={api_key}'
    try:
        response = requests.get(url)
        data = response.json()
        balance_wei = int(data['result'])
        balance = balance_wei / 10**18

        # Get the value in USD
        price_url = f'https://api.coingecko.com/api/v3/simple/price?ids={coin}&vs_currencies=usd'
        price_response = requests.get(price_url)
        price_data = price_response.json()
        price_usd = price_data[coin]['usd']
        return {
            'balance': balance,
            'price_usd': price_usd
        }

    except requests.exceptions.HTTPError as e:
        print(f"An error occurred: {e}")
        return None

@retry(stop=stop_after_attempt(3), wait=wait_exponential())
def get_evm_token_balance(
        wallet_address, chain_url, coin,
        token_contract_address, api_key
):
    url = f'https://{chain_url}/api?module=account&action=tokenbalance&contractaddress={token_contract_address}&address={wallet_address}&tag=latest&apikey={api_key}'

    try:
        response = requests.get(url)
        data = response.json()
        balance_token = int(data['result']) / 10**18

        # Get the value in USD
        token_price_url = f'https://api.coingecko.com/api/v3/simple/token_price/{coin}?contract_addresses={token_contract_address}&vs_currencies=usd'
        token_price_response = requests.get(token_price_url)
        token_price_data = token_price_response.json()
        token_usd_price = token_price_data[token_contract_address.lower()]['usd']
        return {
            'balance': balance_token,
            'price_usd': token_usd_price,
        }
    except requests.exceptions.HTTPError as e:
        print(f"An error occurred: {e}")
        return None
