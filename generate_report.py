#!/usr/bin/env python3
import logging
import os
import pandas as pd
import requests
from dotenv import load_dotenv
from truflation.data.pipeline import Pipeline
from truflation.data.pipeline_details import PipeLineDetails
from truflation.data.source_details import SourceDetails
from truflation.data.export_details import ExportDetails
from balances import get_evm_balance, get_tron_balance

load_dotenv()
logger = logging.getLogger(__name__)
PIPELINE_NAME = 'Virtual Asset report'
ETHERSCAN_KEY = os.environ.get('ETHERSCAN_KEY')
BSCSCAN_KEY = os.environ.get('BSCSCAN_KEY')

def get_value(row):
    wallet = row['wallet']
    if row['blockchain'] == 'btc':
        response = requests.get(
            f'https://blockchain.info/rawaddr/{wallet}'
        )
        d = response.json()
        print(d['final_balance'])
    elif row['blockchain'] == 'eth':
        object = get_evm_balance(
            wallet, 'api.etherscan.io', 'ethereum', ETHERSCAN_KEY
        )
        print(object)
        response = requests.get(
            f'https://api.etherscan.io/api?module=account&action=tokenbalance&contractaddress=0x57d90b64a1a57749b0f932f1a3395792e12e7055&address={wallet}&tag=latest&apikey={ETHERSCAN_KEY}'
        )
        d = response.json()
    elif row['blockchain'] == 'bnb':
        object = get_evm_balance(
            wallet, 'api.bscscan.com', 'binancecoin', BSCSCAN_KEY
        )
        print(object)
        response = requests.get(
            f'https://api.bscscan.com/api?module=account&action=tokenbalance&contractaddress=0x57d90b64a1a57749b0f932f1a3395792e12e7055&address={wallet}&tag=latest&apikey={BSCSCAN_KEY}'
        )
    elif row['blockchain'] == 'trx':
        object = get_tron_balance(wallet)
        print(object)
    return None

def transformer(inputs: dict):
    outputs = pd.DataFrame()
    inputs['wallets'].apply(get_value, axis=1)
    return outputs

def get_details(**config):
    sources = [
        SourceDetails(
            'wallets',
            'csv',
            'wallets.csv'
        )
    ]

    exports = [
        ExportDetails(
            'report',
            "csv:csv",
            'report.csv'
        )
    ]

    return PipeLineDetails(
        name=PIPELINE_NAME,
        sources=sources,
        exports=exports,
        transformer=transformer
    )

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    pipeline_details = get_details()
    my_pipeline = Pipeline(pipeline_details)
    my_pipeline.ingest()
