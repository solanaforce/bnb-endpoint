# import asyncio
from collections import defaultdict
# import datetime
# import json
# import logging
# import os
# import traceback
import time



from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware 

from .models import Settings, db, Wallets, Accounts
from .config import config, get_contract_abi, get_contract_address
from .logging import logger
from .token import Token, get_all_accounts



w3 = Web3(HTTPProvider(config["FULLNODE_URL"], request_kwargs={'timeout': int(config['FULLNODE_TIMEOUT'])}))
w3.middleware_onion.inject(geth_poa_middleware, layer=0) 

def handle_event(transaction):        
    logger.info(f'new transaction: {transaction!r}')


def log_loop(last_checked_block, check_interval):
    from .tasks import walletnotify_flexcryptopay, drain_account
    from app import create_app
    app = create_app()
    app.app_context().push()

    while True:
        
        last_block =  w3.eth.block_number
        if last_checked_block == '' or last_checked_block is None:
            last_checked_block = last_block

        if last_checked_block > last_block:
            logger.exception(f'Last checked block {last_checked_block} is bigger than last block {last_block} in blockchain')
        elif last_checked_block == last_block - 2:
            pass
        else:      
            list_accounts = set(get_all_accounts()) 
            for x in range(last_checked_block + 1, last_block):
                logger.warning(f"now checking block {x}")                
                block = w3.eth.getBlock(x, True)       
                for transaction in block.transactions:
                    if transaction['to'] in list_accounts or transaction['from'] in list_accounts:
                        handle_event(transaction)
                        walletnotify_flexcryptopay.delay(config["COIN_SYMBOL"], transaction['hash'].hex())
                        if ((transaction['to'] in list_accounts and transaction['from']  not in list_accounts) and 
                            ((w3.eth.block_number - x) < 40)):
                            drain_account.delay(config["COIN_SYMBOL"], transaction['to'])

                
                for token in config['TOKENS'][config["CURRENT_BNB_NETWORK"]].keys():
                    token_instance  = Token(token)
                    transfers = token_instance.get_all_transfers(x, x)
                    for transaction in transfers:
                        if (token_instance.provider.toChecksumAddress(transaction['from']) in list_accounts or 
                            token_instance.provider.toChecksumAddress(transaction['to']) in list_accounts):
                            handle_event(transaction)
                            walletnotify_flexcryptopay.delay(token, transaction['txid'])
                            if ((token_instance.provider.toChecksumAddress(transaction['from']) not in list_accounts and 
                                token_instance.provider.toChecksumAddress(transaction['to']) in list_accounts) and 
                                ((w3.eth.block_number - x) < 40)):
                                drain_account.delay(token, token_instance.provider.toChecksumAddress(transaction['to']))
                
                last_checked_block = x # TODO store this value in database

                pd = Settings.query.filter_by(name = "last_block").first()
                pd.value = x

                with app.app_context():
                    db.session.add(pd)
                    db.session.commit()
                    db.session.close()
    
        time.sleep(check_interval)

def events_listener():

    from app import create_app
    app = create_app()
    app.app_context().push()

    if (not Settings.query.filter_by(name = "last_block").first()) and (config['LAST_BLOCK_LOCKED'].lower() != 'true'):
        logger.warning(f"Changing last_block to a last block on a fullnode, because cannot get it in DB")
        with app.app_context():
            db.session.add(Settings(name = "last_block", 
                                         value = w3.eth.block_number))
            db.session.commit()
            db.session.close() 
            db.session.remove()
            db.engine.dispose()
    
    while True:
        try:
            pd = Wallets.query.all()
            pd2 = Accounts.query.all()
            if ((not pd) and pd2) or (config['FORCE_ADD_WALLETS_TO_DB'].lower() == 'true'):
                logger.warning(f"Wallets should be moved to the database. Creating task.")
                from .tasks import move_accounts_to_db
                move_accounts_to_db.delay()
            pd = Settings.query.filter_by(name = "last_block").first()
            last_checked_block = int(pd.value)

            log_loop(last_checked_block, int(config["CHECK_NEW_BLOCK_EVERY_SECONDS"]))
        except Exception as e:
            sleep_sec = 60
            logger.exception(f"Exception in main block scanner loop: {e}")
            logger.warning(f"Waiting {sleep_sec} seconds before retry.")           
            time.sleep(sleep_sec)


