import random
import threading
import time
from dateutil.relativedelta import relativedelta
from datetime import datetime
import itertools as it

from src import crypto_client
from configs.data_collector_config import settings
from src.data_collector.database_collection_manager import DatabaseCollectionManager


class WebsocketDataCollectionEvent(crypto_client.WebsocketEvent):
    """
    This class represents a WebSocket event handler responsible for collecting,
    processing and managing the data from the websocket events.
    """
    def __init__(self, collection_manager: DatabaseCollectionManager, product_ids: list, is_snapshot=False, logger=None):
        """
        Initializes the WebsocketDataCollectionEvent instance.

        Args:
            collection_manager (DatabaseCollectionManager): Manager that interacts with the database.
            product_ids (list): A list of ids for the products.
            is_snapshot (bool, optional): Determines whether to collect orderbook snapshots. Defaults to False.
            logger (logging.Logger, optional): Logger for logging information. Defaults to None.
        """
        if logger is not None:
            self.logger = logger
        self._collectionManager = collection_manager
        self._rest_client = self.get_live_rest_client()
        self._sequence = {product_id: -1 for product_id in product_ids}
        self.product_ids = product_ids
        self._orderbook_params = {'level': 3}
        self._ob_call_count = 0
        self._ob_prev_call_time = datetime.utcnow()
        self.collect_orderbook_thread = None
        if is_snapshot:
            self.collect_orderbook_thread = threading.Thread(
                target=self.collect_orderbook_snapshots, daemon=False)
            self.collect_orderbook_thread.start()
        self.packet_rate_count = 0
        self.packet_rate_prev_time = datetime.utcnow()
        self.last_match_trade_id = {product_id: None for product_id in product_ids}
        self.is_seq_gap = False
        self.last_rest_call_time = datetime.utcnow().timestamp()

    def get_sequence(self):
        """
        Returns the current sequence dictionary of product_ids.

        Returns:
            dict: The sequence dictionary.
        """
        return self._sequence

    def set_sequence(self, p_id, seq):
        """
        Sets the sequence number for a particular product id.

        Args:
            p_id (str): The product id.
            seq (int): The sequence number.
        """
        self._sequence[p_id] = seq

    def get_last_match_trade_id(self):
        """
        Returns the dictionary of last match trade ids.

        Returns:
            dict: The last match trade id dictionary.
        """
        return self.last_match_trade_id

    def set_last_match_trade_id(self, p_id, seq):
        """
        Sets the last match trade id for a particular product id.

        Args:
            p_id (str): The product id.
            seq (int): The last match trade id.
        """
        self.last_match_trade_id[p_id] = seq

    def get_live_rest_client(self):
        """
        Returns an authenticated REST client instance.

        Returns:
            PrivateClient: A live REST client object.
        """
        auths = crypto_client.Auth(settings["cb_key"], settings["cb_secret"], settings["cb_passphrase"])

        api_url = settings['rest_url']
        messenger = crypto_client.Messenger(auths=auths, url=api_url)
        private = crypto_client.PrivateClient(messenger)
        return private

    def collect_orderbook_snapshots(self):
        """
        Collects orderbook snapshots at regular intervals depending on the environment setting.
        """
        if settings["is_production"]:
            interval_delta = relativedelta(
                minutes=settings["snapshot_interval_minutes"], second=0, microsecond=0)
            current_time = datetime.utcnow()
            next_snapshot = current_time
            next_snapshot = next_snapshot.replace(
                minute=current_time.minute // settings["snapshot_interval_minutes"] * settings["snapshot_interval_minutes"])
            next_snapshot += interval_delta
        else:
            interval_delta = relativedelta(
                minutes=0, seconds=settings["snapshot_interval_seconds"], microsecond=0)
            current_time = datetime.utcnow()
            next_snapshot = current_time
            next_snapshot = next_snapshot.replace(
                second=current_time.second // settings["snapshot_interval_seconds"] * settings["snapshot_interval_seconds"])
            next_snapshot += interval_delta

        interval = (next_snapshot - current_time).total_seconds()
        if interval > 0:
            time.sleep(interval)

        while True:
            self.logger.info("start snapshot {}".format(self.product_ids))
            for prod_id in self.product_ids:
                res = self.get_orderbook(prod_id=prod_id)
                res['time'] = next_snapshot.isoformat()
                res['product_id'] = prod_id
                self._collectionManager.insert_snapshot(res)
            next_snapshot += interval_delta
            cur_time = datetime.utcnow()

            interval = (next_snapshot - cur_time).total_seconds()
            if interval > 0:
                time.sleep(interval)

    def on_listen(self, client: object, value: dict) -> None:
        """
        Listener event method that gets executed when a client is listening.

        Args:
            client (object): The client object.
            value (dict): A dictionary containing the event data.
        """
        pass

    def on_response(self, value: dict) -> None:
        """
        Handles the response event.

        Args:
            value (dict): A dictionary containing the response data.
        """
        # print(f'[Response] {value}')
        sequence = value.get('sequence', -1)
        prod_id = value.get('product_id', -1)
        self.packet_rate_count += 1
        if prod_id != -1:
            if self._sequence[prod_id] == -1:
                self.reset_book(prod_id)
                return
            if sequence < self._sequence[prod_id]:
                return
            elif sequence > self._sequence[prod_id] + 1:
                self.on_sequence_gap(
                    self._sequence[prod_id], sequence, prod_id)
                self.is_seq_gap = True
                return

            if value['type'] == "match":
                # if prod_id == "BTC-USDT":
                #     self.logger.debug(f'[Response] {value}')
                # if prod_id == "BTC-USDT":
                #     self.logger.debug(f"match seq gap before: {self.last_match_trade_id[prod_id]} {value['trade_id']}")
                if self.is_seq_gap:
                    self.is_seq_gap = False
                    if self.last_match_trade_id[prod_id] is not None and value['trade_id'] <= self.last_match_trade_id[prod_id]:
                        return
                # if prod_id == "BTC-USDT":
                #     self.logger.debug(f"match seq gap: {self.last_match_trade_id[prod_id]} {value['trade_id']}")
                #     t = datetime.strptime(value['time'], '%Y-%m-%dT%H:%M:%S.%fZ')
                #     nt = datetime.utcnow()
                #     ts = (nt - t).total_seconds()
                #     self.logger.info(f"packet lag: {ts} {t} {nt}")
                self.last_match_trade_id[prod_id] = value['trade_id']

            self._collectionManager.insert(value)

            self._sequence[prod_id] = sequence
            # print("full {}".format(sequence), end=' ', flush=True)

    def on_collection(self, collection: object, value: dict) -> None:
        """
        Handles the collection event.

        Args:
            collection (object): The collection object.
            value (dict): A dictionary containing the event data.
        """
        pass

    def on_sequence_gap(self, gap_start, gap_end, prod_id):
        """
        Handles sequence gaps.

        Args:
            gap_start (int): The sequence number where the gap starts.
            gap_end (int): The sequence number where the gap ends.
            prod_id (str): The product id.
        """
        self.reset_book(prod_id)
        # self.logger.error(f'Messages missing get trades')
        self.get_missing_trades(prod_id)
        cur_time = datetime.utcnow()
        packet_rate = self.packet_rate_count / \
            ((cur_time - self.packet_rate_prev_time).total_seconds() + 1)
        self.logger.error(f'Messages missing {prod_id} ({gap_start} - {gap_end}). Re-initializing  book at sequence '
                          f'{self._sequence[prod_id]}.\n Packet rate {packet_rate} packets/sec')
        self.packet_rate_count = 0
        self.packet_rate_prev_time = cur_time

    def reset_book(self, prod_id):
        """
        Resets the order book for a particular product id.

        Args:
            prod_id (str): The product id.
        """
        tm = datetime.utcnow().isoformat()
        res_ob = self.get_orderbook(prod_id=prod_id)
        res_ob['time'] = tm
        res_ob['product_id'] = prod_id
        self._collectionManager.insert(res_ob)
        self._sequence[prod_id] = res_ob['sequence']
        self.logger.info(
            f"reset_book seq gap: {prod_id} {self._sequence[prod_id]}")

    def get_missing_trades(self, prod_id):
        """
        Fetches and handles missing trades for a particular product id.

        Args:
            prod_id (str): The product id.
        """
        if self.last_match_trade_id[prod_id] is None:
            return
        result_gen = self.get_trades(prod_id=prod_id)
        # result_gent = self.get_trades(prod_id=prod_id)
        trades = list(it.takewhile(lambda x: x['trade_id'] > self.last_match_trade_id[prod_id], result_gen))
        # tradesh = list(it.islice(result_gent, 100))
        if trades:
            # if prod_id == "BTC-USDT":
            #     # self.logger.info(trades)
            self.logger.info(f"get_missing_trades seq gap: {prod_id} {self.last_match_trade_id[prod_id]} {trades[0]['trade_id']}")
            self.last_match_trade_id[prod_id] = trades[0]['trade_id']

            entry = {
                'product_id': prod_id,
                'trades': trades
            }

            self._collectionManager.insert(entry)

    def get_trades(self, prod_id):
        """
        Fetches the trades for a particular product id.

        Args:
            prod_id (str): The product id.

        Returns:
            list: A list of trades.
        """
        no_of_retries = 3
        ob = None
        tm = datetime.utcnow().timestamp()
        interval = tm - self.last_rest_call_time
        # self.logger.debug(f"rest interval: {interval}")
        self.last_rest_call_time = tm
        if interval > 30:
            self._rest_client = self.get_live_rest_client()

        for i in range(0, no_of_retries):
            try:
                ob = self._rest_client.products.trades(prod_id, {'limit': 100})
                self.logger.info("trades received {}".format(prod_id))
                break
            except Exception as ex:
                self.logger.exception(f"Get trades failed for {prod_id}. Attempts: {i + 1}. Error: {ex}\n", exc_info=True)
                self._rest_client = self.get_live_rest_client()
                if i >= no_of_retries - 1:
                    raise ex
                continue
        return ob

    def get_orderbook(self, prod_id):
        """
        Retrieves the order book for a specific product id.

        Args:
            prod_id (str): The product id.

        Returns:
            dict: The order book.
        """
        no_of_retries = 3
        ob = None
        tm = datetime.utcnow().timestamp()
        interval = tm - self.last_rest_call_time
        # self.logger.debug(f"rest interval: {interval}")
        self.last_rest_call_time = tm
        if interval > 30:
            self._rest_client = self.get_live_rest_client()

        for i in range(0, no_of_retries):
            try:
                ob = self._rest_client.products.order_book(prod_id, self._orderbook_params)
                self.logger.info("snapshot received {}".format(prod_id))
                break
            except Exception as ex:
                self.logger.exception(f"Get orderbook failed for {prod_id}. Attempts: {i + 1}. Error: {ex}\n", exc_info=True)
                self._rest_client = self.get_live_rest_client()
                if i >= no_of_retries - 1:
                    raise ex
                continue
        return ob

    def update(self, args: dict) -> None:
        """
        Updates the instance based on the arguments passed.

        Args:
            args (dict): A dictionary containing the update parameters.
                         Keys could be 'stop_program', 'new_product_ids', or 'old_product_ids'.
        """
        is_stop_program = args.get('stop_program', -1) != -1
        new_product_ids = args.get('new_product_ids', -1)
        old_product_ids = args.get('old_product_ids', -1)
        if is_stop_program:
            stop_time = args.get('stop_time', -1)
            self._collectionManager.stop_collection(stop_time=stop_time)
        elif new_product_ids != -1:
            for prod_id in new_product_ids:
                self.product_ids.append(prod_id)
                self._sequence[prod_id] = -1
        elif old_product_ids != -1:
            for prod_id in old_product_ids:
                if prod_id in self.product_ids:
                    self.product_ids.remove(prod_id)

    def is_stopped(self) -> bool:
        """
        Checks whether the collection manager has stopped.

        Returns:
            bool: True if the collection manager has stopped, False otherwise.
        """
        return self._collectionManager.is_stopped()

    def on_stop(self):
        """
        Closes the collection manager when the WebsocketDataCollectionEvent is stopped.
        """
        self._collectionManager.close()
