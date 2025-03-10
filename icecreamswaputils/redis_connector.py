from collections.abc import KeysView
import json
import os
import socket
from enum import Enum
from typing import Optional

import platform
import redis

from .safe_thread import SafeThread
from .callback_registry import CallbackRegistry


class Serialization(Enum):
    JSON = 1


class RedisConnector(CallbackRegistry):
    def __init__(
            self,
            redis_host: str = 'localhost',
            redis_port: int = 6379,
            redis_db: int = 0,
            redis_password: str = None,
            redis_username: str = None,
            redis_use_ssl: bool = False,
            ssl_cert_reqs: str = 'required',
            health_check_interval: int = 30
    ):
        super().__init__()
        if platform.system() == 'Darwin':
            keep_alive_options = {
                socket.TCP_KEEPALIVE: 60,
                socket.TCP_KEEPINTVL: 5,
                socket.TCP_KEEPCNT: 3,
            }
        else:
            keep_alive_options = {
                socket.TCP_KEEPIDLE: 60,
                socket.TCP_KEEPINTVL: 5,
                socket.TCP_KEEPCNT: 3,
            }
        self.r = redis.Redis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            username=redis_username,
            password=redis_password,
            ssl=redis_use_ssl,
            ssl_cert_reqs=ssl_cert_reqs,
            socket_keepalive=True,
            socket_keepalive_options=keep_alive_options,
            health_check_interval=health_check_interval
        )
        self.r.ping()

        self.hash_data: dict[str, dict[str]] = {}
        self.hash_frozen: dict[str, bool] = {}

    def __setitem__(self, redis_key: str | tuple[str, str | slice | list[str] | KeysView], data):
        try:
            # web3 package is optional, so don't crash if it's not installed
            from web3.datastructures import AttributeDict
            if isinstance(data, AttributeDict):
                data = data.__dict__
        except ModuleNotFoundError:
            pass

        if isinstance(redis_key, str):
            data_serialized = self.serialize_data(redis_key, data)
            self.r.set(redis_key, data_serialized)
        else:
            # redis hash
            key, hkey = redis_key
            if isinstance(hkey, str):
                data_serialized = json.dumps(data)
                self.r.hset(key, hkey, data_serialized)
                updated_hashes = [hkey]
            elif isinstance(hkey, slice):
                mapping = {key: json.dumps(value) for key, value in data.items()}
                # overwrite mapping in redis
                with self.r.pipeline() as pipe:
                    pipe.delete(key)
                    if len(mapping) != 0:
                        pipe.hset(key, mapping=mapping)
                    pipe.execute()
                updated_hashes = list(mapping.keys())
            else:
                assert len(hkey) == len(data)

                updates: dict = {}
                deletes: list = []
                for redis_hash, value in zip(hkey, data):
                    if value is None:
                        deletes.append(redis_hash)
                    else:
                        updates[redis_hash] = json.dumps(value)

                if len(updates) > 0:
                    self.r.hset(key, mapping=updates)
                if len(deletes) > 0:
                    # remove keys from mapping that have a None value
                    self.r.hdel(key, *deletes)
                updated_hashes = list(hkey)

            # create our own kind of keyspace notifications for individual hash keys
            if len(updated_hashes) > 0:
                self.publish(f"hash_updates:{key}", updated_hashes)

    def __getitem__(self, redis_key: str | tuple[str, str | slice | list[str] | KeysView]):
        if isinstance(redis_key, str):
            data_serialized = self.r.get(redis_key)
            return self.deserialize_data(redis_key, data_serialized)
        else:
            # redis hash
            key, hkey = redis_key

            if key in self.hash_frozen and key not in self.hash_data:
                print(f"RedisConnector subscribed to hash {key}, but no data is in cache")

            if isinstance(hkey, str):
                if key in self.hash_data:
                    return self.hash_data[key][hkey]
                return json.loads(self.r.hget(key, hkey))
            elif isinstance(hkey, slice):
                assert hkey.start is None and hkey.step is None and hkey.stop is None, "only : allowed"
                if key in self.hash_data:
                    return self.hash_data[key]
                data_raw = self.r.hgetall(key)
                return {idx.decode(): json.loads(value) for idx, value in data_raw.items()}
            else:
                if key in self.hash_data:
                    data = self.hash_data[key]
                    return {single_hkey: data.get(single_hkey) for single_hkey in hkey}
                values_serialized = self.r.hmget(key, hkey)
                assert len(hkey) == len(values_serialized)
                return {idx: json.loads(value) if value is not None else None for idx, value in zip(hkey, values_serialized)}

    def publish(self, redis_key: str, data):
        data_serialized = RedisConnector.to_json(data)
        self.r.publish(channel=redis_key, message=data_serialized)

    def subscribe(self, redis_key: str, decode: bool = False, channel: Optional[str] = None) -> SafeThread:
        thread = SafeThread(
            target=self._subscribe_thread,
            kwargs=dict(
                redis_key=redis_key,
                decode=decode,
                channel=channel
            ),
            name=f"redis_hash_subscriber_{redis_key}",
        )
        thread.start()
        return thread

    def _subscribe_thread(self, redis_key: str, decode: bool = False, channel: Optional[str] = None):
        pubsub = self.r.pubsub(ignore_subscribe_messages=True)

        if "*" in redis_key:
            pubsub.psubscribe(redis_key)
        else:
            pubsub.subscribe(redis_key)

        for message in pubsub.listen():
            if decode:
                data = self.deserialize_data(message["channel"].decode(), message["data"])
                self._on_new_data(data, channel=channel)
            else:
                self._on_new_data(message, channel=channel)
        raise Exception("should never return")

    def subscribe_hash(self, redis_key: str, channel: Optional[str] = None, start_frozen=False) -> SafeThread:
        if redis_key in self.hash_frozen:
            raise ValueError(f"Already subscribed to redis hash {redis_key}")
        self.hash_frozen[redis_key] = start_frozen

        # subscribe already to make sure no updates are being lost during loading initial data
        pubsub = self.r.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(f"hash_updates:{redis_key}")

        # load initial data, afterwards delta updates keep it up to date
        data_raw = self.r.hgetall(redis_key)
        self.hash_data[redis_key] = {key_encoded.decode(): json.loads(value_serialized) for key_encoded, value_serialized in data_raw.items()}

        # take care of the delta updates
        thread = SafeThread(
            target=self._subscribe_hash_thread,
            kwargs=dict(
                pubsub=pubsub,
                redis_key=redis_key,
                channel=channel
            ),
            name=f"redis_hash_subscriber_{redis_key}"
        )
        thread.start()

        return thread

    def _subscribe_hash_thread(self, pubsub: redis.client.PubSub, redis_key: str, channel: Optional[str] = None):
        updated_initial = False
        frozen_updates = set()
        while True:
            messages = []
            # if this hash is frozen, check every second if it got unfrozen. If not frozen, simply wait for next update
            message = pubsub.get_message(timeout=1 if self.hash_frozen[redis_key] or not updated_initial else None)
            if message is not None:
                messages.append(message)
                while True:
                    # get all available messages
                    message = pubsub.get_message()
                    if message is None:
                        break
                    messages.append(message)

            # caching frozen state so it does not change during processing
            frozen = self.hash_frozen[redis_key]

            changed_keys: list[str] = []
            if len(messages) != 0:
                # if we got a message and not a timeout, get updates from the message
                for message in messages:
                    changed_keys += json.loads(message["data"])
                changed_keys = list(set(changed_keys))  # deduplicate

            if not frozen and len(frozen_updates) != 0:
                # flush frozen updates
                changed_keys = list(frozen_updates | set(changed_keys))
                frozen_updates = set()

            if not updated_initial and not frozen:
                # send initial update. If hash is frozen, send once it's unfrozen
                self._on_new_data(self.hash_data[redis_key], self.hash_data[redis_key].keys(), channel=channel)
                updated_initial = True

            if len(changed_keys) == 0:
                continue

            if frozen:
                # store updates instead of actually updating things, will get updated once unfrozen
                frozen_updates |= set(changed_keys)
                continue

            changed_values_serialized = self.r.hmget(redis_key, changed_keys)
            for key, value_serialized in zip(changed_keys, changed_values_serialized):
                if value_serialized is None:
                    try:
                        del self.hash_data[redis_key][key]
                    except KeyError:
                        pass
                else:
                    self.hash_data[redis_key][key] = json.loads(value_serialized)
            self._on_new_data(self.hash_data[redis_key], changed_keys, channel=channel)
        raise Exception("should never return")

    def freeze_hash(self, redis_key: str):
        # freezing mainly is for data consistency to not propagate updates during e.g. an initial setup
        # once unfrozen all updates that happened during the freeze are triggered
        self.hash_frozen[redis_key] = True

    def unfreeze_hash(self, redis_key: str):
        self.hash_frozen[redis_key] = False

    def is_hash_frozen(self, redis_key: str) -> bool:
        return self.hash_frozen[redis_key]

    def enable_keyspace_notifications(self):
        if os.getenv("REDIS_NO_KEYSPACE_NOTIFICATIONS_OVERWRITE") is not None:
            return
        self.r.config_set('notify-keyspace-events', 'K$h')

    @staticmethod
    def get_serialization(redis_key: str) -> Serialization:
        # prefix = ":".join(redis_key.split(":")[:-1])
        # serialization = RedisConnector.SERIALIZATION_BY_PREFIX[prefix]
        serialization = Serialization.JSON
        return serialization

    @staticmethod
    def serialize_data(redis_key, data) -> str:
        serialization = RedisConnector.get_serialization(redis_key=redis_key)

        if serialization == Serialization.JSON:
            data_serialized = RedisConnector.to_json(data)
        else:
            raise AttributeError(f"unhandled serialization type: {serialization.name}")

        return data_serialized

    @staticmethod
    def deserialize_data(redis_key: str, data_serialized: bytes):
        serialization = RedisConnector.get_serialization(redis_key=redis_key)

        if serialization == Serialization.JSON:
            data = RedisConnector.from_json(data_serialized)
        else:
            raise AttributeError(f"unhandled serialization type: {serialization.name}")

        return data

    @staticmethod
    def to_json(data) -> str:
        try:
            return json.dumps(data, default=RedisConnector.json_default)
        except Exception as e:
            raise e

    @staticmethod
    def from_json(data_serialized: bytes):
        data = json.loads(data_serialized)
        return data

    @staticmethod
    def json_default(data):
        try:
            # hexbytes is sub dependency of web3, which is optional
            from hexbytes import HexBytes
            if isinstance(data, HexBytes):
                return data.hex()
        except ModuleNotFoundError:
            pass

        try:
            # web3 is optional
            from web3.datastructures import AttributeDict
            if isinstance(data, AttributeDict):
                return data.__dict__
        except ModuleNotFoundError:
            pass

        try:
            # numpy is an optional dependency
            import numpy as np
            if isinstance(data, np.ndarray):
                return data.tolist()
        except ModuleNotFoundError:
            pass
        return data
