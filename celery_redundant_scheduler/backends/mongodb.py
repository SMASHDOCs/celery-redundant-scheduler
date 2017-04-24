# -*- coding: utf-8 -*-

from __future__ import absolute_import

from celery.exceptions import ImproperlyConfigured

from datetime import datetime, timedelta

import time

import os

from celery.utils.log import get_logger

from .base import BaseBackend

logger = get_logger(__name__)

debug, info, error, warning = (logger.debug, logger.info,
                               logger.error, logger.warning)

try:
    import pymongo
    MongoClient = pymongo.MongoClient
except ImportError:
    MongoClient = None


class Backend(BaseBackend):
    def __init__(self, *args, **kwargs):
        super(Backend, self).__init__(*args, **kwargs)
        self.collection = getattr(self.app.conf, 'CELERYBEAT_REDUNDANT_MONGO_COLLECTION', 'redundant-scheduler')
        self.lock_expires = getattr(self.app.conf, 'CELERYBEAT_REDUNDANT_MONGO_LOCK_EXPIRES', 60)
        self._lock = False
        self.lock_count = 0

    def get_connection(self):
        if MongoClient is None:
            raise ImproperlyConfigured('`Pymongo `library is not installed')

        if self.options.get("URL") is not None:
            info("Using URL" + self.options.get("URL"))
            client = MongoClient(self.options.get("URL"))
            database = client.get_default_database()
        else:
            info("Using options {}".format(self.options))
            client = MongoClient(host=self.options.get("HOST"), port=self.options.get("PORT"))
            database = client.get_database(self.options.get("DB"))

        try:
            database[self.collection].lock.drop_index("created_1")
        except pymongo.errors.OperationFailure:
            pass
        
        database[self.collection].lock.create_index("created", expireAfterSeconds=self.lock_expires)
        database[self.collection].lock.create_index("key", unique=True)

        return database[self.collection]

    def lock(self):
        try:
            self.connection.lock.insert({"key": "lock", "created": datetime.utcnow()})
            self._lock = True
        except pymongo.errors.DuplicateKeyError:
            self._lock = False
            time.sleep(15)

        self.lock_count += 1

    def unlock(self):
        if self.lock_count > 0:
            self.lock_count -= 1

        if self._lock and self.lock_count <= 0:
            self.connection.lock.delete_one({"key": "lock"})
            self._lock = False

    def get(self, key):
        result = self.connection.find_one({"key": key})
        if result is not None:
            return result["last_run"]

    def set(self, key, value):
        res = self.connection.update({"key": key}, {"key": key, "last_run": value}, upsert=True)
        return res

