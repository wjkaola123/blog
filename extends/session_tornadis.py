# coding: utf-8
import uuid
import json
import tornadis
import tornado.gen
import logging

logger = logging.getLogger(__name__)


class Session(dict):
    def __init__(self, request_handler):
        super(Session, self).__init__()
        self.session_id = None
        self.session_manager = request_handler.application.session_manager
        self.request_handler = request_handler
        self.client = None

    @tornado.gen.coroutine
    def init_fetch(self):
        self.client = yield self.session_manager.get_redis_client()
        yield self.fetch_client()

    def get_session_id(self):
        if not self.session_id:
            self.session_id = self.request_handler.get_secure_cookie(self.session_manager.session_key_name)
        return self.session_id

    def generate_session_id(self):
        if not self.get_session_id():
            self.session_id = str(uuid.uuid1())
            self.request_handler.set_secure_cookie(self.session_manager.session_key_name, self.session_id,
                                                   expires_days=self.session_manager.session_expires_days)
        return self.session_id

    @tornado.gen.coroutine
    def fetch_client(self):
        if self.get_session_id():
            data = yield self.call_client("GET", self.session_id)
            if data:
                self.update(json.loads(data))

    @tornado.gen.coroutine
    def save(self, expire_time=None):
        session_id = self.generate_session_id()
        data_json = json.dumps(self)
        yield self.call_client("SET", session_id, data_json)
        if expire_time:
            yield self.call_client("EXPIRE", session_id, expire_time)

    @tornado.gen.coroutine
    def call_client(self, *args, **kwargs):
        if self.client:
            reply = yield self.client.call(*args, **kwargs)
            if isinstance(reply, tornadis.TornadisException):
                logger.error(reply.message)
            else:
                raise tornado.gen.Return(reply)


class SessionManager(object):
    def __init__(self, options):
        self.connection_pool = None
        self.options = options
        self.session_key_name = options['session_key_name']
        self.session_expires_days = options['session_expires_days']

    def get_connection_pool(self):
        if not self.connection_pool:
            self.connection_pool = tornadis.ClientPool(host=self.options['host'],port=self.options['port'],
                                                       password=self.options['password'], db=self.options['db_no'],
                                                       max_size=self.options['max_connections'])
        return self.connection_pool

    @tornado.gen.coroutine
    def get_redis_client(self):
        connection_pool = self.get_connection_pool()
        with (yield connection_pool.connected_client()) as client:
            if isinstance(client, tornadis.TornadisException):
                logger.error(client.message)
            else:
                raise tornado.gen.Return(client)
