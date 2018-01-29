# coding=utf-8
import tornado.ioloop
import tornado.gen
import tornadis
import logging

logger = logging.getLogger(__name__)


class PubSubTornadis(object):

    def __init__(self, redis_pub_sub_config, loop=None):
        self.redis_pub_sub_config = redis_pub_sub_config
        if not loop:
            loop = tornado.ioloop.IOLoop.current()
        self.loop = loop
        self.autoconnect = self.redis_pub_sub_config['autoconnect']
        self.client = self.get_client()
        self.pub_client = None
        self.connect_times = 0
        self.max_connect_wait_time = 10

    def get_client(self):
        client = tornadis.PubSubClient(host=self.redis_pub_sub_config['host'], port=self.redis_pub_sub_config['port'],
                                       password=self.redis_pub_sub_config['password'],
                                       autoconnect=self.autoconnect)
        return client

    def get_pub_client(self):
        if not self.pub_client:
            self.pub_client = tornadis.Client(host=self.redis_pub_sub_config['host'],
                                              port=self.redis_pub_sub_config['port'],
                                              password=self.redis_pub_sub_config['password'],
                                              autoconnect=self.autoconnect)
        return self.pub_client

    @tornado.gen.coroutine
    def pub_call(self, msg, *channels):
        pub_client = self.get_pub_client()
        if not pub_client.is_connected():
            yield pub_client.connect()
        if not channels:
            channels = self.redis_pub_sub_config['channels']
        for channel in channels:
            yield pub_client.call("PUBLISH", channel, msg)

    def long_listen(self):
        self.loop.add_callback(self.connect_and_listen, self.redis_pub_sub_config['channels'])

    @tornado.gen.coroutine
    def connect_and_listen(self, channels):
        connected = yield self.client.connect()
        if connected:
            subscribed = yield self.client.pubsub_subscribe(*channels)
            if subscribed:
                self.connect_times = 0
                yield self.first_do_after_subscribed()
                while True:
                    msgs = yield self.client.pubsub_pop_message()
                    try:
                        yield self.do_msg(msgs)
                        if isinstance(msgs, tornadis.TornadisException):
                            # closed connection by the server
                            break
                    except Exception, e:
                        logger.exception(e)
            self.client.disconnect()
        if self.autoconnect:
            wait_time = self.connect_times \
                if self.connect_times < self.max_connect_wait_time else self.max_connect_wait_time
            logger.warn("等待{}s，重新连接redis消息订阅服务".format(wait_time))
            yield tornado.gen.sleep(wait_time)
            self.long_listen()
            self.connect_times += 1

    # override
    @tornado.gen.coroutine
    def first_do_after_subscribed(self):
        logger.info("订阅成功")

    # override
    @tornado.gen.coroutine
    def do_msg(self, msgs):
        logger.info("收到订阅消息"+ str(msgs))
