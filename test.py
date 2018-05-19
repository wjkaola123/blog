import concurrent.futures
import time

from concurrent.futures import as_completed, wait
from service.article_service import ArticleService
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import config

engine_config = config['database']['engine_url']
engine = create_engine(engine_config, **config['database']["engine_setting"])
config['database']['engine'] = engine
db_poll = sessionmaker(bind=engine)
sess = db_poll()

thread_executor = concurrent.futures.ThreadPoolExecutor(
    config['max_threads_num'])
async_do = thread_executor.submit


def get_count_by_month(db_session):
    sql = r'''select substr(update_time,1, 7) as monthw, 
                        count(id) as num from articles 
                        group by substr(update_time,1, 7)
                        '''
    month_writes = db_session.execute(sql).fetchall()
    return month_writes


def test():
    futures = async_do(ArticleService.get_count_by_month, (sess))
    futures_list = []
    futures_list.append(futures)
    for f in as_completed(futures_list):
        article_month_count = f.result()
        print(dict(article_month_count))


if __name__ == '__main__':
    test()