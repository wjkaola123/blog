# coding=utf-8


class BaseService(object):
    @staticmethod
    def query_pager(query, pager, count=None):
        if count:
            pager.set_total_count(count)
        else:
            pager.set_total_count(query.count())
        query_result = pager.build_query(query)
        pager.set_result(query_result.all())
        return pager
