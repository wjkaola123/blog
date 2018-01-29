# coding=utf-8
import logging
import re

from model.site_info import SiteCollection
from sqlalchemy.orm import joinedload, undefer
from model.models import Article, Source
from model.constants import Constants
from model.search_params.article_params import ArticleSearchParams
from . import BaseService
from comment_service import CommentService

logger = logging.getLogger(__name__)


class ArticleService(object):
    MARKDOWN_REG = "[\\\`\*\_\[\]\#\+\-\!\>\s]";
    SUMMARY_LIMIT = 120;

    @staticmethod
    def get_article_all(db_session, article_id, show_source_type=False, add_view_count=None):
        query = db_session.query(Article);
        if show_source_type:
            query = query.options(joinedload(Article.source)).\
                options(joinedload(Article.articleType).load_only("id", "name"))
        article = query.options(undefer(Article.summary), undefer(Article.content), undefer(Article.update_time)).\
            get(article_id)
        if article and add_view_count:
            article.num_of_view = Article.num_of_view + add_view_count
            db_session.commit()
        return article

    @staticmethod
    def page_articles(db_session, pager, search_params):
        query = db_session.query(Article)
        count = SiteCollection.article_count
        if search_params:
            if search_params.show_comments_count:
                stmt = CommentService.get_comments_count_subquery(db_session)
                query = db_session.query(Article, stmt.c.comments_count).\
                    outerjoin(stmt, Article.id == stmt.c.article_id)
            if search_params.show_summary:
                query = query.options(undefer(Article.summary))
            if search_params.show_content:
                query = query.options(undefer(Article.content))
            if search_params.show_source:
                query = query.options(joinedload(Article.source))
            if search_params.show_article_type:
                query = query.options(joinedload(Article.articleType).load_only("id", "name"))
            if search_params.order_mode == ArticleSearchParams.ORDER_MODE_CREATE_TIME_DESC:
                query = query.order_by(Article.create_time.desc())
            if search_params.source_id:
                count = None
                query = query.filter(Article.source_id == search_params.source_id)
            if search_params.articleType_id:
                count = None
                query = query.filter(Article.articleType_id == search_params.articleType_id)
        pager = BaseService.query_pager(query, pager, count)
        if pager.result:
            if search_params.show_comments_count:
                result = []
                for article, comments_count in pager.result:
                    article.fetch_comments_count(comments_count if comments_count else 0)
                    result.append(article)
                pager.result = result
        return pager

    @staticmethod
    def add_article(db_session, article):
        try:
            summary = article["summary"].strip() if article["summary"] else None
            if not summary:
                summary = ArticleService.get_core_content(article["content"], ArticleService.SUMMARY_LIMIT)
            article_to_add = Article(title=article["title"], content=article["content"],
                                     summary=summary, articleType_id=article["articleType_id"],
                                     source_id=article["source_id"])
            db_session.add(article_to_add)
            db_session.commit()
            return article_to_add
        except Exception, e:
            logger.exception(e)
        return None

    @staticmethod
    def update_article(db_session, article):
        try:
            summary = article["summary"].strip() if article["summary"] else None
            if not summary:
                summary = ArticleService.get_core_content(article["content"], ArticleService.SUMMARY_LIMIT)
            article_to_update = ArticleService.get_article_all(db_session, article["id"])
            article_old = Article(title=article_to_update.title, content=article_to_update.content,
                                  summary=article_to_update.summary, articleType_id=article_to_update.articleType_id,
                                  source_id=article_to_update.source_id)
            article_to_update.title = article["title"]
            article_to_update.content = article["content"]
            article_to_update.summary = summary
            article_to_update.articleType_id = int(article["articleType_id"]) if article["articleType_id"] else None
            article_to_update.source_id = int(article["source_id"]) if article["source_id"] else None
            db_session.commit()
            return article_to_update, article_old
        except Exception, e:
            logger.exception(e)
        return None

    @staticmethod
    def delete_article(db_session, article_id):
        try:
            article = db_session.query(Article).get(article_id)
            if article:
                comments_deleted = CommentService.remove_by_article_id(db_session, article_id, False)
                db_session.delete(article)
                db_session.commit()
            return article, comments_deleted;
        except Exception, e:
            logger.exception(e)
        return None

    @staticmethod
    def get_core_content(content, limit=0):
        core_content = re.sub(ArticleService.MARKDOWN_REG, '', content)
        if limit > 0:
            return core_content[:limit]
        return core_content

    @staticmethod
    def get_count(db_session):
        article_count = db_session.query(Article).count()
        return article_count

    # article_sources
    @staticmethod
    def get_article_sources(db_session):
        article_sources = db_session.query(Source).all()
        if article_sources:
            for source in article_sources:
                source.fetch_articles_count()
        return article_sources

    @staticmethod
    def set_article_type_default_by_article_type_id(db_session, article_type_id, auto_commit=True):
        try:
            db_session.query(Article).filter(Article.articleType_id == article_type_id).\
                update({Article.articleType_id: Constants.ARTICLE_TYPE_DEFAULT_ID})
            if auto_commit:
                db_session.commit()
        except Exception, e:
            logger.exception(e)
