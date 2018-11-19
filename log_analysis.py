#!/usr/bin/env python3

import psycopg2
from datetime import datetime

DBNAME = "newsdata"
TABLE_ARTICLES = "articles"
TABLE_AUTHORS = "authors"
TABLE_LOG = "log"

VIEW_POP_ARTICLE = "most_pop_articles"
VIEW_POP_PATH = "most_pop_path"

VIEW_MOST_POP_AUTHOR = "most_pop_authors"

VIEW_LOG_STAUS_OK_PER_DAY = "log_ok_per_day"
VIEW_LOG_ERROR_PER_DAY = "log_error_per_day"

path_count = 'path_count'
status_ok = '\'2%\''


# Helper functions
def execute_query(query):
    cursor = db.cursor()
    cursor.execute(query)
    return cursor


def create_view(view_query):
    cursor = db.cursor()
    cursor.execute(view_query)
    cursor.close()


def print_view(view):
    print("----------------")
    print("Printing view - %s" % view)
    query = "select * from %s" % view
    print_all_rows(query)


def print_all_rows(query):
    cursor = execute_query(query)
    for row in cursor.fetchall():
        print(str(row))
    cursor.close()
    print("\n")


def create_views():
    create_popular_path()
    create_popular_article_view()
    create_most_pop_author_view()
    create_log_per_day_view()
    create_errors_per_day_view()
    db.commit()


def drop_all_views():
    cursor = db.cursor()
    drop_view = "drop view if exists %s"
    cursor.execute(drop_view % VIEW_POP_ARTICLE)
    cursor.execute(drop_view % VIEW_POP_PATH)
    cursor.execute(drop_view % VIEW_MOST_POP_AUTHOR)
    cursor.execute(drop_view % VIEW_LOG_ERROR_PER_DAY)
    cursor.execute(drop_view % VIEW_LOG_STAUS_OK_PER_DAY)
    cursor.close()
    db.commit()


def print_all_views():
    print_view(VIEW_POP_PATH)
    print_view(VIEW_POP_ARTICLE)
    print_view(VIEW_MOST_POP_AUTHOR)
    print_view(VIEW_LOG_ERROR_PER_DAY)
    print_view(VIEW_LOG_STAUS_OK_PER_DAY)


# Includes bad url's
def create_popular_path():
    query = """
                create view {0} as
                select path as path, count(*) as {1}
                from log
                group by path
                order by {1} desc
                """.format(VIEW_POP_PATH,
                           path_count)
    create_view(query)


def create_popular_article_view():
    query = """
            create view {0} as
            select {1}.{2} as sum, articles.title, articles.author
            from articles, {1}
            where {1}.path = concat('/article/', articles.slug)
            group by path, articles.title, articles.author, sum
            order by sum desc
            """.format(VIEW_POP_ARTICLE,
                       VIEW_POP_PATH,
                       path_count)
    create_view(query)


def create_most_pop_author_view():
    query = """
            create view {0} as
                select authors.name, count(authors.name) as num
                from authors, log, articles
                where log.path = concat('/article/', articles.slug)
                    and articles.author = authors.id
                group by authors.name
                order by num desc
            """.format(VIEW_MOST_POP_AUTHOR)
    create_view(query)


def query_most_pop_author():
    query = "select * from %s" % VIEW_MOST_POP_AUTHOR
    cursor = execute_query(query)
    print_most_pop_author(cursor)
    cursor.close()


def print_most_pop_author(cursor):
    print("\nQuestion #2: ")
    print("Who are the most popular article authors of all time?")
    for row in cursor.fetchall():
        view_per_author = str(row[1])
        author_name = row[0]
        print('\"' + author_name + '\" -- ' + view_per_author + ' views.')


def create_log_per_day_view():
    query = """
            create view %s as
              select date(time) as day, log.status, count(*) as num
              from log
              where status like %s
              group by date(time), log.status
              order by date(time)
            """ % (VIEW_LOG_STAUS_OK_PER_DAY, status_ok)
    create_view(query)


def create_errors_per_day_view():
    query = """
            create view {0} as
            select date(time) as day, count(*) as errors, log.status
            from {1}
            where log.status not like {2}
            group by date(time), log.status
            order by date(time)
            """.format(VIEW_LOG_ERROR_PER_DAY,
                       TABLE_LOG,
                       status_ok)
    create_view(query)


def query_extensive_error_logs():
    query = """
            select {0}.day, {0}.errors, {1}.num
            from {0}, {1}
            where {0}.day = {1}.day and ({0}.errors * 100) > {1}.num
            order by day
            """.format(VIEW_LOG_ERROR_PER_DAY,
                       VIEW_LOG_STAUS_OK_PER_DAY)
    cursor = execute_query(query)
    print_extensive_errors_logs(cursor)
    cursor.close()


def print_extensive_errors_logs(cursor):
    print("\nQuestion #3: ")
    print("On which days did more than 1% of requests lead to errors?")
    for row in cursor.fetchall():
        percentage = float(100 * row[1]) / float(row[2])
        percentage_str = "%.2f" % percentage
        date = "{:%B %d, %Y}".format(row[0])
        print(date + " -- " + percentage_str + "%% errors.")


def print_most_popular_articles():
    print("\nQuestion #1: ")
    print("What are the most popular three articles of all time?")
    cursor = execute_query("select * from %s limit 3" % VIEW_POP_ARTICLE)
    for row in cursor.fetchall():
        title = row[1]
        count = row[0]
        print('\"' + title + '\"' + " -- " + str(count) + " views.")
    cursor.close()


if __name__ == '__main__':
    db = psycopg2.connect(dbname='news')
    drop_all_views()
    create_views()
    # print_all_views()

    print_most_popular_articles()
    query_most_pop_author()
    query_extensive_error_logs()

    db.close()
