#! /usr/bin/env python

import psycopg2

# Here goal defines the name of the quary


def execute(goal):
    """This function connects to the database, runs quary,
    and returns the result"""
    conn = psycopg2.connect('dbname='+"news")
    cursor = conn.cursor()
    cursor.execute(goal)
    d = cursor.fetchall()
    conn.close()
    return d


def Top3articles():
    """This function returns the top 3 most reading articles"""

    # The Required Query for top 3 articles
    goal = """
        SELECT articles.title, COUNT(*) AS num
        FROM articles
        JOIN log
        ON log.path LIKE concat('/article/%', articles.slug)
        GROUP BY articles.title
        ORDER BY num DESC
        LIMIT 3;
    """

    # To Run this Query
    output = execute(goal)

    # To Print output
    print('\nTOP THREE ARTICLES OF ALL TIME:')
    count = 1
    for r in output:
        n = '(' + str(count) + ') "'
        t = r[0]
        v = '" with ' + str(r[1]) + " views"
        print(n + t + v)
        count += 1


def Top3authors():
    """This function returns the top 3 most popular authors"""

    # The Required Query for top3 authors
    goal = """
        SELECT authors.name, COUNT(*) AS num
        FROM authors
        JOIN articles
        ON authors.id = articles.author
        JOIN log
        ON log.path like concat('/article/%', articles.slug)
        GROUP BY authors.name
        ORDER BY num DESC
        LIMIT 3;
    """

    # To run this Query
    output = execute(goal)

    # To Print the output
    print('\nTOP THREE AUTHORS OF ALL TIME BY VIEWS:')
    count = 1
    for r in output:
        print('(' + str(count) + ') ' + r[0] + ' with ' + str(r[1]) + " views")
        count += 1


def with_errors():
    """This function returns days with more than 1% errors"""

    # The Required quary for 1% errors
    goal = """
        SELECT total.day,
          ROUND(((errors.error_requests*1.0) / total.requests), 3) AS percent
        FROM (
          SELECT date_trunc('day', time) "day", count(*) AS error_requests
          FROM log
          WHERE status LIKE '404%'
          GROUP BY day
        ) AS errors
        JOIN (
          SELECT date_trunc('day', time) "day", count(*) AS requests
          FROM log
          GROUP BY day
          ) AS total
        ON total.day = errors.day
        WHERE (ROUND(((errors.error_requests*1.0) / total.requests), 3) > 0.01)
        ORDER BY percent DESC;
    """

    # To run this quary
    output = execute(goal)

    # To Print the output
    print('\nDAYS WITH MORE THAN 1% ERRORS:')
    for r in output:
        Date = r[0].strftime('%B %d, %Y')
        err = str(round(r[1]*100, 1)) + "%" + " errors"
        print(Date + " -- " + err)

print('Executing The Results please wait...\n')
Top3articles()
Top3authors()
with_errors()
