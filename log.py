#! /usr/bin/env python3

import psycopg2

# Here goal defines the name of the quary
def execute(goal):
    """ This function connects to the database,
    runs quary,and returns the result """
    try:
        conn = psycopg2.connect('dbname='+"news")
        cursor = conn.cursor()
        cursor.execute(goal)
        d = cursor.fetchall()
        conn.close()
        return d
    except:
        print("Failed to return to database postgresql")
        return None
    else:
        return cursor
    


def Top3articles():
    """This function returns the top three popular reading articles"""


    # To Print output
    print('\nTop three articles of all time:')
    output = execute("""  
        select title,count(*) as num from articles,log where
        log.path=CONCAT('/article/',articles.slug) group by
        articles.title order by
        num DESC limit 3; 
    """
    )
    for result in output:
               print ("%s: %s views" % (result[0],result[1]))

    """This function returns the top three most popular authors"""
def Top3authors():

    # To run this Query
    output = execute( """
       SELECT authors.name,count(*) FROM log,articles,authors
       WHERE  log.path = '/article/' || articles.slug
       AND articles.author = authors.id
       GROUP BY authors.name
       ORDER BY count(*) DESC;
          """)

    # To Print the output
    print('\nTop three authors of all time by views:')
    for r in output:
                print ("%s: %s views" % (r[0], r[1]))

def with_errors():
    """This function returns days with more than 1% errors"""
    # To Print the output
    print('\nDays with more than 1% error:')
    output = execute( """
    select * from (select date(time),round(100.0*sum(case log.status
    when '200 OK'  then 0 else 1 end)/count(log.status),3)
    as error from log group
    by date(time) order by error desc) as subq where error > 1;
                   """)
    for result in output:
         print (" %s: %s views" % (result[0], result[1]))

print('Executing The Results please wait...\n')
Top3articles()
Top3authors()
with_errors()
