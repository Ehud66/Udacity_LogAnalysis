# Udacity_LogAnalysis

## Questions
1. What are the most popular three articles of all time? Which articles have been accessed the most? Present this information as a sorted list with the most popular article at the top.

    Example:

        "Princess Shellfish Marries Prince Handsome" — 1201 views
        "Baltimore Ravens Defeat Rhode Island Shoggoths" — 915 views
        "Political Scandal Ends In Political Scandal" — 553 views
2. Who are the most popular article authors of all time? That is, when you sum up all of the articles each author has written, which authors get the most page views? Present this as a sorted list with the most popular author at the top.

    Example:

        Ursula La Multa — 2304 views
        Rudolf von Treppenwitz — 1985 views
        Markoff Chaney — 1723 views
        Anonymous Contributor — 1023 views
3. On which days did more than 1% of requests lead to errors? The log table includes a column status that indicates the HTTP status code that the news site sent to the user's browser. (Refer to this lesson for more information about the idea of HTTP status codes.)

    Example:

        July 29, 2016 — 2.5% errors
## Requirements
* Python 3.6.5
* psycopg2

## How to run
* load the data onto the database
```sql
psql -d news -f newsdata.sql
```
* connect to the database
```sql
psql -d news
```
* create views - uncomment create_views() call in __init__ in order to create views via code.
* python3 log_analysis.py

### Create Views
View - Most popular path
```sql
create view most_pop_path as
select path as path, count(*) as path_count
from log
group by path
order by path_count desc;
```

View - Most popular articles
```sql
create view most_pop_articles as
select most_pop_path.path_count as sum, articles.title, articles.author
from articles, most_pop_path
where most_pop_path.path = concat('/article/', articles.slug)
group by path, articles.title, articles.author, sum
order by sum desc
```

View - Most popular authors
```sql
create view most_pop_authors as
select authors.name, count(authors.name) as num
from authors, log, articles
where log.path = concat('/article/', articles.slug)
	and articles.author = authors.id
group by authors.name
order by num desc
```

View - Log status Ok per day
```sql
create view log_ok_per_day as
select date(time) as day, log.status, count(*) as num
from log
where status like '2%'
group by date(time), log.status
order by date(time)
```

View - Log status Errors per day
```sql
create view log_error_per_day as
select date(time) as day, count(*) as errors, log.status
from log
where log.status not like '2%'
group by date(time), log.status
order by date(time)
```