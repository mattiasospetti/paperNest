import psycopg2
import csv

connection = psycopg2.connect(database="souscritootest", user="testread", password="testread", host="souscritootest.cuarmbocgsq7.eu-central-1.rds.amazonaws.com", port=5432)

cursor = connection.cursor()

cursor.execute("" \
"SELECT column_name, data_type \
FROM information_schema.columns \
WHERE table_schema = 'test_sql' \
  AND table_name = 'test_call';")

# Fetch all rows from database
record = cursor.fetchall()

print("Data from Database:- ", record)

cursor.execute("" \
"SELECT column_name, data_type \
FROM information_schema.columns \
WHERE table_schema = 'test_sql' \
  AND table_name = 'test_client';")

# Fetch all rows from database
record = cursor.fetchall()

print("Data from Database:- ", record)


cursor.execute("with cte1 as ( \
select distinct firstname, lastname, phonenumber \
from test_sql.test_client \
) \
select concat(cte1.firstname, ' ', cte1.lastname) as client_full_name, count(*) as nb_calls, to_char( interval '1 second' * sum(calldurationinseconds), 'HH24:MI:SS' ) as total_duration_calls, max(calldate) \
from test_sql.test_call \
join cte1 \
on (cte1.phonenumber::INTEGER = test_call.clientphonenumberin) or (cte1.phonenumber::INTEGER = test_call.clientphonenumberout) \
group by cte1.firstname, cte1.lastname;")

# Fetch all rows from database
record = cursor.fetchall()

print("Data from Database:- ", record)


with open('task2.csv','w', newline='') as out:
    csv_out=csv.writer(out)
    csv_out.writerow(['client_full_name','nb_calls', 'total_duration_calls', 'most_recent_call'])
    for row in record:
        csv_out.writerow(row)