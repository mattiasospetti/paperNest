import psycopg2
import json

connection = psycopg2.connect(database="souscritootest", user="testread", password="testread", host="souscritootest.cuarmbocgsq7.eu-central-1.rds.amazonaws.com", port=5432)

cursor = connection.cursor()

'''
cursor.execute("" \
"SELECT * from test_sql.test_call limit 1;")

# Fetch all rows from database
record = cursor.fetchall()

print("Data from Database:- ", record)
'''

cursor.execute("with cte1 as( \
    select id, clientphonenumberin, clientphonenumberout, \
    case \
        when calldate BETWEEN '2023/01/01' and '2023/03/30' then 'Q1_2023' \
        when calldate BETWEEN '2023/04/01' and '2023/06/30' then 'Q2_2023' \
        else null \
    end quarter\
    from test_sql.test_call \
) \
select quarter, count(*) nb_calls, \
    count(case when (clientphonenumberin is not null and clientphonenumberout is null) or (clientphonenumberin is null and clientphonenumberout is null) then 1 else null end) nb_calls_inbound, \
    count(case when (clientphonenumberin is null and clientphonenumberout is not null) then 1 else null end) nb_calls_outbount \
from cte1 \
where quarter is not null \
group by quarter;")


# Fetch all rows from database
record = cursor.fetchall()

print("Data from Database:- ", record)

res = {}

for elem in record:
    res[elem[0]] = {
        'nb_calls': elem[1],
        'nb_calls_inbound': elem[2],
        'nb_calls_outbound': elem[3]
    }


json_data = json.dumps(res)

with open("task1.json", "w") as f:
    f.write(json_data)