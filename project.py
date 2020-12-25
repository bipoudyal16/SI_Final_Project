import psycopg2
import boto3
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import connection

 

try:
   #boto3 Got informtion from https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs.html
   sqs = boto3.resource('sqs',aws_access_key_id =  'AKIAWNKTUM4AU2PABW23',
                        aws_secret_access_key='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', region_name='us-west-2')
   queue = sqs.create_queue(QueueName='landmarks', Attributes={'DelaySeconds': '5'})

   #connecting to postgis https://www.postgresqltutorial.com/postgresql-python/connect/

   connection = psycopg2.connect(user="postgres",
                            password="admin",
                            host="127.0.0.1")

   connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT);
   cursor = connection.cursor()
   cursor.execute("drop database if exists landmarks;")
   createdatabase = """create database landmarks; """
   cursor.execute(createdatabase)
   connection.commit()


   #create extension postgis
   create_extension_query_postgis = """create extension if not exists postgis;"""
   cursor.execute(create_extension_query_postgis)
   connection.commit()

   #create tables and indexes in the databse called Landmark: using same SQL statements used in PGAdmin
   create_tables_landmarks = """  CREATE TABLE landmarks 
(
  gid character varying(5) NOT NULL,
  name character varying(50),
  address character varying(50),
  date_built character varying(10),
  architect character varying(50),
  landmark character varying(10),
  latitude double precision,
  longitude double precision,
  the_geom geometry,
  CONSTRAINT landmarks_pkey PRIMARY KEY (gid),
  CONSTRAINT enforce_dims_the_geom CHECK (st_ndims(the_geom) = 2),
  CONSTRAINT enforce_geotype_geom CHECK (geometrytype(the_geom) = 'POINT'::text OR the_geom IS NULL),
  CONSTRAINT enforce_srid_the_geom CHECK (st_srid(the_geom) = 4326)
);
""" 
   cursor.execute(create_tables_landmarks)
   connection.commit()
   create_index = """ CREATE INDEX landmarks_the_geom_gist ON landmarks USING gist (the_geom )"""
   cursor.execute(create_index)
   connection.commit()
   
   #Copy the CSV data into the Database
   insert_data = """copy landmarks(name,gid,address,date_built,architect,landmark,latitude,longitude) FROM 'C:\\Users\\Bishnu Poudyal\\Desktop\\Final_Project\\Individual_Landmarks.csv' DELIMITERS ',' CSV HEADER """
   cursor.execute(insert_data)
   connection.commit()

   #creating new message https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs.html
   response = queue.send_message(MessageBody='landmarks',MessageAttributes={
      'uploadmessage':{
         'StringValue':'Uploaded Successfully!!!',
         'DataType':'String'
         }})
   queue = sqs.get_queue_by_name(QueueName='landmarks')
   
   #Translate latitude and longitude into POINT geometry 
   table_update = """UPDATE landmarks SET the_geom = ST_GeomFromText('POINT(' || longitude || ' ' || latitude || ')',4326) """
   cursor.execute(table_update)
   connection.commit()

   #Writing PostGIS queries to display 10 location for this latitude and longitude https://www.postgresqltutorial.com/postgresql-python/query/
   
   selece_queries = """SELECT distinct ST_Distance(ST_GeomFromText('POINT(-87.6348345 41.8786207)', 4326), landmarks.the_geom) AS planar_degrees, 
   name, 
   architect
   FROM landmarks
   ORDER BY planar_degrees ASC 
   LIMIT 5 """
   count = 1
   cursor.execute(selece_queries)
   connection.commit()
   location_details=[]
   records = cursor.fetchall()
   print("5 closest landmarks to -87.6348345 41.8786207")
   print("*******************")
   for row in records:
       print("Location-" + str(count))
       print("----------")
       print("Planar_Degrees - " + str(row[0]))
       print("Name - " + str(row[1]))
       print("Architect - " + str(row[2]))
       print("Latitude - "+ str(row[3]))
       print("Longitude - "+ str(row[4]))
       print("*******************")
       count +=1
       location_details.append(str(row[0]))
       location_details.append(str(row[1]))
       location_details.append(str(row[2]))
       location_details.append(str(row[3]))
       location_details.append(str(row[4]))

   #sending location data to the queue    
   response = queue.send_message(MessageBody='landmarks',MessageAttributes={
      'Locations':{
         'StringValue':",".join(location_details),
         'DataType':'String'
         }})
   connection.commit()
   
   
except (Exception, psycopg2.Error) as error:
    if(connection):
        print(error)

finally:
    #closing database connectionection.
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connectionection is closed")
