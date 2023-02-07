#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pyspark


# In[24]:


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType,LongType
from pyspark.sql.window import Window
from pyspark.sql import *
from pyspark.sql.functions import *


# In[4]:


spark = SparkSession.builder.appName('SparkTask8').config("spark.driver.memory", "40g").getOrCreate()


# In[5]:


spark 


# In[37]:


# spark = SparkSession \
#     .builder \
#     .master('local[*]') \
#     .appName('SparkTask8') \
#     .config('spark.sql.debug.maxToStringFields', 2000) \
#     .config('spark.debug.maxToStringFields', 2000) \
#     .getOrCreate()


# ## User Data: 

# In[5]:


#df1.printSchema()


# In[38]:


data_user.printSchema()


# In[7]:


Schema1=StructType([
    StructField('average_stars',DoubleType()),
    StructField('compliment_cool',IntegerType()),
    StructField('compliment_cute',IntegerType()),
    StructField('compliment_funny',IntegerType()),
    StructField('compliment_hot',IntegerType()),
    StructField('compliment_list',IntegerType()),
    StructField('compliment_more',IntegerType()),
    StructField('compliment_note',IntegerType()),
    StructField('compliment_photos',IntegerType()),
    StructField('compliment_plain',IntegerType()),
    StructField('compliment_profile',IntegerType()),
    StructField('compliment_writer',IntegerType()),
    StructField('cool',IntegerType()),
    StructField('elite',StringType()),
    StructField('fans',IntegerType()),
    StructField('friends',StringType()),
    StructField('funny',IntegerType()),
    StructField('name',StringType()),
    StructField('review_count',IntegerType()),
    StructField('useful',IntegerType()),
    StructField('user_id',StringType()),
    StructField('yelping_since',StringType()),
])


# In[8]:


df1 = spark.read.schema(Schema1).json('yelp_academic_dataset_user.json')


# In[9]:


df1.write.option("header","true").csv('data_user.csv')


# In[10]:


data_user = spark.read.csv('data_user.csv',header=True, inferSchema=True)


# In[12]:


data_user.show()


# ## Review Data:

# In[ ]:


#df2=spark.read.json('yelp_academic_dataset_review.json')


# In[16]:


df2.printSchema()


# In[13]:


Schema2=StructType([
    StructField('business_id',StringType()),
    StructField('cool',LongType()),
    StructField('date',StringType()),
    StructField('funny',LongType()),
    StructField('review_id',StringType()),
    StructField('stars',DoubleType()),
    StructField('text',StringType()),
    StructField('useful',LongType()),
    StructField('user_id',StringType()),
])


# In[14]:


df2=spark.read.schema(Schema2).json('yelp_academic_dataset_review.json')


# In[15]:


display(df2)


# In[17]:


df2.write.option('header','true').csv('data_review.csv')


# In[19]:


data_review = spark.read.csv('data_review.csv',header=True,inferSchema=True)


# In[20]:


data_review.show()


# ## Business data:

# In[21]:


Schema3 =StructType([
StructField('attributes',
                StructType([
                StructField('ByAppointmentOnly',StringType()),
                StructField('AcceptsInsurance',StringType()),
                StructField('AgesAllowed',StringType()),
                StructField('Alcohol',StringType()),
                StructField('Ambience',StringType()),
                StructField('BYOB',StringType()),
                StructField('BYOBCorkage',StringType()),
                StructField('BestNights',StringType()),
                StructField('BikeParking',StringType()),
                StructField('BusinessAcceptsBitcoin',StringType()),
                StructField('BusinessAcceptsCreditCards',StringType()),
                StructField('BusinessParking',
                            StructType([
                                StructField('garage',StringType()),
                                StructField('street',StringType()),
                                StructField('vaildated',StringType()),
                                StructField('lot',StringType()),
                                StructField('valet',StringType()),
                            ])),
                StructField('Caters',StringType()),
                StructField('CoatCheck',StringType()),
                StructField('Corkage',StringType()),
                StructField('DietaryRestrictions',StringType()),
                StructField('DogsAllowed',StringType()),
                StructField('DriveThru',StringType()),
                StructField('GoodForDancing',StringType()),
                StructField('GoodForKids',StringType()),
                StructField('GoodForMeal',StringType()),
                StructField('HairSpecializesIn',StringType()),
                StructField('HappyHour',StringType()),
                StructField('HasTV',StringType()),
                StructField('Music',StringType()),
                StructField('NoiseLevel',StringType()),
                StructField('Open24Hours',StringType()),
                StructField('OutdoorSeating',StringType()),
                StructField('RestaurantsAttire',StringType()),
                StructField('RestaurantsCounterService',StringType()),
                StructField('RestaurantsDelivery',StringType()),
                StructField('RestaurantsGoodForGroups',StringType()),
                StructField('RestaurantsPriceRange2',StringType()),
                StructField('RestaurantsReservations',StringType()),
                StructField('RestaurantsTableService',StringType()),
                StructField('RestaurantsTakeOut',StringType()),
                StructField('Smoking',StringType()),
                StructField('WheelchairAccessible',StringType()),
                StructField('WiFi',StringType()),
            ])),
                StructField('business_id',StringType()),
                StructField('name',StringType()),
                StructField('address',StringType()),
                StructField('city',StringType()),
                StructField('state',StringType()),
                StructField('postal_code',StringType()),
                StructField('latitude',DoubleType()),
                StructField('longitude',DoubleType()),
                StructField('stars',DoubleType()),
                StructField('review_count',IntegerType()),
                StructField('is_open',IntegerType()),

                StructField('categories',StringType()),
                StructField('hours',StructType([
                    StructField('Monday',StringType()),
                    StructField('Thusday',StringType()),
                    StructField('Wednesday',StringType()),
                    StructField('Thursday',StringType()),
                    StructField('Friday',StringType()),
                    StructField('Saturday',StringType()),
                ]))

])


# In[28]:


df3=spark.read.schema(Schema3).json('yelp_academic_dataset_business.json')


# In[29]:


df3=df3.withColumn("categories",explode(split("categories",",")))
df3=df3.withColumn("attributes",col("attributes").cast("string"))
df3=df3.withColumn("hours",col("hours").cast("string"))


# In[33]:


df3.printSchema()


# In[30]:


df3.write.option('header','true').csv('data_business.csv')


# In[32]:


df3.show()


# In[34]:


data_business = spark.read.csv('data_business.csv',header=True,inferSchema = True)


# In[35]:


data_business.show()


# In[42]:


#List the categories and its record count using spark.
df3.withColumn("categories",explode(split("categories",',')))
df3.groupBy("categories").agg(count("categories")).show()


# In[43]:


df3.createOrReplaceTempView("df3")


# In[44]:


#Select the Top 5 best rated (Consider the restaurants has minimum 10 reviews) Restaurants using spark.
spark.sql("select name,review_count,stars,categories from df3 where categories='Restaurants' and review_count>=10 order By stars desc,review_count desc limit 5").show()


# In[46]:


#Select worst 5 Home Services using spark.
spark.sql("select business_id,name,review_count,stars,categories from df3 where categories='Home Services' order by review_count desc limit 5").show()


# In[47]:


#Select the City which has max high rated Nightlife.
spark.sql("select city,max(stars) from df3 where categories LIKE 'Nightlife%' group by city order by 2 desc limit 1").show()


# In[49]:


df2.createOrReplaceTempView("df2")


# In[51]:


#Get the userid, name, category and review count of all the users and write into CSV.
df4=spark.sql("select df2.user_id, df3.name, df3.categories ,df3.review_count from df2 inner join df3 on df2.business_id=df3.business_id")
df4.show()
df4.write.option('header','true').csv('Final_data_task_8.csv')

