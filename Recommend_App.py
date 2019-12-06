# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 08:41:14 2019

@author: Shreya
"""

from pyspark.sql import SparkSession
#from pyspark.sql.functions import isnan, when, count, col

from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, DoubleType

from recommendation import RecommendationEngine
from utilityFile import Utility
import random
from pyspark.sql.functions import rand


def main():
    
    #global reco_engine
    global utility 
    
    utility = Utility()
    spark = SparkSession.builder.master('local').appName('recommender').getOrCreate()
    ##df_sampled = spark.read.load("sampled_reviews_part.json",format="json",inferSchema="true",header="true")
    df_sampled = spark.read.json("C:\MBS-Rutgers programs\Big Data Algo\Project\data\sampled_reviews_part.json")
    df_books = spark.read.json("C:\MBS-Rutgers programs\Big Data Algo\Project\data\metaBooks.json")
    
    df_sampled = df_sampled.drop('reviewerID','revIdIndex','prodIndex')
    df_sampled = utility.to_StringIndex('reviewer',df_sampled)
    df_sampled = utility.to_StringIndex('asin',df_sampled)
    
    print("Splitting data for evaluation...")
    (training_data, test_data) = df_sampled.randomSplit([0.9, 0.1])
    
    print("training sample count: ",training_data.count())
    
    print("Creating a recommendation engine instance...")
    
    reco_engine = RecommendationEngine(spark,df_sampled,df_books)
    #reco_engine.evaluate_model(test_data)
    
    print("Adding new user...")
    df_newuser = add_new_user(spark,df_books,10)
    
    ## Show this df_newuser to the reco screen
    
    ## Receive ratings from the screen for each books 
    
     
    new_user_predictions = reco_engine.predict_ratings(df_newuser)
    
    new_user_predictions.sort("prediction",ascending=False).show(10)
    
    
    
      
"""Manually creating a new user and selecting some books for prediction """   
def add_new_user(spark,df_books,no_of_books):
    new_user_ratings=[]
    new_user_id = ''.join(random.choice('0123456789ABCDEF') for i in range(14))
    list_books = df_books.select('asin').orderBy(rand()).limit(no_of_books).collect()
    for book in list_books:
        record = (new_user_id,book[0])  #,float(random.randint(0,5))"""
        new_user_ratings.append(record) 
        del record
    #rdd = spark.sparkContext.parallelize(new_user_ratings)
    schema = StructType([
             StructField("reviewer", StringType(), True),
             StructField("asin", StringType(), True),
             #StructField("overall", DoubleType(), True)
            ])
    df_newuser = spark.createDataFrame(new_user_ratings, schema)
    
    df_newuser = utility.to_StringIndex('reviewer',df_newuser)
    df_newuser = utility.to_StringIndex('asin',df_newuser)
    
    return df_newuser


"""Given a user_id and a list of book_ids, predict ratings for them. This method will receive inputs from the html page """
"""def get_ratings_for_book_ids(spark,user_id, book_ids):

    schema = StructType([
     StructField("asin", StringType(), True)
     StructField("overall", DoubleType(), True)
    ])
    
    user_id = ''.join(random.choice('0123456789ABCDEF') for i in range(14))

    df_newuser = spark.createDataFrame(book_ids, schema)
    df_newuser = df_newuser.withColumn("reviewer",user_id)
    df_newuser = utility.to_StringIndex('reviewer',df_newuser)
    df_newuser = utility.to_StringIndex('asin',df_newuser)
    #ratings = self.__predict_ratings(df_newuser,10)

    return df_newuser"""
    
  
if __name__=='__main__':
    main()
