# -*- coding: utf-8 -*-
"""
Created on Wed Dec  4 18:50:28 2019

@author: Shreya
"""
from pyspark.sql import SparkSession
#from pyspark.sql.functions import isnan, when, count, col
#from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
#from pyspark.sql.functions import rand
#from pyspark.ml.feature import StringIndexer

#from pyspark.sql.types import StructType
#from pyspark.sql.types import StructField
#rom pyspark.sql.types import StringType, IntegerType, DoubleType


#import random
from time import time
import logging

from utilityFile import Utility

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



"""A Book recommendation engine"""
class RecommendationEngine:
    
    
    """Init the recommendation engine given a Spark session and a dataset path"""
    def __init__(self,spark,df_sampled,df_books):
        
        logger.info("Starting up the Recommendation Engine: ")
        
        self.utility = Utility()
        self.spark = spark
        # Load ratings data for later use
        self.df_sampled = df_sampled
        # Load books data for later use
        self.data_books = df_books
        #self.spark.read.json("C:\MBS-Rutgers programs\Big Data Algo\Project\data\metaBooks.json")
        self.train_model("reviewer_Idx","asin_Idx","overall") 
                
        
 
    """Train the ALS model with the current dataset"""
    def train_model(self,userCol,itemCol,ratingCol):
        
        
        logger.info("Training the ALS model...")

        #for rank in ranks:
        als = ALS(userCol=userCol, itemCol=itemCol, ratingCol=ratingCol,rank=self.rank,
                         maxIter=self.iterations, regParam=.05, nonnegative=True,coldStartStrategy="drop", implicitPrefs=False)
        
        """Pre-processing the alphanumeric columns to numeric value"""
        
        
        t0 = time()
        self.model = als.fit(self.df_sampled)
        tt = time() - t0
        
        logger.info("New model trained in %s seconds",round(tt,3))
        
        
    def evaluate_model(self,test_data_df):
        
        logger.info("Evaluating Model...")
        
        #data_to_predict = test_data_df.drop("overall")
        
        t0 = time()
        test_data_df = self.model.transform(test_data_df)
        tt = time() - t0
        
        logger.info("Time taken to predict in %s seconds",round(tt,3))
        
        # = data_predictions.withColumn("overall",test_data_df['overall'])
        
        evaluator = RegressionEvaluator(metricName="rmse", labelCol="overall",predictionCol="prediction")
        
        rmse = evaluator.evaluate(test_data_df)
        
        logger.info("RMSE:",rmse)
        
        
        
        
    """Add additional movie rating in the format (reviewer, asin, rating)"""
    """def _add_ratings(self, df_newuser):
        
        # Combine the new_ratings with the original review dataset
        df_combined = self.df_sampled.unionByName(df_newuser)
        self.__train_model('reviewer_Idx','asin_Idx','overall')
        return df_combined"""
        
        
    """Gets predictions for a new (reviewer, asin) """
    
    def predict_ratings(self, new_user_to_predict):
    
        t0 = time()
        new_user_predictions = self.model.transform(new_user_to_predict)
        tt = time() - t0
        print ("New model trained in %s seconds",round(tt,3))
        #new_user_predictions = new_user_predictions.join(new_user_to_predict,on='asin',how='inner')
        new_user_predictions = new_user_predictions.join(self.data_books,on='asin',how='inner')
        new_user_predictions = new_user_predictions.select(new_user_predictions.reviewer,new_user_predictions.asin,self.data_books.title,new_user_predictions.prediction)
               
        return new_user_predictions
    
    
    """Recommends up to books_count top unrated books to user_id """
    def get_ratings_unrated_books(self, user_id):
    
        # Get pairs of (userID, movieID) for user_id unrated books
        user_unrated_books = self.df_sampled.filter(~ (self.df_sampled.reviewer == user_id)).select('asin')
        user_unrated_books = user_unrated_books.withColumn('reviewer',user_id)
        user_unrated_books = self.utility.to_StringIndex('reviewer',user_unrated_books)
        user_unrated_books = self.utility.to_StringIndex('asin',user_unrated_books)
        
        # Get predicted ratings
        ratings = self.predict_ratings(user_unrated_books)

        return ratings
    

    
    
     # Train the model
    rank = 20
    iterations = 10

    
    
    
    

