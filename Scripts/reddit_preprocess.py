from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split

def initialize_spark():
    """Initialize and return a Spark session."""
    return SparkSession.builder.appName("Reddit Data Processing").getOrCreate()

def process_comments_data(spark):
    """Process the Reddit comments data."""
    # Load Reddit comments data
    comments_df = spark.read.csv("./Data/Reddit/Raw/reddit-r-bitcoin-data-for-jun-2022-comments.csv", header=True, inferSchema=True)
    
    # Drop rows with null values in specific columns
    comments_df = comments_df.dropna(subset=["permalink", "body", "created_utc"])
    
    # Extract post_id from permalink
    comments_df = comments_df.withColumn("post_id", split(col("permalink"), "/").getItem(6))
    
    # Save the processed comments data to a new CSV file
    comments_df.write.csv("./Data/Reddit/Processed/processed_comments.csv", header=True, mode="overwrite")

def process_posts_data(spark):
    """Process the Reddit posts data."""
    # Load Reddit posts data
    posts_df = spark.read.csv("./Data/Reddit/Raw/reddit-r-bitcoin-data-for-jun-2022-posts.csv", header=True, inferSchema=True)
    
    # Drop rows with null values in the specified column
    posts_df = posts_df.dropna(subset=["created_utc"])
    
    # Save the processed posts data to a new CSV file
    posts_df.write.csv("./Data/Reddit/Processed/processed_posts.csv", header=True, mode="overwrite")

def main():
    """Main function to execute the Reddit data processing script."""
    # Initialize Spark session
    spark = initialize_spark()

    # Process both comments and posts data
    process_comments_data(spark)
    process_posts_data(spark)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
