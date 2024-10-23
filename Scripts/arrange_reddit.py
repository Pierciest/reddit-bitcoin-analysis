from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.types import TimestampType
import pyspark.sql.functions as F

def initialize_spark():
    """Initialize and return a Spark session."""
    return SparkSession.builder.appName("RedditPostCommentAnalysis").getOrCreate()

def load_data(spark, posts_path, comments_path):
    """
    Load the posts and comments data from CSV files.
    
    Args:
    - spark: Spark session
    - posts_path: Path to the posts CSV file
    - comments_path: Path to the comments CSV file
    
    Returns:
    - posts_df: DataFrame for posts
    - comments_df: DataFrame for comments
    """
    posts_df = spark.read.csv(posts_path, header=True, inferSchema=True)
    comments_df = spark.read.csv(comments_path, header=True, inferSchema=True)
    
    return posts_df, comments_df

def preprocess_data(posts_df, comments_df):
    """
    Preprocess the data: Convert Unix timestamp to datetime and handle column conflicts.
    
    Args:
    - posts_df: DataFrame for posts
    - comments_df: DataFrame for comments
    
    Returns:
    - posts_df: Preprocessed posts DataFrame
    - comments_df: Preprocessed comments DataFrame
    """
    # Convert 'created_utc' to Timestamp
    posts_df = posts_df.withColumn('created_utc', from_unixtime(col('created_utc')).cast(TimestampType()))
    comments_df = comments_df.withColumn('created_utc', from_unixtime(col('created_utc')).cast(TimestampType()))

    # Add 24-hour window to posts_df
    posts_df = posts_df.withColumn('end_time', col('created_utc') + F.expr('INTERVAL 24 HOURS'))

    # Rename conflicting columns in comments_df
    comments_df = comments_df.withColumnRenamed('created_utc', 'comment_created_utc')
    
    return posts_df, comments_df

def join_data(posts_df, comments_df):
    """
    Join posts and comments data on post id and within the 24-hour window.
    
    Args:
    - posts_df: Preprocessed posts DataFrame
    - comments_df: Preprocessed comments DataFrame
    
    Returns:
    - result_df: Joined DataFrame
    """
    result_df = posts_df.alias('p') \
        .join(comments_df.alias('c'), 
              (col('p.id') == col('c.id')) & 
              (col('c.comment_created_utc') >= col('p.created_utc')) & 
              (col('c.comment_created_utc') <= col('p.end_time')), 
              how='left') \
        .select('p.id', 'p.title', 'p.created_utc', 'c.body', 'c.comment_created_utc')
    
    return result_df

def save_output(result_df, output_path):
    """
    Save the result DataFrame to a CSV file.
    
    Args:
    - result_df: DataFrame to be saved
    - output_path: Path to save the CSV file
    """
    result_df.write.csv(output_path, header=True)

def main():
    """Main function to orchestrate the data processing."""
    # Initialize Spark session
    spark = initialize_spark()

    # File paths
    posts_path = './Data/Reddit/reddit-r-bitcoin-data-for-jun-2022-posts.csv'
    comments_path = './Data/Reddit/reddit-r-bitcoin-data-for-jun-2022-comments.csv'
    output_path = './Data/Reddit/output/posts_with_comments.csv'

    # Load the data
    posts_df, comments_df = load_data(spark, posts_path, comments_path)

    # Preprocess the data
    posts_df, comments_df = preprocess_data(posts_df, comments_df)

    # Join the data
    result_df = join_data(posts_df, comments_df)

    # Save the result
    save_output(result_df, output_path)

    # Show result
    result_df.show(truncate=False)

# Run the main function
if __name__ == "__main__":
    main()
