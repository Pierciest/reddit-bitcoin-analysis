from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, regexp_extract
from pyspark.sql.types import TimestampType
import pyspark.sql.functions as F

def initialize_spark():
    """Initialize and return a Spark session."""
    return SparkSession.builder.appName("RedditPostCommentAnalysis").getOrCreate()

def process_comments_data(spark):
    """Process the Reddit comments data."""
    comments_df = spark.read.csv("./Data/Reddit/Raw/reddit-r-bitcoin-data-for-jun-2022-comments.csv", header=True, inferSchema=True)
    comments_df = comments_df.dropna(subset=["permalink", "body", "created_utc"])
    comments_df = comments_df.withColumn("post_id", regexp_extract(col("permalink"), '/comments/([a-z0-9]+)/', 1))
    return comments_df
    
def process_posts_data(spark):
    """Process the Reddit posts data."""
    posts_df = spark.read.csv("./Data/Reddit/Raw/reddit-r-bitcoin-data-for-jun-2022-posts.csv", header=True, inferSchema=True)
    posts_df = posts_df.dropna(subset=["created_utc"])
    return posts_df
    
def preprocess_data(posts_df, comments_df):
    """Preprocess the data."""
    posts_df = posts_df.withColumn('created_utc', from_unixtime(col('created_utc')).cast(TimestampType()))
    comments_df = comments_df.withColumn('comment_created_utc', from_unixtime(col('created_utc')).cast(TimestampType()))
    comments_df = comments_df.filter(col('permalink').isNotNull())
    posts_df = posts_df.filter(col('selftext').isNotNull())
    posts_df = posts_df.withColumn('end_time', col('created_utc') + F.expr('INTERVAL 24 HOURS'))
    return posts_df, comments_df

def join_data(posts_df, comments_df):
    """Join posts and comments data."""
    result_df = posts_df.alias('p') \
        .join(
            comments_df.alias('c'),
            (col('p.id') == col('c.post_id')) &
            (col('c.comment_created_utc') >= col('p.created_utc')) &
            (col('c.comment_created_utc') <= col('p.end_time')),
            how='left'
        ) \
        .select(
            col('p.id').alias('post_id'),
            col('p.title').alias('post_title'),
            col('p.created_utc').alias('post_created_utc'),
            F.coalesce(col('c.body'), F.lit("")).alias('comment_body'),
            col('c.comment_created_utc')
        )
    return result_df

def save_output(result_df, spark_output_path, pandas_output_path):
    """Save results both as Spark data and as a Pandas DataFrame."""
    # Save as Spark DataFrame to CSV
    result_df.coalesce(1).write.csv(spark_output_path, header=True, mode="overwrite")
    
    # Convert to Pandas DataFrame and save as CSV
    result_df.toPandas().to_csv(pandas_output_path, index=False)

def main():
    """Main function to orchestrate the data processing."""
    # Initialize Spark session
    spark = initialize_spark()

    # Load the data
    comments_df = process_comments_data(spark)
    posts_df = process_posts_data(spark)

    # Preprocess the data
    posts_df, comments_df = preprocess_data(posts_df, comments_df)

    # Join the data
    result_df = join_data(posts_df, comments_df)

    # Define output paths
    spark_output_path = './Data/Reddit/Processed/output_combined_spark.csv'
    pandas_output_path = './Data/Reddit/Processed/output_combined_pandas.csv'

    # Save the result in both formats
    save_output(result_df, spark_output_path, pandas_output_path)

    # Show result
    result_df.show(truncate=False)

# Run the main function
if __name__ == "__main__":
    main()
