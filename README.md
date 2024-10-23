# Reddit Bitcoin Analysis

## Reddit Post-Comment Analyzer

This repository contains a PySpark script designed to process Reddit post and comment data. It is optimized to handle large datasets efficiently and forms a part of a larger project focused on analyzing vast amounts of Reddit data.

### Overview

The script links comments to posts that were created within 24 hours of the post creation time. It processes two datasets:
- **Reddit Posts**: CSV file containing posts made in a subreddit.
- **Reddit Comments**: CSV file containing comments made in a subreddit.

### Key Features
- Uses PySpark for efficient processing of large datasets.
- Joins Reddit posts and comments based on post `id` and timestamps.
- Filters comments made within 24 hours of each post.
- Outputs the result as a CSV file containing matched posts and comments.


