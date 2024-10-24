# Reddit Bitcoin Analysis

This repository focuses on analyzing the correlation between the volatility in decentralized economies and social media platforms, specifically using **Bitcoin** and **Reddit** as sample data. The primary goal is to employ **Deep Learning** techniques to identify correlations between sub-trends on Reddit and Bitcoin's market volatility.

Currently the progress is in the data gathering part, stay tuned for the updates.


## Data Gathering

### Google Trends Fetcher

This module contains a script for processing Google Trends data to evaluate the correlation between Reddit post volumes and Google search trends.

#### Overview

Google Trends does not provide an official API for retrieving search data, and the numbers represent relative searches rather than actual search volumes. This script converts relative volumes into global estimates. Since daily volume data cannot be fetched for long time spans, two datasets are required:

- **Monthly Volume**: A dataset containing the search volumes by **month**.
- **Daily Volume**: A dataset containing the search volumes by **day** for each month.

#### Key Features

- Retrieves **Monthly Volume** data.
- Retrieves **Daily Volume** data.
- Maps the relative monthly volume across each month to individual days and scales the volumes accordingly.
- Outputs a CSV file with the search volume for each day.

### Reddit Post-Comment Analyzer

This module contains a **PySpark** script optimized for processing Reddit post and comment data at scale. It is part of the larger project to analyze significant volumes of Reddit data efficiently.

#### Overview

The script links **comments** to their respective **posts**, ensuring that comments are made within 24 hours of the post's creation time. It processes two main datasets:

- **Reddit Posts**: CSV file containing posts from a subreddit.
- **Reddit Comments**: CSV file containing comments from the same subreddit.

#### Key Features

- Utilizes **PySpark** to process large datasets efficiently.
- Joins Reddit **posts** and **comments** based on post `id` and comment timestamps.
- Filters comments made within 24 hours of the post creation.
- Outputs a CSV file containing the matched posts and their associated comments.
