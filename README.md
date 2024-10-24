# Reddit Bitcoin Analysis

This repository focuses on analyzing the correlation between the volatility in decentralized economies and social media platforms, specifically using **Bitcoin** and **Reddit** as sample data. The primary goal is to employ **Deep Learning** techniques to identify correlations between sub-trends on Reddit and Bitcoin's market volatility.

Currently, the progress is in the **data gathering** phase. Stay tuned for updates.

---

## Data

### Data Gathering

#### Google Trends Fetcher

This module contains a script designed to process Google Trends data and analyze the correlation between Reddit post volumes and Google search trends.

##### Overview

Google Trends does not offer an official API for direct data retrieval. Instead, search volumes are relative measures of interest over time. This script converts those relative search volumes into global estimates. Since Google Trends does not provide daily volume data for extended time periods, two datasets are used:

- **Monthly Volume**: Dataset with search volumes by **month**.
- **Daily Volume**: Dataset with search volumes by **day** for each month.

##### Key Features

- Retrieves **Monthly Volume** data from Google Trends.
- Retrieves **Daily Volume** data from Google Trends.
- Maps relative monthly search volumes to individual days within each month, scaling the volumes appropriately.
- Outputs the daily search volumes as a CSV file.

#### Reddit Post-Comment Analyzer

This module contains a **PySpark** script optimized for processing Reddit posts and comments at scale. It is an essential part of the overall project, designed to handle large volumes of Reddit data efficiently.

##### Overview

The script links **comments** to their respective **posts**, ensuring comments are made within 24 hours of the original post. It processes two datasets:

- **Reddit Posts**: A CSV file containing posts made in a specific subreddit.
- **Reddit Comments**: A CSV file containing comments made in the same subreddit.

##### Key Features

- Leverages **PySpark** to efficiently process large datasets.
- Joins Reddit **posts** and **comments** based on post `id` and comment timestamps.
- Filters comments that were made within 24 hours of a post's creation.
- Outputs a CSV file containing matched posts and their associated comments.

---

### Data Pre-processing

_Coming soon..._

---

### Data Clustering and Tagging

_Coming soon..._

---

## Deep/Machine Learning

_Coming soon..._

---

## Analysis and Results

_Coming soon..._
