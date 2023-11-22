
# E-Commerce Sentiment Analysis for Customer Reviews

This project offers a comprehensive analysis of sentiment trends across various product categories on e-commerce platforms, primarily Amazon. It also entails the development of a sentiment analysis model adept at classifying customer reviews into positive, negative, and neutral sentiments.

## Table of Contents
1. [Project Overview](#project-overview)
2. [Data Dictionary](#data-dictionary)
3. [Project Workflow](#project-workflow)
4. [Setup and Execution](#setup-and-execution)
5. [Contributions](#contributions)
6. [License](#license)
7. [Collaborators and Contact](#collaborators-and-contact)

## Project Overview <a name="project-overview"></a>
The primary objectives of this project are:
- To discern sentiment trends across diverse product categories.
- To engineer a robust sentiment analysis model for review classification.

## Data Dictionary <a name="data-dictionary"></a>

The dataset provides detailed insights into products and associated customer reviews. Below is a breakdown of the dataset columns:

| Field Name                | Description |
|---------------------------|-------------|
| Product_ID                | Unique identifier for each product. |
| product                   | Product name and description. |
| price_dollars             | Listed price in USD. |
| star_ratings              | Product's average star rating. |
| total_ratings             | Total number of product reviews. |
| url                       | Direct link to the Amazon product page. |
| Customer_i_ID             | Unique identifier for customer reviews (with `i` indicating the review number, ranging from 1 to 5). |
| Customer_i_Star_Rating    | Star rating provided by the customer. |
| Customer_i_Comment        | Feedback or comment from the customer. |
| Customer_i_buying_influence | Indicator of the review's influence on purchasing decisions. |
| Customer_i_Date           | Date when the review was posted. |
| Customer_i_Unique_ID      | Composite identifier combining `Product_ID` and `Customer_i_ID`. |

## Project Workflow <a name="project-workflow"></a>
The project covers the following key tasks:
1. Import necessary Python libraries.
2. Load data from Amazon.com.
3. Assess the data's quality and structure.
4. Clean and preprocess the data.
5. Conduct exploratory data analysis (EDA).
6. Implement Natural Language Processing (NLP) techniques for sentiment analysis.
7. Build and evaluate the sentiment classification model.

## Setup and Execution <a name="setup-and-execution"></a>
Ensure all required Python libraries, as specified in the project notebooks, are installed. Begin by executing the primary notebook (`Internship_Project.ipynb`). For additional data formatting, refer to the `create_new_csv.ipynb` notebook.

The dataset originates from Amazon.com, and related scripts (`scraper_script.py` and `data_processing_script.py`) are included for extended functionalities.

## Contributions <a name="contributions"></a>
We welcome and appreciate contributions. For significant modifications, kindly initiate an issue for discussion before making a pull request.

## License <a name="license"></a>
This project is open-source under the MIT license. We encourage the use, modification, and distribution of the code as deemed fit.

## Collaborators and Contact <a name="collaborators-and-contact"></a>
We invite developers to fork this repository and submit pull requests for enhancements or fixes. All contributions enrich the project. For inquiries or issues, please raise an issue or reach out to the repository maintainer.

## AWS S3 Operations <a name="aws-s3-operations"></a>
This project leverages Amazon's S3 service for data storage and retrieval. The provided notebook `aws_s3.ipynb` elucidates the following S3 operations using the `boto3` library:

- **List S3 Buckets**: Enumerate all available S3 buckets.
- **List Objects in a Bucket**: Detail all objects within a specified bucket.
- **Upload JSON to S3**: Automate the upload of `.json` files from a local directory to a designated S3 bucket and prefix.
- **Alternative JSON Upload**: An alternate technique to upload `.json` files to S3.
- **Download CSV from S3**: Facilitate the download of `.csv` files from a specific S3 path to a local directory.
- **Determine AWS Region**: Ascertain the currently configured AWS region.

For a comprehensive understanding and execution of these operations, refer to the `aws_s3.ipynb` notebook.
