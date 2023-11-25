# Welcome to my Polygon Finance ElT data pipeline project!

## Project Overview
This project involves the creation of an Extract, Load, Transform (ELT) data pipeline that fetches data from the Polygon Finance website using its API, loads it into a PostgreSQL database, transfers it to a Google Cloud Storage (GCS) bucket, and finally moves it to BigQuery, the analytical database. The entire pipeline is orchestrated with Airflow using the Celery executor on Docker-compose. The tasks are grouped into three categories: stock, forex, and crypto, each with three different workflows - extraction to PostgreSQL, upload to GCS, and upload to BigQuery

## Impact and Usefulness
The pipeline provides a robust and automated way to gather, process, and analyze financial data from various markets, including stocks, forex, and cryptocurrencies. This data can be used by businesses and investors to make informed decisions based on real-time and historical market trends. The pipeline's automation reduces the time and effort required to gather and process this data, allowing businesses to focus more on analysis and decision-making.

## Data Dictionary
For a detailed understanding of the data fields in the tables containing daily open, high, low, and close (OHLC) data, refer to the provided [Data Dictionary Link](https://docs.google.com/document/d/10Vmmcs7miKZ3VB2K5HKOE2j8AaI1VO04tMVaPde0ViM/edit?usp=sharing). The dictionary encompasses information for stocks/equities, forex, and cryptocurrencies, with specific details for each market, such as adjustments for splits, request IDs, exchange symbols, and more.

## Data Quality Checks and Testing
A notable enhancement to this pipeline is the incorporation of robust data quality checks and testing within dbt. These checks ensure the reliability and accuracy of the processed data, contributing to the overall integrity of the analytics and insights derived from the pipeline.

## Data Transformation and Metrics
The dbt tool is used to perform transformations and generate metrics. For each of the three finance data (stock, crypto, forex), a metric was developed to get the average trading prices and the number of transactions from increasing historical data. These metrics are represented in the agg_stock_avg, agg_crypto_avg, and agg_forex_avg tables.

## Technical Details
The pipeline uses various technologies and tools to perform its tasks. The Polygon Finance API is used to extract the data, which is then loaded into a PostgreSQL database. The data is then transferred to a GCS bucket using the PostgresToGCSOperator in Airflow. Subsequently, the GCSToBigQuery Operator is employed to move the data from GCS to BigQuery. Finally, dbt is used within BigQuery for further transformations, metrics generation, and rigorous data quality checks. Orchestrated with Airflow from the data extraction down to the final stage involving dbt(A dbt job was utilized and it get triggered after the data finishes loading into BigQuery).

## Image Workflows
Below are images showing the Airflow logs, complete table lineage, data at BigQuery, and data at GCS. These images will provide a visual representation of the pipeline's operation and the data it processes.

- Airflow dag log
![image](https://github.com/krissemmy/Polygon-Finance-Data-ELT/assets/119800888/23cb53c1-c0f5-41b6-8e3c-170431c77f54)

- Table Lineage
![image](https://github.com/krissemmy/Polygon-Finance-Data-ELT/assets/119800888/7c909848-1015-4611-ae02-9be89ef5ed61)

- Data at GCS Bucket
![image](https://github.com/krissemmy/Polygon-Finance-Data-ELT/assets/119800888/447cd451-6125-4067-a113-b81d645b6f5b)

- Raw tables in BiQuery
- Crypto
![image](https://github.com/krissemmy/Polygon-Finance-Data-ELT/assets/119800888/8c8c5d4f-8b9b-44f1-8907-81b76366c54a)

- Forex
![image](https://github.com/krissemmy/Polygon-Finance-Data-ELT/assets/119800888/996885b7-fc23-4150-aeff-fa97480f259d)

- Stock
![image](https://github.com/krissemmy/Polygon-Finance-Data-ELT/assets/119800888/180b7c56-59ba-4b8c-a963-bcdf0ea00535)

- Stock data in Postgres
![image](https://github.com/krissemmy/Polygon-Finance-Data-ELT/assets/119800888/98825322-bb18-4600-94ec-6f70f03d7a9c)


## Installation and Setup
There is a separate README file that provides detailed instructions on how to install and set up Airflow with the necessary connections. This file can be accessed via the provided link. Check out the Airflow setup instructions [here](https://github.com/krissemmy/Polygon-Finance-Data-ELT/blob/main/Airflow_Codes/airflow_setup/airflow_setup.md)

## Future Improvement
Just like any Data Engineer, I'm looking to always find improvement and scale my data solutions to efficiently solve business problems.
some improvements I have in mind:
- Expansion of metrics based on specific business needs.
- Integration of additional financial markets for broader insights.
- Optimization of pipeline performance for scalability.

## Conclusion
This project stands as a robust and automated solution, offering a streamlined approach to gather, process, and analyze financial data from diverse markets. Beyond providing valuable insights, it serves as a testament to the power and flexibility of modern data pipeline technologies. Open to feedback and improvement, the project maintains an adaptive stance, with opportunities for further enhancements, scalability, and a strong commitment to data quality. Feel free to contribute through pull requests to shape the evolution of this dynamic data engineering solution.
