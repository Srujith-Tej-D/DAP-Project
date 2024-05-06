# DAP-Project
Semester - 1 DataBase and anylatical programming Project

Python-based data analytics project designed for effortless data management across MongoDB and PostgreSQL databases. 
Employ adaptable Python scripts to extract and manipulate data, facilitating seamless transitions between MongoDB and PostgreSQL databases. 
Utilize PostgreSQL data to create interactive visualizations in Python for comprehensive data analysis.
Dataset 1 link: https://catalog.data.gov/dataset/speed-camera-violations
Dataset 2 link: https://catalog.data.gov/dataset/traffic-crashes-crashes

For our geospatial analysis, we utilized a shapefile obtained from the official Chicago government site, which contains comprehensive geographic data for the area. This shapefile includes detailed latitude and longitude coordinates, which are essential for accurately determining and analyzing various locations within the city. By leveraging this rich dataset, we can effectively map and assess spatial patterns and trends across Chicago, enhancing our understanding of traffic-related incidents and safety measures.
Shape file link: https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Boundaries-Community-Areas-current-/cauq-8yn6

In our project, Dagster serves as the backbone for our ETL processes, facilitating a fully automated pipeline that streamlines every step from data acquisition to storage. Starting with the downloading of datasets directly from the Internet, Dagster seamlessly manages the ingestion of data. It further automates the extraction, transformation, and loading (ETL) phases, ensuring that the processed data is efficiently transferred into our PostgreSQL database. This robust automation allows the entire data workflow to be controlled by a single command: `dagit -f etl.py`, making the process both efficient and user-friendly. This setup not only saves time but also enhances the reliability and reproducibility of our data analysis pipeline.

File Info: -  
              1. datasets.py            ---    Downloading datasets from internet.
              2. extract.py             ---    Extracting data from downloaded files and ingesting it to MongoDB.
              3. transform_n_load.py    ---    Transforming and loading the extracted data to postgresql.
              4. etl.py                 ---    It executes all the OP's as a Pipeline.
              5. visuals.py             ---    Contains all the visualizations used to answer the research questions.
              6. geo_export*            ---    Files to helps for transforming the lattitude and longitude to text location.
              7. env_deps.yml           ---    Containes all the required libraries to be installed before running.
              
Note : - 
          Install all the libraries mention in env_deps.yml before executing the etl.py or visuals.py
          Modify the username and pasword along with db name in both MongoDB and Postgresql before executing

Running : -

             dagit -f etl.py       --->  To execute the Data pipeline.
             dagit -f visuals.py  ---->  To get the viusalizations in dagster ui ( should be executed after pipeline (etl.py) )

             Note : - Since etl.py is taking around 15 - 16 mins to execute we have created another job separately for visuals.
          
