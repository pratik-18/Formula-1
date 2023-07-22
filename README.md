# Formula-1 Data Analysis

## Introduction 🌟

Welcome to the project readme! 📚 This document provides an extensive overview of the project, covering its architecture, technical aspects, and instructions for usage. 🏗️ The primary objective of this project is to utilize Azure Databricks for data processing, analysis, and the creation of data pipelines. 💻 Specifically, it involves extracting data from the Ergast website, importing it into the Data Lake, performing data transformations using Databricks Notebooks, and generating interactive dashboards for data analysis. 🔄 Throughout the project, we showcase various data integration techniques, design patterns, and tools, including Azure Data Factory Pipeline and Power BI. 📊

## Architecture Overview 📐

The project employs a multi-layered architecture to manage, process, and analyze the data effectively. The architecture consists of the following layers:

![System Architecture](https://github.com/pratik-18/Formula-1/blob/master/Images/SysArchi.png)

1. **Raw Layer (Bronze):** This layer acts as the initial storage for the raw data sourced from the Ergast website. For the purpose of learning, the data is manually imported into the Data Lake's raw container. In a production environment, an automated solution such as Azure Data Factory Pipeline would typically be employed to fetch data directly from the Ergast API at regular intervals.
2. **Processed Layer (Silver):** The raw data goes through processing using Databricks Notebooks within the Lakehouse architecture. In this layer, data transformations and schema applications take place. The processed data is then stored in processed (silver) delta tables. These tables are Managed tables, and the specified location during the creation of the Database allows observation of how data is stored.
3. **Presentation Layer (Gold):** This layer contains highly aggregated data with essential business-level insights. The data from the processed tables is read and transformed to obtain aggregated data. Similar to the previous layer, the resulting data is stored in Presentation (gold) delta tables, which are Managed tables with location specified during database creation.

Data from any of these sets of Delta tables can be utilized for Data Science and Machine Learning workloads. Delta Lake provides connectors to BI tools such as Power BI and Tableau. Roles and access controls can be defined for data governance Delta Lake also offers advanced features, including GDPR compliance, time travel, and improved data management capabilities Additionally eliminates the need for data copying to a Data Warehouse.

### Incremental Load 🔄

The project implements an efficient incremental load methodology, facilitating the ingestion of exclusively new or updated data into the processed layer. By performing an analysis of the existing dataset alongside incoming data, the project accurately detects and selectively imports only the pertinent modifications or additions. This advanced approach significantly reduces processing time while optimizing the overall efficiency of the data ingestion process.

### Production Quality and Scheduling 📅

To ensure production quality, the project addresses the scheduling and monitoring aspects of data pipelines. Azure Data Factory (ADF) is utilized as the scheduling solution. ADF automates the execution of pipelines at regular intervals without manual intervention. It also provides monitoring capabilities and issue notifications. Furthermore, the project explores the use of Databricks jobs for scheduling purposes, considering their limitations and alternatives provided by ADF.

### Data Analysis and Dashboards 📊

Data analysis is conducted using Databricks Notebooks, which leverage data from the presentation layer stored in the Delta Lake format. The utilization of Delta tables allows the use of SQL for analysis purposes. Additionally, the project enables the integration of Power BI with Databricks, enabling the creation of interactive and visually appealing BI reports.

## Technical Aspects 🏗️

The project incorporates several technical aspects and tools, including:

- **Azure Databricks:** The primary focus of the project, used for data processing, analysis, and dashboard creation.
- **Azure Data Factory Pipeline:** Employed in a production environment for automated data extraction from the Ergast API at regular intervals.
- **Azure Data Lake Gen 2:** The storage infrastructure for all data layers.
- **Power BI:** Integrated with Delta Lake to create visually appealing BI reports based on transformed and aggregated data.
- **Delta Lake:** Presentation & Presentation layer data is stored in the Delta Lake format, providing advanced features for data management, compliance, and time travel.
- **Partitioning:** Implemented in the processed & presentation layer to enhance data retrieval and query performance.

## Set-up 🔧

To utilize this project, follow the steps outlined below:

1. Set up ☁️ Azure environment with Azure Databricks, Azure Data Factory, and Azure Data Lake Gen2. 📁
2. After creating the required resources, create three containers in the Data Lake: raw, processed, and presentation. Load the three folders from the raw folder of this repository into the raw container of the Data Lake. 📁
3. Clone the project repository and import the necessary notebooks into Azure Databricks. 📥
4. Mount the Data Lake Containers to Databricks. Cluster Scoped Authentication using Access Keys is recommended for simplicity, although there are alternative methods available. 🔑
5. Run the notebooks in the following order:
    - **Data Ingestion:** The notebooks in the ingestion folder import data from the Raw Layer into the Processed Layer. These notebooks can be executed separately or all at once using the [0_Ingest_all_files.py](https://github.com/pratik-18/Formula-1/blob/master/Notebooks/ingestion/0_Ingest_all_files.py) notebook. Ensure to provide the correct parameters, including the source and `file_date`. 📂
    - **Data Processing:** The notebooks in the trans folder perform data transformations on the data loaded into the Presentation Layer. When running these notebooks, make sure to provide the correct parameter, which is the `file_date`. 🚀
    - **Data Analysis:** The notebooks in the analysis folder demonstrate the analysis of data stored in the Presentation Layer using SQL thanks to Lakehouse Architecture. 📈
6. Schedule the required notebooks using Azure Data Factory Pipeline or Databricks jobs to automate the data ingestion and processing tasks. I Personally used, Azure Data Factory to schedule the notebooks but Databricks flow can also be used for the same thing. Here is a brief explanation of how I did it.
    1. First Pipeline for Scheduling all ingestion notebooks, but it first checks whether the data exists or not. 📅
        
        ![Pipeline 1](https://github.com/pratik-18/Formula-1/blob/master/Images/P_1.png)
        
    2. Similarly, the second pipeline schedules all processing notebooks, this also checks for the existence of data. 🔄
        
        ![Pipeline 2](https://github.com/pratik-18/Formula-1/blob/master/Images/P_2.png)
        
    3. The last pipeline is the master pipeline and this runs the above-mentioned two pipelines. 🎯
        
        ![Pipeline 3](https://github.com/pratik-18/Formula-1/blob/master/Images/P_3.png)
        
    
    `Pipelines a & b` take `file_date` as a parameter & `Pipeline a` also has a variable for specifying data source. The master pipeline also has a parameter `file_date` and this parameter is passed down to `pipelines a & b`. Finally, the master pipeline can be scheduled using the Tumbling window Trigger & the value of `window_end_date` from this trigger is used as a value of `file_date`. 📆
    
7. Connect Power BI to Databricks to access the data in the presentation layer and create BI reports. 📊💡

## Conclusion 🌟

In conclusion, this readme provides an extensive overview of the project, including its architecture and technical aspects. 🏗️ It serves as a comprehensive guide for understanding the project's objectives, architectural components, and the tools employed. 📚 The project demonstrates the effective utilization of Azure Databricks, Azure Data Factory, Delta Lake, and Power BI in building data pipelines, processing data, and creating interactive dashboards. 💻🔧 The inclusion of an incremental load approach ensures efficient data ingestion, minimizing processing time. ⏱️🚀

With these powerful technologies and strategies, this project showcases the successful implementation of a robust data processing and analysis solution. 🎉📊

Thank you for exploring this project! 🙌
