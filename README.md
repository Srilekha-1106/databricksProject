# Real-Time Data Processing with Unity Catalog and CI/CD in Azure Databricks

This project demonstrates the implementation of real-time data processing and governance using Azure Databricks, Unity Catalog, and CI/CD pipelines. Key steps include setting up storage accounts, 
configuring Databricks, using medallion architecture and creating workflows to manage and process data efficiently.

This project offers continuous integration and deployment, ensuring that all notebooks and data are consistently deployed across environments. It provides efficient handling of data with incremental 
loading and real-time processing capabilities. The use of Unity Catalog ensures robust data management with secure access controls. Autoscaling and workflow automation allow for efficient resource management. 
Additionally, we have set up CI/CD pipelines for the entire data workflow across catalogs using Azure DevOps, streamlining the process and enhancing operational efficiency.

The process began with the creation of an Azure Resource Group and two storage accounts. Containers were set up to store raw data, which was then organized into a structured format. An Azure Databricks workspace 
was established, and a Databricks access connector was configured for secure data access.
A central metastore and a development catalog were created within the Databricks workspace, along with the necessary storage credentials and external locations. A Databricks cluster was then set up to handle 
data processing tasks. The provided files were run to verify paths and variables, followed by enabling autoscaling and creating workflows for execution.
Triggers were set up to manage different data streams, ensuring incremental data processing when new files were added to Azure Data Lake Storage. The processed data was integrated with Power BI for reporting 
and analysis. A CI/CD pipeline was established using Azure DevOps to automate deployment, ensuring that all folders are updated in the live environment whenever there is a push to the main branch. Using CI/CD, 
all files were copied to the live folder in the UAT catalog, with admin access required for interaction. This setup facilitates seamless integration, deployment, and data accessibility across environments.

At the end of the project, the workspace appears as shown in the image. It includes the main branch with all the changes already pulled, all the files organized Notebooks folder. The setup ensures all project 
components are easily accessible and well-structured. A pipeline was created to facilitate the movement of data from the development catalog to the UAT catalog, requiring admin approval. The Azure DevOps 
interface illustrates the stages of deployment, ensuring a controlled and authorized transition of data between these environments.
##### Databricks Workspace
<img width="948" alt="databricks workspace" src="https://github.com/user-attachments/assets/74c69b9d-34ef-4841-ac84-b55585cefabd">

##### Reporting the files with PowerBI
<img width="948" alt="databricks workspace" src="https://github.com/user-attachments/assets/62223bb1-05cf-4a77-9ef4-3511118a5276">

##### CI/CD Pipeline
<img width="948" alt="databricks workspace" src="https://github.com/user-attachments/assets/62d37ca7-f29e-4d81-b3c0-428572e3f5a4">
