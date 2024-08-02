# Real-Time Data Processing with Unity Catalog and CI/CD in Azure Databricks
## Azure Databricks Project Setup and Automation

## Project Overview

This project involves setting up an Azure Databricks environment, integrating it with Azure storage accounts, automating data processing workflows, and implementing CI/CD pipelines to ensure seamless integration and deployment of data and notebooks.

## Steps and Implementation

### 1. Azure Resource Group Creation
An Azure Resource Group was created to organize and manage all related resources.
![image](https://github.com/user-attachments/assets/c4aa04c2-0f9c-4d84-9b51-356a8dd4b21b)

### 2. Storage Accounts Setup
Two storage accounts were created to store and manage the project data.
![image](https://github.com/user-attachments/assets/4930f92e-95c3-4aca-b534-df1b9f67f88d)

### 3. Container Configuration
Within the `projectstgaccount`, three containers were created, with the `landing` container designated for storing raw data.
![image](https://github.com/user-attachments/assets/37708be9-d1da-44b0-a17a-ca259851e502)
![image](https://github.com/user-attachments/assets/d709b33d-14c3-48fc-b0e1-cfdfb628855c)

### 4. Medallion Folder Structure
Three folders were created in the medallion structure to organize data systematically.
![image](https://github.com/user-attachments/assets/38b65fba-8de2-4efe-ba82-13ba74f493de)

### 5. Azure Databricks Workspace Setup
An Azure Databricks workspace was established to facilitate data processing and analysis.
![image](https://github.com/user-attachments/assets/1d45a719-b5e8-458f-a3d2-1e4ff9b72432)

### 6. Databricks Access Connector
A Databricks access connector was created and added to the Blob Storage Contributor role for the two storage accounts, 
ensuring secure data access.

![image](https://github.com/user-attachments/assets/4b232bed-4c55-459b-896c-171296f88a3c)
![image](https://github.com/user-attachments/assets/e054b3a6-f901-4a14-8f02-0d29e8dad04b)
![image](https://github.com/user-attachments/assets/f6be1b33-306d-4de7-9dd5-f2f20fee71a4)

### 7. Databricks Metastore and Catalog
Within the Azure Databricks workspace, a metastore was created and attached to the workspace. Subsequently, 
a development catalog was set up.

![image](https://github.com/user-attachments/assets/eabb2366-9989-47eb-8d6a-21ad4e4ccbb0)
![image](https://github.com/user-attachments/assets/e6813e2c-111c-4609-bf9e-ed2187f7f301)
![image](https://github.com/user-attachments/assets/88da9e20-b775-43b7-8cd4-c51e1f02e651)

### 8. Storage Credentials and External Locations
Storage credentials and external locations were configured to manage data access and storage.
![image](https://github.com/user-attachments/assets/117d74b7-e990-4f99-bcfd-aae797c793c0)
![image](https://github.com/user-attachments/assets/a2f98942-880c-43d6-bfa1-21965144533c)

### 9. Cluster Creation
A Databricks cluster was created to execute data processing tasks.
![image](https://github.com/user-attachments/assets/58c8bb74-e486-4dec-b9f0-74ae412b064b)

### 10. File Verification
All provided files were manually run to verify that paths and variable names were correctly defined.
![image](https://github.com/user-attachments/assets/68fd3447-674c-4a45-8361-2ce5229b7a0e)

All schemas are created in the dev catalog
![image](https://github.com/user-attachments/assets/43f0dd1d-d27b-4d4a-89eb-5b6b7b52bafd)

### 11. Autoscaling and Workflow Creation
Autoscaling was enabled, and workflows were created to automate the execution of data processing tasks. 
![image](https://github.com/user-attachments/assets/769ddedf-5ce6-42b7-b5a7-91d7ac42ceb3)

### 12. Dbutils Widgets
Keys and parameters for `dbutils` widgets were created to handle dynamic configurations.
![image](https://github.com/user-attachments/assets/9b54d0b3-7fd0-4baf-8287-453394b89522)

### 13. Trigger Creation and Incremental Data Processing
Triggers were created to automate task execution. Multiple triggers were cloned to manage different data streams, such as raw roads and raw traffic.
New files added to Azure Data Lake Storage (ADLS) initiate the triggers, ensuring incremental data processing and successful job completion.
![image](https://github.com/user-attachments/assets/f92fbe37-918c-4251-a6a1-88ccfa1edb12)

### 14. Data Reporting
Processed data was integrated with Power BI for comprehensive reporting and analysis.
![image](https://github.com/user-attachments/assets/7358d851-127e-4f82-89b5-cd8bc2c63d8f)

### 15. CI/CD Pipeline Setup
A CI/CD pipeline was established to automate the deployment process. When there is a push to the main branch, all folders are copied to the live folder, requiring admin access for interaction. This setup ensures seamless integration and deployment of all notebooks to different environments, keeping the live folder updated with the latest data.

![image](https://github.com/user-attachments/assets/156a9023-8a7e-49e2-b47f-7b472f2870ec)

## Conclusion

This project demonstrates the efficient setup and automation of an Azure Databricks environment. It includes secure data integration, automated workflows, and comprehensive reporting, enhanced by a robust CI/CD pipeline to ensure consistent and up-to-date data deployment across different environments. This approach facilitates seamless integration, deployment, and data accessibility while maintaining data integrity and security.

At the end of the project, the workspace appears as shown in the image. It includes the main branch with all the changes already pulled, all the files organized Notebooks folder. The setup ensures all project 
components are easily accessible and well-structured. A pipeline was created to facilitate the movement of data from the development catalog to the UAT catalog, requiring admin approval. The Azure DevOps 
interface illustrates the stages of deployment, ensuring a controlled and authorized transition of data between these environments.
