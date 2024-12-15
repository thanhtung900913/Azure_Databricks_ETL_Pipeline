# Azure Medallion Architecture Data Pipeline

## **Overview**
This project implements a scalable and efficient Data Engineering pipeline using the Medallion architecture on the Azure platform. The pipeline ingests, processes, and transforms raw data into meaningful insights through distinct layers: Landing Zone, Bronze, Silver, and Gold.

## **Key Features**
- **Azure Databricks**: Utilized for notebook-based development and compute resources.
- **Azure Storage**: Used to store raw data, intermediate files, and processed datasets as Delta Lake files.
- **Delta Lake**: Ensures ACID transactions, versioning, and schema enforcement for data.
- **Spark Streaming**: Processes streaming data for near real-time analytics.
- **Unity Catalog**: Manages data governance and secures access to data.
- **GitHub Actions**: Enables Continuous Integration and Continuous Deployment (CI/CD).

## **Architecture**
The project is structured around the Medallion architecture with the following zones:

1. **Landing Zone**:
   - Raw data ingestion point.
   - Stores data in its original format without transformations.

2. **Bronze Zone**:
   - Contains ingested data with minimal transformations.
   - Used for initial exploration and debugging.

3. **Silver Zone**:
   - Stores cleaned and standardized datasets.
   - Implements deduplication and schema alignment.

4. **Gold Zone**:
   - Optimized for analytics and business intelligence.
   - Contains curated data ready for consumption by downstream applications.

## **Technologies Used**

### **Azure Services**
- **Azure Databricks**: Develop and run scalable data engineering workloads.
- **Azure Storage Account**: Store raw files, Delta tables, and processed datasets.
- **Unity Catalog**: Govern and control access to data at a granular level.

### **Data Processing**
- **Apache Spark**: Perform batch and streaming transformations.
- **Delta Lake**: Ensure robust data management.

### **CI/CD**
- **GitHub**: Code repository and version control.
- **GitHub Actions**: Automate pipeline deployments and testing.

## **Data Pipeline Workflow**
1. **Data Ingestion**:
   - Collect data from external sources into the Landing Zone.
   - Supported formats: CSV, JSON, Parquet, and others.

2. **Bronze Processing**:
   - Move raw data to Bronze tables with minimal changes.
   - Track metadata for provenance.

3. **Silver Processing**:
   - Apply data cleansing, transformation, and normalization.
   - Store standardized data in Silver tables.

4. **Gold Processing**:
   - Aggregate and enrich data for final consumption.
   - Store business-specific insights in Gold tables.

5. **Data Delivery**:
   - Serve Gold datasets to BI tools or machine learning models.

## **Repository Structure**
```
.
├── notebooks/            # Databricks notebooks
├── .github/
│   └── workflows/        # CI/CD workflows using GitHub Actions
├── data/                 # Sample datasets for testing
├── README.md             # Project documentation
└── LICENSE               # License file

```

## **CI/CD Pipeline**
The CI/CD process automates testing and deployment:
1. **Trigger**: Push or pull request to the main branch.
2. **Validation**: Lint code, validate notebook execution.
3. **Deployment**:
   - Sync notebooks with Databricks workspace.
   - Deploy Delta Lake tables and pipelines.

## **Getting Started**
### **Prerequisites**
- Azure subscription.
- Access to Azure Databricks workspace.
- GitHub repository linked to Azure Databricks workspace.
- Installed CLI tools:
  - Azure CLI
  - Databricks CLI

### **Setup Instructions**
1. Clone the repository:
   ```bash
   git clone https://github.com/thanhtung900913/Azure_Databricks_ETL_Pipeline.git
   ```
2. Configure Azure resources:
   - Create a Storage Account.
   - Set up Databricks workspace.
   - Define Unity Catalog policies.
3. Deploy notebooks and pipeline:
   ```bash
   databricks workspace import_dir notebooks /Workspace/Notebooks
   ```
4. Trigger GitHub Actions for deployment.

## **License**
This project is licensed under the [MIT License](LICENSE).

## **Contact**
For questions or collaboration opportunities, contact:
- **Name**: Tung Nguyen
- **Email**: thanhtung2962004@gmail.com
- **GitHub**: [https://github.com/your-profile](https://github.com/thanhtung900913)

