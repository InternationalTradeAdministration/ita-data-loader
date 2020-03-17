# ITA Data Loader
A tool that helps import (stage) data for reporting purposes. This application will allow users to upload data 
files in a logical/systematic way so that a Data Factory pipeline can copy the data into an Azure SQL Database.
In some instances, the files are normalized during upload for sustainability. In other instances, APIs are made
available for data to be extracted. API documentation coming soon.

## Local Development
Steps to run this application on you local machine for development purposes.

**Prerequisites** 
 - Java 8
 - Gradle
 - NPM (`cd client` then `npm install`)

**Backend** 
 - `gradle bootRun `, http://localhost:8080

**Frontend** 
 - `cd client && npm start`, http://localhost:8081

## Running Tests
**Backend** `./gradlew test`

**Frontend**  `cd client && npm test`

## Build
 - CI/CD build script ```./build.sh```
 - Local build script ```./build-local.sh```

## Azure Configuration
**Prerequisites** 
- Azure CLI <https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest>
- Docker CLI <https://docs.docker.com/engine/reference/commandline/cli/>
- KUBECTL CLI (if deploying to AKS) <https://kubernetes.io/docs/tasks/tools/install-kubectl/>
- An existing Azure Subscription

**Steps** 
1. Log into Azure using the Azure CLI 
    - ```az login```
1. Create a Resource Group 
    - ```az group create --name <Resource Group Name> --location <Azure Region>```
1. Create a Blob Storage Account (Don't enable Hierarchical Namespace, i.e. we don't want Data Lake Gen 2) 
    - ```az storage account create --name <Storage Account Name> --resource-group <Resource Group> --location <Azure Region> --kind StorageV2``` 
1. An Azure AD App Registration is required to procure an OAuth Client ID and Client Secret
    - a sample manifest is located here: /manifests/sample-ita-data-loader-app-registration.json
1. Create a SQL Database: <https://docs.microsoft.com/en-us/azure/sql-database/scripts/sql-database-create-and-configure-database-cli?toc=%2fcli%2fazure%2ftoc.json>
1. Create a Data Factory using the Azure Portal (<https://portal.azure.com>) and deploy the configuration using this Git repository: <https://github.com/InternationalTradeAdministration/ita-datafactory-config>
1. Create an App Registration for the Data Factory with the Contributor Role
   - a sample manifest is located here: /manifests/sample-data-factory-app-registration.json
1. Deploy ITA Data Factory Log Extractor (see section below)
1. Create a Container Registry
    - az acr create --resource-group <Recource Group Name> --name <Container Name> --sku Basic
1. From here, you may customize and use the deploy scripts to deploy this application.

## Required Environment Variables
    - AZURE_OAUTH_CLIENT_ID: Active Directory Client ID
    - AZURE_OAUTH_CLIENT_SECRET: Active Directory Client Secret
    - AZURE_STORAGE_ACCOUNT: Blob Storage Account
    - AZURE_STORAGE_ACCOUNT_KEY: Blob Storage Account Key
    - DATAFACTORY_STATUS_URL: The URL to this ITA Data Factory Log Extractor - App Function
    - FLYWAY_URL: The jdbc connection to a AZURE SQL Database
    - FLYWAY_USER: Username to the AZURE SQL Database
    - FLYWAY_PASSWORD: Password to the AZURE SQL Database

## Deployment
 - AKS: ```./deploy-aks.sh```
 - For Azure DevOps pipeline configuration, update: ```azure-pipelines.yml```
 
## Data Factory Log Extractor
This App Function allows us to retrieve the last known status of a given pipeline within a given data factory:
    <https://github.com/InternationalTradeAdministration/ita-datafactory-log-extractor>
 - Data factory pipelines and storage containers must have the same name to retrieve the pipeline status
 - Deploying this project will result in a URL to perform HTTP requests, use that URL when populating the DATAFACTORY_STATUS_URL environment variable

## Database Notes
 - Flyway is used to manage the state of the database, that's all. This application does not otherwise interact with the database
 - SQL Scripts for updating the Database can be found here:  /src/main/resources/db/migration
 
 ## Additional Notes
  - The dataloader storage container needs to be manually updated once initially, to set users that have access to containers. See example below.
  - Only system administrators should have access to the Dataloader container
  - Storage containers only get created when that application is booted up (initialized)
  - New containers need to be manually created in Azure
  - Ex:
  ```json
{
  "businessUnits": [
    {
      "containerName": "demo",
      "businessName": "Demo Business Unit",
      "users": []
    },
    {
      "containerName": "dataloader",
      "businessName": "Dataloader",
      "users": [
        "admin@foo.com" 
      ]
    }
  ]
}
```
