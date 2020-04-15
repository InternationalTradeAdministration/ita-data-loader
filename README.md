# Data Loader

[![Build Status](https://dev.azure.com/InternationalTradeAdministration/Data%20Services/_apis/build/status/Dataloader%20-%20Dev2?branchName=master)](https://dev.azure.com/InternationalTradeAdministration/Data%20Services/_build/latest?definitionId=89&branchName=master)

A tool that helps import (stage) data for reporting purposes. This application will allow users to upload data
files in a logical/systematic way so that a Data Factory pipeline can copy the data into an Azure SQL Database.
In some instances, the files are normalized during upload for sustainability.

## Local Development

Steps to run this application on your local machine for development purposes.

### Prerequisites

- Java 8
- Gradle
- [NPM](https://www.npmjs.com/get-npm) (`cd client` then `npm install`)
- [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)
- [Docker CLI](https://docs.docker.com/engine/reference/commandline/cli)
- [KUBECTL CLI](https://kubernetes.io/docs/tasks/tools/install-kubectl) (if deploying to AKS)

#### Running Locally

- Backend: `gradle bootRun`, [](http://localhost:8080)
- Frontend: `cd client && npm start`, [](http://localhost:8081)

## Test

- Backend: `./gradlew test`
- Frontend:  `cd client && npm test`

## Build Scripts

-Application: `./build.sh`
-Container: `./build-push-docker-image.sh`

## Azure Configuration

1. Log into Azure
  
    ```shell script
    az login
    ```

1. Create a Resource Group, skip this step if you already have one you would like to use

    ```shell script
    az group create --name <resource-group> --location eastus
    ```

1. Create a Blob Storage Account (Don't enable Hierarchical Namespace, i.e. we don't want Data Lake Gen 2)

    ```shell script
    az storage account create -n <account-name> -g <resource-group> --location eastus --kind StorageV2 --access-tier Hot --sku Standard_ZRS
    az storage account blob-service-properties update --account-name <account-name> --enable-delete-retention true --delete-retention-days 365
    ```

1. An Azure AD App Registration is required to procure an OAuth Client ID and Client Secret; a sample manifest is located [here](/manifests/sample-ita-data-loader-app-registration.json)
1. Create a [SQL Server](https://docs.microsoft.com/en-us/cli/azure/sql/server?view=azure-cli-latest#az-sql-server-create), skip this step if you already have one you would like to use

    ```shell script
    az sql server create -l eastus -g  <resource-group> -n <server-name> -u <username> -p <password>
    ```

1. Create a [SQL Database](https://docs.microsoft.com/en-us/cli/azure/sql/db?view=azure-cli-latest#az-sql-db-create)

    ```schell script
    az sql db create -g <resource-group> -n <database-name> -s <server-name> -e GeneralPurpose -f Gen5 -z false  -c 4 --compute-model Serverless
    ```

1. Create a Data Factory using the [Azure Portal](https://portal.azure.com) and deploy the configuration using this
[repository](https://github.com/InternationalTradeAdministration/ita-datafactory-config)
1. Create an App Registration for the Data Factory with the Contributor Role, a sample manifest is located [here](/manifests/sample-data-factory-app-registration.json)
1. Create a Container Registry, skip this step if you already have one you would like to use

    ```shell script
    az acr create --resource-group <Recource Group Name> --name <Container Name> --sku Basic
    ```

1. From here, you may deploy this application with the appropriate environment variables, see below
1. For detail related to the deployment of containerized applications in Azure, reference this [repo](https://github.com/InternationalTradeAdministration/azure-samples) with deployment scenarios

### Required Environment Variables

- AZURE_OAUTH_CLIENT_ID: Active Directory Client ID
- AZURE_OAUTH_CLIENT_SECRET: Active Directory Client Secret
- AZURE_STORAGE_ACCOUNT: Blob Storage Account
- AZURE_STORAGE_ACCOUNT_KEY: Blob Storage Account Key
- FLYWAY_URL: The jdbc connection to a AZURE SQL Database
- FLYWAY_USER: Username to the AZURE SQL Database
- FLYWAY_PASSWORD: Password to the AZURE SQL Database
- DATAFACTORY_CLIENT_ID: Client ID for the data facotry App Registration
- DATAFACTORY_CLIENT_SECRET: Client Secret for the data facotry App Registration
- DATAFACTORY_NAME: The name of the data factory
- DATAFACTORY_RESOURCE_GROUP: The resource group for the data factory
- AZURE_TENANT_ID: The Azure tenant id the datafactory is in
- AZURE_SUBSCRIPTION_ID: The Azure subscription id the datafactory is in

## Database Notes

- Flyway is used to manage the state of the database, that's all. This application does not otherwise interact with the database
- SQL Scripts for updating the Database can be found here:  /src/main/resources/db/migration

## Additional Notes

Once the application is initialized, a system administrator needs to be set. 
 1. Go to the application URL (ex: https://dataloader-itadev2.vangos-cloudapp.us/#/)
 1. Select "Dataloader ADMIN"
 1. Add the trade.gov email address of a system administrator to the Dataloader ADMIN "Business Unit"
 1. Click Save
 
 ![Dataloader ADMIN Configuration](/screenshots/admin-panel.png)
 