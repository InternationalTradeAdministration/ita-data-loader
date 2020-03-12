#!/usr/bin/env bash

#You'll need to log into Azure first 'sudo az acr login'
cd docker
sudo az acr login --name dataservices
sudo docker login dataservices.azurecr.io -u dataservices -p $AZURE_CONTAINER_KEY  #use container username and access key
sudo docker build -t dataservices.azurecr.io/ita-dataloader .
sudo docker push dataservices.azurecr.io/ita-dataloader
cd ..
