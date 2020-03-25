#!/usr/bin/env bash

cd docker
sudo az acr login --name dataservices
sudo docker login dataservices.azurecr.io -u dataservices -p $AZURE_CONTAINER_KEY
sudo docker build -t dataservices.azurecr.io/ita-dataloader .
sudo docker push dataservices.azurecr.io/ita-dataloader:latest
cd ..