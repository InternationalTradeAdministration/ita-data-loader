#!/usr/bin/env bash

cd docker
sudo az acr login --name $AZURE_CONTAINER_USER
sudo docker login $AZURE_CONTAINER_USER.azurecr.io -u $AZURE_CONTAINER_USER -p $AZURE_CONTAINER_KEY
sudo docker build -t $AZURE_CONTAINER_USER.azurecr.io/ita-dataloader .
sudo docker push $AZURE_CONTAINER_USER.azurecr.io/ita-dataloader:latest
cd ..