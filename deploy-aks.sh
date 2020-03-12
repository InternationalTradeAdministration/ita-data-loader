#!/usr/bin/env bash

./build-push-docker-image.sh
kubectl delete pod,service ita-dataloader
kubectl apply -f kube-config.yml
