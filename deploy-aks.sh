#!/usr/bin/env bash

./build-push-docker-image.sh
kubectl delete deployment,service,ingress ita-dataloader -n mdsnamespace
kubectl apply -f kube-config.yml -n mdsnamespace