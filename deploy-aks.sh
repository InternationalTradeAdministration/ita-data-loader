#!/usr/bin/env bash

kubectl delete deployment,service ita-dataloader -n mdsnamespace
kubectl delete ingress ita-dataloader -n mdsnamespace
sleep 10 #may need to wait longer...
kubectl apply -f kube-config.yml
