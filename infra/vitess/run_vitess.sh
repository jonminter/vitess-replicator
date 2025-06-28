#!/bin/bash

kubectl apply -f operator.yaml
kubectl apply -f 101_initial_cluster.yaml
