# Web-scale Data Management Project Template

Basic project structure with Python's Flask and Redis. 
**You are free to use any web framework in any language and any database you like for this project.**

### Project structure

* `k8s`
    Folder containing the kubernetes deployments, apps and services for the ingress, order, payment and stock services.
    
* `order`
    Folder containing the order application logic and dockerfile. 
    
* `payment`
    Folder containing the payment application logic and dockerfile. 

* `stock`
    Folder containing the stock application logic and dockerfile. 

* `test`
    Folder containing some basic correctness tests for the entire system. (Feel free to enhance them)

* `mongo`
    Folder containing kubernetes files for mongo.

* `ses`
    Folder containing ses application to manage locks.

### Deployment types:

#### docker-compose (local development)

After coding the REST endpoint logic run `docker-compose up --build` in the base folder to test if your logic is correct
(you can use the provided tests in the `\test` folder and change them as you wish). 

***Requirements:*** You need to have docker and docker-compose installed on your machine.

#### minikube (local k8s cluster)

~~This setup is for local k8s testing to see if your k8s config works before deploying to the cloud. First deploy your database using helm by running the `deploy-charts-minicube.sh` file (in this example the DB is Redis but you can find any database you want in https://artifacthub.io/ and adapt the script). Then adapt the k8s configuration files in the `\k8s` folder to match your system and then run `kubectl apply -f .` in the k8s folder.~~

Start minikube cluster.

`minikube start -p wsdm`

Enable ingress.

`minikube addons enable ingress`

Auth ghcr (set pat to expire in 30 days). Kube 3pa [docs](https://kubernetes.io/docs/tasks/configure-pod-container/pull-image-private-registry) if needed.

`kubectl create secret docker-registry regcred-ghcr --docker-server=https://ghcr.io --docker-username=<username> --docker-password=<pat> --docker-email=<email>` 

Deploy to cluster.

`./deploy-minikube.sh`

Cleanup cluster (pv will be released).

`./delete-minikube.sh` 

***Requirements:*** You need to have minikube (with ingress enabled) ~~and helm~~ installed on your machine.

#### kubernetes cluster (managed k8s cluster in the cloud)

Similarly to the `minikube` deployment but run the `deploy-charts-cluster.sh` in the helm step to also install an ingress to the cluster. 

***Requirements:*** You need to have access to kubectl of a k8s cluster.

## Microservices (local docker) - not advised (use minikube)
Before running the microservices you need to make sure the local or remote database is active. For MongoDB on Ubuntu this can be done using the following steps:
1. Run `sudo systemctl status mongod` to check the status of MongoDB
2. If disabled run: `sudo systemctl start mongod`
