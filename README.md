# MongoDB Atlas Integration for Honeycomb

[![OSS Lifecycle](https://img.shields.io/osslifecycle/honeycombio/atlas)](https://github.com/honeycombio/home/blob/main/honeycomb-oss-lifecycle-and-practices.md)

## Installation

```
go get github.com/honeycombio/atlas
```

## Usage

### Preparing MongoDB logging

Atlas does not currently allow you to set the log verbosity level. To enable logging, you will need to configure profiling on each DB that you are interested in collecting from. To do this, run the following in your Mongo shell:

```
dbnames = db.getMongo().getDBNames()
for (var k in dbnames) { adb = db.getMongo().getDB(dbnames[k]); adb.setProfilingLevel(2, -1); }
```

### Atlas Permissions

Your user will need ownership permissions on the cluster to pull the logs. See [Atlas Logs API](https://docs.atlas.mongodb.com/reference/api/logs/) for more information.

### Running the Atlas integration

```bash
# Your Atlas API Key, configured at:
# https://cloud.mongodb.com/v2#/account/publicApi
API_KEY=abcd1234-ab12-ab12-1234567890ab
# Your Atlas Username
USERNAME=myuser@example.com
# This is the Atlas Group ID/Project ID
# You can find this in your Atlass "Project Settings" page
GROUP_ID=abc1def2abc1def2ab9999
# Your Honeycomb Write Key
WRITE_KEY=abcdef1234567890999999999
# Target Honeycomb Dataset
DATASET=atlas-logs
atlas \
  --api_key=${API_KEY} \
  --username=${USERNAME} \
  --group_id=${GROUP_ID} \
  --cluster=Cluster0 \
  --writekey=${WRITE_KEY} \
  --dataset=${DATASET}
```

You can specify more clusters by providing the ``--cluster`` parameter multiple times.

### Kubernetes

Atlas is a good candidate for running inside k8s. You can modify the included spec file (`kubernetes/atlas.example.yml`) with the required arguments (USERNAME, API_KEY, WRITE_KEY, etc) and apply with `kubectl apply`:

```
kubectl apply -f kubernetes/atlas.example.yml
```
