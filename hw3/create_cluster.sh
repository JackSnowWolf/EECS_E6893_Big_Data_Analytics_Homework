#!/bin/bash
gcloud beta dataproc clusters create hw3 \
	--optional-components=ANACONDA,JUPYTER \
	--image-version=preview --enable-component-gateway \
	--metadata 'PIP_PACKAGES=requests_oauthlib google-cloud-bigquery tweepy' \
	--metadata gcs-connector-version=1.9.16 \
	--metadata bigquery-connector-version=0.13.16 \
	--project  hardy-symbol-252200 \
	--bucket  big_data_storage \
	--initialization-actions=gs://dataproc-initialization-actions/python/pip-install.sh,gs://dataproc-initialization-actions/connectors/connectors.sh \
	--single-node

