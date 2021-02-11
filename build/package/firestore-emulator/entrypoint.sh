#!/bin/bash

set -xe

# Config gcloud project
gcloud config set project "${FIRESTORE_PROJECT_ID}"

# Start emulator
gcloud beta emulators firestore start \
  --host-port="0.0.0.0:${PORT}"