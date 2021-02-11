FROM google/cloud-sdk:alpine
RUN apk --update add openjdk8-jre
RUN gcloud components install cloud-firestore-emulator beta --quiet

COPY entrypoint.sh .

ENV PORT 8080
EXPOSE "$PORT"

ENV FIRESTORE_PROJECT_ID "dummy-firestore-id"

ENTRYPOINT ["./entrypoint.sh"]
