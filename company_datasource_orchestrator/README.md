# Environment

- `GOOGLE_APPLICATION_CREDENTIALS` is only needed on your local machine to work with google cloud api. No need on vm instance as it already linked with a application service account on google.

- `export GOOGLE_APPLICATION_CREDENTIALS=~/.ssh/leo-#####-20200325-de0fd81409d7.json`


# How to run
```shell
docker-compose build
docker-compose up
```

# Run in production
```shell
docker-compose up -d
```

# CI/CD
``` shell
```
