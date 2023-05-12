>Author: hieu.hoang@tpptechnology.com
>Created at: 2023-02-19
>Updated at: 2023-02-19

# Overview
Deployment is done mostly automatically using [GitLab CI/CD](https://docs.gitlab.com/ee/ci/) and local [GitLab Runner](https://docs.gitlab.com/runner/). The deployment pipeline is defined in `.gitlab-ci.yml` files within each folder, and a top-level `.gitlab-ci.yml` file. Deployment variables for all environments are stored in [GitLab repo's CI/CD Setting](https://gitlab.com/tpp-leonardo/leo-etl/-/settings/ci_cd), under `Variables` section.

Generally, a deployment pipeline for a Leonardo's DP component consists of 2 step: `Build` and `Deploy`. Build steps generally build a Docker image from the latest code in the deployed branch, and push to a container registry. Deploy steps pull the latest image from the registry and re-create the containers with changed images. There are 2 exceptions:
- `deployment` folder does not contain codes for a normal Python component, but is a collection of all Jinja templates that will be rendered into `docker-compose.yaml` files and `.env` files. 
	- When there is a change in this folder, build step will re-render the files using Deployment variables.
	- After a successful build, a deploy step must be triggered manually.  When triggered, deploy step uploads rendered files to a Google Cloud Storage bucket, usually with `deployment` in name.
	- After a successful deployment, developer must manually `ssh` into the machines that host the containers, and sync the changes from GCS using `gsutil rsync`, and re-create the containers using the updated `docker-compose.yaml` and `.env` files.
- `data_migration` folder contain an [`alembic`](https://alembic.sqlalchemy.org/en/latest/) project. Changes in databases are defined here and deployed to all environments to maintain database consistency.
	- When there are changes or new migrations added, deployment pipeline is triggered. Build step builds a Docker image that contains the latest migration code from the branch that triggers the deployment.
	- After a successful build, a migration step must be manually triggered. However, this is only recommended on Staging. For Production, there are some issues with the deploy step, so it must be manually run by `ssh` into Production machine and run the containers. More information in each environment below.

# Development

## Checklist

## Deployment templates

## Migration

# Staging

## Checklist

## Deployment templates

## Migration

# Production

## Checklist

## Deployment templates

## Migration
