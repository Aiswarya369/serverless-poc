# MSI Load Control Functions

## Purpose

***
Includes Lambdas for handling load control on meters
See: [Load Control services](https://cresconet.atlassian.net/wiki/spaces/MSI/pages/2425815201/Load+Control+services)

## Resources

***

### Lambda Functions

- `msi-<stage>-dlc-override-apigw-fn`:
    - Source: [dlc_override_apigw_fn.py](./src/lambdas/dlc_override_apigw_fn.py)
    - Docs:
      [Lambda - initiate Load Control override](https://cresconet.atlassian.net/wiki/spaces/MSI/pages/2426437695/Lambda+-+initiate+Load+Control+override)


- `msi-<stage>-dlc-override-statemachine-fn`:
    - Source: [dlc_override_statemachine_fn.py](./src/lambdas/dlc_override_statemachine_fn.py)
    - Docs:
      [Lambda - perform direct Load Control override](https://cresconet.atlassian.net/wiki/spaces/MSI/pages/2426208323/Lambda+-+perform+direct+Load+Control+override)


- `msi-<stage>-get-request-status`:
    - Source: [dlc_get_request_status.py](./src/lambdas/dlc_get_request_status.py)
    - Docs:
      [Lambda - get direct Load Control request status](https://cresconet.atlassian.net/wiki/spaces/MSI/pages/2468577281/Lambda+-+get+direct+Load+Control+request+status)


- `msi-<stage>-dlc-cancel-override-apigw-fn`:
    - Source: [dlc_cancel_override_apigw_fn.py](./src/lambdas/dlc_cancel_override_apigw_fn.py)
    - Docs:
      [Lambda - Initiate Cancel Load Control Override](https://cresconet.atlassian.net/wiki/spaces/MSI/pages/2467266581/Lambda+-+Initiate+Cancel+Load+Control+Override)


- `msi-<stage>-dlc-cancel-override-statemachine-fn`:
    - Source: [dlc_override_statemachine_fn.py](./src/lambdas/dlc_override_statemachine_fn.py)
    - Docs:
      [State Machine - Cancel Load Control Override](https://cresconet.atlassian.net/wiki/spaces/MSI/pages/2581627043/State+Machine+-+Cancel+Load+Control+Override)

## Dependencies

***

### Local Pre-requisites

- Cresconet Nexus repositories set up in Poetry
  ````shell
  poetry config repositories.cresconet https://ci-cd.cresconet-services.com/nexus/repository/pypi-releases/simple 
  poetry config http-basic.cresconet <username> <password>
  ````
- IP Whitelisted in `cisystem-alb-sg` on `intellihub-nz` (Oncor Dev Account)

### Installing Dependencies

````shell
# installs dependencies from package-lock.json
npm ci 
# installs dependencies from poetry-lock.json
poetry install
````

### Incrementing Versions

When incrementing versions or adding/updating dependencies:

1. Ensure that versions in `package.json` and `pyproject.toml` are inline.
2. Update dependencies:
    ````shell
    # updates package-lock.json
    npm install
    # updates poetry-lock.json
    poetry update
    ````
3. Commit `package-lock.json` and `poetry.lock` files to source control

## Packaging

***
*In order to check serverless set up the following can be run:*

1. Install dependencies, see section above.
2. Package locally
    ````shell
    # Package with Serverless
    sls package --stage <stage>
    ````

## Deployment

***

### Pre-requisites

- Shared Lambda layers deployed as part of cloud formation stack
  [msi-lambda-layer](https://bitbucket.org/teamravens/msi-lambda-layer/src/develop/)

### Deployments

Deployments are done via Jenkins, using [`Jenkinsfile_deploy`](./Jenkinsfile_deploy)
and [CrescoNet Shared Pipelines](https://bitbucket.org/teamravens/cresconet-jenkins-shared-pipelines)