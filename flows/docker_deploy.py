from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from etl_data import etl_flow


docker_container_block = DockerContainer.load("de-zoomcamp")

deploy = Deployment.build_from_flow(
    flow=etl_flow,
    name='docker-flow',
    infrastructure=docker_container_block
)


if __name__ == "__main__":
    deploy.apply()