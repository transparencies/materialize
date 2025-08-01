# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from kubernetes.client import (
    V1Container,
    V1ContainerPort,
    V1Deployment,
    V1DeploymentSpec,
    V1EnvVar,
    V1LabelSelector,
    V1ObjectMeta,
    V1PodSpec,
    V1PodTemplateSpec,
    V1Service,
    V1ServicePort,
    V1ServiceSpec,
)

from materialize.cloudtest import DEFAULT_K8S_NAMESPACE
from materialize.cloudtest.k8s.api.k8s_deployment import K8sDeployment
from materialize.cloudtest.k8s.api.k8s_resource import K8sResource
from materialize.cloudtest.k8s.api.k8s_service import K8sService


class MySqlService(K8sService):
    def __init__(
        self,
        namespace: str,
    ) -> None:
        super().__init__(namespace)
        service_port = V1ServicePort(name="sql", port=3306)

        self.service = V1Service(
            api_version="v1",
            kind="Service",
            metadata=V1ObjectMeta(
                name="mysql", namespace=namespace, labels={"app": "mysql"}
            ),
            spec=V1ServiceSpec(
                type="NodePort",
                ports=[service_port],
                selector={"app": "mysql"},
            ),
        )


class MySqlDeployment(K8sDeployment):
    def __init__(self, namespace: str, apply_node_selectors: bool) -> None:
        super().__init__(namespace)
        env = [
            V1EnvVar(name="MYSQL_ROOT_PASSWORD", value="p@ssw0rd"),
        ]
        ports = [V1ContainerPort(container_port=3306, name="sql")]
        container = V1Container(
            name="mysql",
            image=self.image("mysql", tag="9.4.0", release_mode=True, org=None),
            args=[
                "--log-bin=mysql-bin",
                "--gtid_mode=ON",
                "--enforce_gtid_consistency=ON",
                "--binlog-format=row",
                "--log-slave-updates",
                "--binlog-row-image=full",
                "--server-id=1",
            ],
            env=env,
            ports=ports,
        )

        node_selector = None
        if apply_node_selectors:
            node_selector = {"supporting-services": "true"}

        template = V1PodTemplateSpec(
            metadata=V1ObjectMeta(namespace=namespace, labels={"app": "mysql"}),
            spec=V1PodSpec(containers=[container], node_selector=node_selector),
        )

        selector = V1LabelSelector(match_labels={"app": "mysql"})

        spec = V1DeploymentSpec(replicas=1, template=template, selector=selector)

        self.deployment = V1Deployment(
            api_version="apps/v1",
            kind="Deployment",
            metadata=V1ObjectMeta(name="mysql", namespace=namespace),
            spec=spec,
        )


def mysql_resources(
    namespace: str = DEFAULT_K8S_NAMESPACE, apply_node_selectors: bool = False
) -> list[K8sResource]:
    return [
        MySqlService(namespace),
        MySqlDeployment(namespace, apply_node_selectors),
    ]
