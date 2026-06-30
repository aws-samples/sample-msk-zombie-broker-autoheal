#!/usr/bin/env python3
"""Render the AWS architecture diagram for sample-msk-zombie-broker-autoheal.

Uses the `diagrams` library (official AWS icon set) + Graphviz.

    pip install diagrams   # requires the `dot` binary (brew install graphviz)
    python3 docs/architecture_diagram.py   # -> docs/img/architecture-aws.png
"""
from diagrams import Diagram, Cluster, Edge
from diagrams.aws.compute import Lambda
from diagrams.aws.integration import Eventbridge, SimpleNotificationServiceSns
from diagrams.aws.analytics import ManagedStreamingForKafka
from diagrams.aws.database import Dynamodb
from diagrams.aws.management import Cloudwatch, CloudwatchLogs

graph_attr = {"fontsize": "18", "bgcolor": "white", "pad": "0.6", "splines": "spline"}
node_attr = {"fontsize": "13"}

with Diagram(
    "Amazon MSK Zombie Broker Auto-Heal",
    filename="docs/img/architecture-aws",
    outformat="png",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    node_attr=node_attr,
):
    sched = Eventbridge("EventBridge Scheduler\n(rate: every 5 min)")

    with Cluster("Self-heal control loop"):
        fn = Lambda("selfheal Lambda\n(scanner + actuator)")
        ddb = Dynamodb("DynamoDB\n(cooldown / reboot state)")
        logs = CloudwatchLogs("CloudWatch Logs")

    cw = Cloudwatch("CloudWatch Metrics\nBytesInPerSec +\nper-broker URP")

    with Cluster("Amazon MSK (ZooKeeper, 3 brokers)"):
        msk = ManagedStreamingForKafka("MSK cluster")

    sns = SimpleNotificationServiceSns("SNS\n(operator alert)")

    sched >> Edge(label="invoke") >> fn
    msk >> Edge(label="emits metrics", style="dashed", color="darkgreen") >> cw
    fn >> Edge(label="GetMetricData") >> cw
    fn >> Edge(label="cooldown state") >> ddb
    fn >> Edge(label="kafka:RebootBroker (on zombie)", color="firebrick", fontcolor="firebrick") >> msk
    fn >> Edge(label="notify") >> sns
    fn >> Edge(style="dotted") >> logs
