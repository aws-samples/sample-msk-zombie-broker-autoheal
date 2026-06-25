"""
Offline unit tests for the self-heal guardrail logic.
No AWS calls, no boto3 needed at runtime: we stub a fake `boto3` module and the
metric/state data, then drive selfheal_lambda.handler through every guardrail
branch. Run:  python3 -m unittest -v  (from the repo root)
"""
import os
import sys
import types
import unittest

# --- stub boto3 BEFORE importing the lambda (it calls boto3.client at import) ---
_fake = types.ModuleType("boto3")
_fake.client = lambda *a, **k: object()  # replaced per-test below
sys.modules["boto3"] = _fake

os.environ.update({
    "CLUSTER_ARN": "arn:aws:kafka:us-east-1:111122223333:cluster/demo/abc-1",
    "CLUSTER_NAME": "demo", "REGION": "us-east-1", "STATE_TABLE": "t",
    "SNS_TOPIC_ARN": "arn:aws:sns:us-east-1:111122223333:demo-alerts",
    "COOLDOWN_S": "600", "DAILY_CAP": "4", "DETECT_WINDOW_MIN": "3", "DRY_RUN": "false",
})

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import selfheal_lambda as L  # noqa: E402


class FakeKafka:
    def __init__(self, n=3): self.n = n; self.rebooted = []
    def describe_cluster(self, ClusterArn):
        return {"ClusterInfo": {"NumberOfBrokerNodes": self.n, "State": "ACTIVE"}}
    def reboot_broker(self, ClusterArn, BrokerIds):
        self.rebooted += BrokerIds
        return {"ClusterOperationArn": "arn:op/1"}


class FakeCW:
    """bytes_in: {broker_id: [vals newest-first]}, urp: [vals newest-first] (applied to every broker)"""
    def __init__(self, bytes_in, urp): self.bytes_in = bytes_in; self.urp = urp
    def get_metric_data(self, MetricDataQueries, **k):
        out = []
        for q in MetricDataQueries:
            qid = q["Id"]
            if qid.startswith("urp"):
                out.append({"Id": qid, "Values": list(self.urp)})
            else:
                b = int(qid[3:])
                out.append({"Id": qid, "Values": list(self.bytes_in.get(b, []))})
        return {"MetricDataResults": out}


class FakeDDB:
    def __init__(self, items=None): self.items = items or {}
    def get_item(self, TableName, Key):
        k = Key["brokerSk"]["S"]
        return {"Item": self.items[k]} if k in self.items else {}
    def put_item(self, TableName, Item):
        self.items[Item["brokerSk"]["S"]] = Item


class FakeSNS:
    def __init__(self): self.msgs = []
    def publish(self, **k): self.msgs.append(k)


def wire(kafka=None, cw=None, ddb=None, sns=None, dry=False):
    L.kafka = kafka or FakeKafka()
    L.cw = cw or FakeCW({1: [10, 10, 10], 2: [10, 10, 10], 3: [10, 10, 10]}, [0, 0, 0])
    L.ddb = ddb or FakeDDB()
    L.sns = sns or FakeSNS()
    L.DRY_RUN = dry


def item(broker, last_reboot=0, reboots_today=0, day="", consecutive=0):
    return {"clusterArn": {"S": L.CLUSTER_ARN}, "brokerSk": {"S": f"broker#{broker}"},
            "last_reboot": {"N": str(last_reboot)}, "reboots_today": {"N": str(reboots_today)},
            "day": {"S": day}, "consecutive": {"N": str(consecutive)}}


class GuardrailTests(unittest.TestCase):
    def test_no_zombie(self):
        wire()  # all brokers have BytesIn>0, URP=0
        self.assertEqual(L.handler({}, None)["action"], "none")

    def test_healthy_broker_high_bytes_urp0(self):
        # URP=0 means even a 0-bytes broker is NOT a zombie (idle, not stalled)
        wire(cw=FakeCW({1: [0, 0, 0], 2: [10, 10, 10], 3: [10, 10, 10]}, [0, 0, 0]))
        self.assertEqual(L.handler({}, None)["action"], "none")

    def test_single_zombie_reboots(self):
        k = FakeKafka()
        wire(kafka=k, cw=FakeCW({1: [0, 0, 0], 2: [10, 10, 10], 3: [10, 10, 10]}, [2, 2, 2]))
        r = L.handler({}, None)
        self.assertEqual(r["action"], "reboot_broker"); self.assertEqual(r["broker"], 1)
        self.assertEqual(k.rebooted, ["1"])

    def test_two_zombies_escalate_lse_no_reboot(self):
        k = FakeKafka()
        wire(kafka=k, cw=FakeCW({1: [0, 0, 0], 2: [0, 0, 0], 3: [10, 10, 10]}, [5, 5, 5]))
        r = L.handler({}, None)
        self.assertEqual(r["action"], "escalate_lse"); self.assertEqual(k.rebooted, [])

    def test_cooldown_skips(self):
        import time
        ddb = FakeDDB({"broker#1": item(1, last_reboot=int(time.time()) - 60)})  # 60s ago < 600
        k = FakeKafka()
        wire(kafka=k, ddb=ddb, cw=FakeCW({1: [0, 0, 0], 2: [9, 9, 9], 3: [9, 9, 9]}, [1, 1, 1]))
        r = L.handler({}, None)
        self.assertEqual(r["action"], "cooldown"); self.assertEqual(k.rebooted, [])

    def test_reboot_ineffective_escalates_replacenode(self):
        ddb = FakeDDB({"broker#1": item(1, last_reboot=1, consecutive=1)})  # old reboot, still zombie
        k = FakeKafka()
        wire(kafka=k, ddb=ddb, cw=FakeCW({1: [0, 0, 0], 2: [9, 9, 9], 3: [9, 9, 9]}, [1, 1, 1]))
        r = L.handler({}, None)
        self.assertEqual(r["action"], "escalate_replacenode"); self.assertEqual(k.rebooted, [])

    def test_daily_cap_escalates(self):
        import datetime
        today = datetime.date.today().isoformat()
        ddb = FakeDDB({"broker#1": item(1, last_reboot=1, reboots_today=4, day=today, consecutive=0)})
        k = FakeKafka()
        wire(kafka=k, ddb=ddb, cw=FakeCW({1: [0, 0, 0], 2: [9, 9, 9], 3: [9, 9, 9]}, [1, 1, 1]))
        r = L.handler({}, None)
        self.assertEqual(r["action"], "daily_cap"); self.assertEqual(k.rebooted, [])

    def test_dry_run_notifies_without_reboot(self):
        k = FakeKafka()
        wire(kafka=k, cw=FakeCW({1: [0, 0, 0], 2: [9, 9, 9], 3: [9, 9, 9]}, [1, 1, 1]), dry=True)
        r = L.handler({}, None)
        self.assertEqual(r["action"], "dry_run_would_reboot"); self.assertEqual(k.rebooted, [])

    def test_healthy_resets_consecutive(self):
        ddb = FakeDDB({"broker#2": item(2, consecutive=1)})
        wire(ddb=ddb)  # default: all healthy, URP=0
        L.handler({}, None)
        self.assertEqual(ddb.items["broker#2"]["consecutive"]["N"], "0")


if __name__ == "__main__":
    unittest.main()
