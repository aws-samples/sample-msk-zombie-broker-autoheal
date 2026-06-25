"""
Regression tests — lock in the two bugs found by the live-MSK POC (2026-06-20)
so they can never silently come back, plus the reboot-deferred error path.

Reuses the boto3 stub + fakes from test_guardrails (importing it runs that setup).
Run: python3 -m unittest discover -s tests
"""
import unittest
import test_guardrails as G  # sets env, stubs boto3, imports selfheal_lambda as G.L

L = G.L


class RecordingCW:
    """Captures the MetricDataQueries that _scan builds, returns empty values."""
    def __init__(self): self.queries = None
    def get_metric_data(self, MetricDataQueries, **k):
        self.queries = MetricDataQueries
        return {"MetricDataResults": [{"Id": q["Id"], "Values": []} for q in MetricDataQueries]}


class Bug1_UrpDimensionRegression(unittest.TestCase):
    """Bug 1: MSK emits UnderReplicatedPartitions ONLY per-broker [Cluster Name, Broker ID].
    The old code queried it cluster-only -> urp always false -> never detects.
    _scan MUST build a per-broker URP query (with a Broker ID dim) for every broker,
    and MUST NOT build a cluster-only 'urp' query."""

    def setUp(self):
        self.cw = RecordingCW(); L.cw = self.cw

    def test_scan_builds_per_broker_urp_queries_with_broker_dim(self):
        L._scan(3)
        ids = [q["Id"] for q in self.cw.queries]
        # per-broker URP queries exist for every broker
        for b in (1, 2, 3):
            self.assertIn(f"urp{b}", ids, f"missing per-broker URP query urp{b}")
            self.assertIn(f"bin{b}", ids, f"missing per-broker BytesIn query bin{b}")
        # the buggy cluster-only 'urp' query must NOT exist
        self.assertNotIn("urp", ids, "cluster-only 'urp' query reintroduced (Bug 1 regression!)")

    def test_urp_query_carries_broker_id_dimension(self):
        L._scan(2)
        for q in self.cw.queries:
            if q["Id"].startswith("urp"):
                dims = {d["Name"]: d["Value"] for d in q["MetricStat"]["Metric"]["Dimensions"]}
                self.assertEqual(q["MetricStat"]["Metric"]["MetricName"], "UnderReplicatedPartitions")
                self.assertIn("Broker ID", dims, "URP query lost its per-broker Broker ID dimension")
                self.assertIn("Cluster Name", dims)


class ClusterUnderReplicatedAggregation(unittest.TestCase):
    """_cluster_under_replicated must be True iff ANY broker reports URP>0 within lookback."""

    def test_any_broker_positive_is_true(self):
        res = {"urp1": [0, 0, 0], "urp2": [0, 30, 0], "urp3": [0, 0, 0]}
        self.assertTrue(L._cluster_under_replicated(res, 3))

    def test_all_zero_is_false(self):
        res = {"urp1": [0, 0, 0], "urp2": [0, 0, 0], "urp3": [0, 0, 0]}
        self.assertFalse(L._cluster_under_replicated(res, 3))

    def test_missing_series_is_false(self):
        self.assertFalse(L._cluster_under_replicated({}, 3))


class RebootDeferredErrorPath(unittest.TestCase):
    """Hardening: if kafka:RebootBroker raises (e.g. a cluster op is already in
    progress), the Lambda must NOT crash, must record cooldown state (so it can't
    loop), and must return action=reboot_deferred."""

    def test_reboot_api_error_records_cooldown_and_defers(self):
        class RaisingKafka(G.FakeKafka):
            def reboot_broker(self, ClusterArn, BrokerIds):
                raise RuntimeError("A cluster operation is already in progress")
        ddb = G.FakeDDB()
        k = RaisingKafka()
        # single confirmed zombie: broker 1 BytesIn=0, URP>0, live mode
        G.wire(kafka=k, ddb=ddb,
               cw=G.FakeCW({1: [0, 0, 0], 2: [9, 9, 9], 3: [9, 9, 9]}, [1, 1, 1]), dry=False)
        r = L.handler({}, None)
        self.assertEqual(r["action"], "reboot_deferred")
        self.assertEqual(r["broker"], 1)
        self.assertEqual(k.rebooted, [])  # no successful reboot recorded
        # cooldown state WAS written despite the API error (prevents tight retry loop)
        self.assertIn("broker#1", ddb.items)
        self.assertEqual(ddb.items["broker#1"]["reboots_today"]["N"], "1")
        # immediate re-invoke -> cooldown guardrail blocks another attempt
        r2 = L.handler({}, None)
        self.assertEqual(r2["action"], "cooldown")


if __name__ == "__main__":
    unittest.main()
