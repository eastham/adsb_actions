"""class test for the cooldown math."""

from time import time
from rules import RuleExecutionLog
from stats import Stats

def test_rule_execution_log():
    Stats.reset()
    rel = RuleExecutionLog()

    timestamp = time()
    rel.log("rulename", "n12345", timestamp, "")
    assert rel.within_cooldown("rulename", "n12345", 100, timestamp+1)
    assert rel.within_cooldown("rulename", "n12345", 100, timestamp+99)
    assert not rel.within_cooldown("rulename", "n12345", 100, timestamp+100)
    assert not rel.within_cooldown("rulename", "n12345", 100, timestamp+150)
    assert len(rel.last_execution_time) == 1
    assert rel.rule_execution_counters['rulename'].count == 1

    rel.log("rulename2", "n123", timestamp, "test note")
    assert not rel.within_cooldown("rulename", "n123", 100, timestamp+1)
    assert rel.within_cooldown("rulename2", "n123", 100, timestamp+1)
    assert rel.within_cooldown("rulename", "n12345", 100, timestamp+1)
    assert rel.rule_execution_counters['rulename2'].count == 1

    for _, counter in rel.rule_execution_counters.items():
        counter.print_report()
