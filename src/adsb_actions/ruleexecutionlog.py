"""Keep track of execution counts and last execution times for each 
rule/aircraft."""

class ExecutionCounter:
    """execution counter for a single rule.  Keeps track of how many times
    the rule has fired, and how many times it fired with each note. 
    
    Attributes:
        rulename: name of the rule
        count: number of times the rule fired
        note_dict: dict of notes -> count of times the rule fired with that 
            note"""

    def __init__(self, rulename: str):
        self.rulename = rulename

        self.count = 0
        self.note_dict: dict[str, int] = {}

    def increment(self, note: str = None):
        self.count += 1
        if note:
            if not note in self.note_dict:
                self.note_dict[note] = 0
            self.note_dict[note] += 1

    def print_report(self):
        print(f"Rule {self.rulename} matched {self.count} times.")
        for note, count in self.note_dict.items():
            print(f"    Including {note} {count} times.")

class RuleExecutionLog:
    """Keep track of last execution times for each rule/aircraft.
    This enables the "cooldown" condition to inhibit rules that 
    shouldn't fire frequently (like a rule sending a pager alert)
    
    Attributes:
        last_execution_time (dict): (rulename, flight_id) -> last-execution-timestamp
        rule_execution_counters (dict): rulename -> counter of executions
    """

    def __init__(self):
        self.last_execution_time: dict[tuple[str, str], int] = {}
        self.rule_execution_counters: dict[str, ExecutionCounter] = {}

    def log(self, rulename: str, flight_id: str, now: int, note: str) -> None:
        """Log a firing of the given rulename + flight"""
        if rulename not in self.rule_execution_counters:
            self.rule_execution_counters[rulename] = ExecutionCounter(rulename)
        counter = self.rule_execution_counters[rulename]
        counter.increment(note)

        # log entry for this rule + flight
        entry_key = self._generate_entry_key(rulename, flight_id)
        self.last_execution_time[entry_key] = now

        # log entry for this rule only, used for rule_cooldown
        rule_key = self._generate_entry_key(rulename, "")
        self.last_execution_time[rule_key] = now

    def within_cooldown(self, rulename: str, flight_id: str, cooldown_secs: int, 
                        now: int) -> bool:
        """Has the given rulename fired for the given flight within cooldown_secs?"""
        entry_key = self._generate_entry_key(rulename, flight_id)
        if entry_key in self.last_execution_time:
            if now - self.last_execution_time[entry_key] < cooldown_secs:
                return True
        return False

    def _generate_entry_key(self, rulename: str, flight_id: str) -> tuple:
        return rulename, flight_id
