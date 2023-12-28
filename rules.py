import logging
from callbacks import Callbacks
from flight import Flight
from stats import Stats

logger = logging.getLogger(__name__)
logger.level = logging.DEBUG

class RuleExecutionLog:
    """Keep track of last execution times for each rule/aircraft.
    
    Basically a dict of rulenameXXXaircraft -> timestamp"""
    SEP = " XxX "
    def __init__(self):
        self.log_entries: dict = {}

    def get_entry_name(self, rulename: str, flight_id: str) -> str:
        return rulename + self.SEP + flight_id

    def log(self, rulename: str, flight_id: str, now: int) -> None:
        entry_name = self.get_entry_name(rulename, flight_id)
        self.log_entries[entry_name] = now

    def within_cooldown(self, rulename: str, flight_id: str, cooldown: int, now: int) -> bool:
        entry_name = self.get_entry_name(rulename, flight_id)
        if entry_name in self.log_entries:
            if now - self.log_entries[entry_name] < cooldown:
                return True
        return False

class Rules:
    yaml_data: dict = {}
    rule_exection_log = RuleExecutionLog()

    def __init__(self, data: dict):
        self.yaml_data = data

    def process_flight(self, flight: Flight) -> None:
        rule_items = self.yaml_data['rules'].items()
        for rule_name, rule_value in rule_items:
            logger.info("Checking rules %s", rule_name)
            if self.conditions_match(flight, rule_value['conditions'], rule_name):
                logger.info("MATCH rule %s", rule_name)
                self.do_actions(flight, rule_value['actions'], rule_name)

    def conditions_match(self, flight: Flight, conditions: dict,
                         rule_name: str) -> bool:
        overall_result = True
        Stats.condition_match_calls += 1
        logger.info("condition_match checking rules: %s", str(conditions))
        for condition_name, condition_value in conditions.items():
            result = False
            if 'aircraft_list' == condition_name:
                #print(f"checking aircraft list {condition_value}")
                ac_list = self.yaml_data['aircraft_lists'][condition_value]
                #print(f"ac list is {ac_list}")
                result = flight.flight_id in ac_list
            elif 'transition_regions' == condition_name:
                result = (flight.was_in_bboxes([condition_value[0]]) and
                          flight.is_in_bboxes([condition_value[1]]))
            elif 'regions' == condition_name:
                result = flight.is_in_bboxes(condition_value)
            elif 'proximity' == condition_name:
                logger.critical("proximity condition not implemented")
            elif 'cooldown' == condition_name:
                result = not self.rule_exection_log.within_cooldown(rule_name,
                                                                    flight.flight_id,
                                                                    condition_value*60,
                                                                    flight.lastloc.now)
                pass
            else:
                logger.warning("unmatched condition: %s", condition_name)

            if result:
                Stats.condition_matches_true += 1
                logger.info("one condition matched: %s for %s", condition_name, flight.flight_id)
            overall_result = overall_result and result
        logger.info("overall result %s", str(overall_result))
        return overall_result

    def do_actions(self, flight: Flight, action_items: dict, rule_name: str) -> None:
        for action_name, action_value in action_items.items():
            self.rule_exection_log.log(rule_name, flight.flight_id, flight.lastloc.now)
            if 'slack' == action_name:
                logger.debug("doing slack for %s", flight.flight_id)
            elif 'page' == action_name:
                logger.debug("doing page for %s", flight.flight_id)
            elif 'callback' == action_name:
                Stats.callbacks_fired += 1
                logger.debug("doing callback for %s", flight.flight_id)
                func = getattr(Callbacks, action_value)
                if func:
                    func(flight)
                else:
                    logger.warning("callback not found: %s", action_value)
            elif 'note' == action_name:
                logger.debug("SETTING NOTE for %s", flight.flight_id)
                flight.flags['note'] = action_value
            else:
                logger.warning("unmatched action: %s", action_name)

    def do_expire(self, flight: Flight) -> None:
        for rule_name, rule_value in self.yaml_data['rules'].items():
            if (rule_name == "expire_callback_rule" and
                self.conditions_match(flight, rule_name, rule_value)):
                logger.debug("doing expire callback for %s", flight.flight_id)
                func = globals().get(rule_value)
                func(flight)

    def handle_proximity_condition(self, flight_list: list) -> None:
        """
        Check distance between all aircraft, if any prox conditions are used.
        O(n^2), can be expensive, but altitude and bbox limits help..

        # XXX delete these:
        MIN_ALT_SEPARATION = 400 # 8000 # 400
        MIN_ALT = 4000 # 100 # 4000
        MIN_DISTANCE = .3 # 1   # .3 # nautical miles 
        MIN_FRESH = 10 # seconds, otherwise not evaluated

        # XXX for each rule with proximity...
            # XXXcheck other non-prox conditions...
            # XXX load parameters from rule_value...
        for i, flight1 in enumerate(flight_list):
            if not flight1.in_any_bbox(): continue
            if last_read_time - flight1.lastloc.now > MIN_FRESH: continue
            for j, flight2 in enumerate(flight_list[i+1:]):
                if not flight2.in_any_bbox(): continue
                if last_read_time - flight2.lastloc.now > MIN_FRESH: continue

                loc1 = flight1.lastloc
                loc2 = flight2.lastloc
                if (loc1.alt_baro < MIN_ALT or loc2.alt_baro < MIN_ALT): continue
                if abs(loc1.alt_baro - loc2.alt_baro) < MIN_ALT_SEPARATION:
                    dist = loc1 - loc2

                    if dist < MIN_DISTANCE:
                        print("%s-%s inside minimum distance %.1f nm" %
                            (flight1.flight_id, flight2.flight_id, dist))
                        print("LAT, %f, %f, %d" % (flight1.lastloc.lat, flight1.lastloc.lon, last_read_time))
                        #if annotate_cb:
                        #    annotate_cb(flight1, flight2, dist, abs(loc1.alt_baro - loc2.alt_baro))
                        #    annotate_cb(flight2, flight1, dist, abs(loc1.alt_baro - loc2.alt_baro))
""" 
