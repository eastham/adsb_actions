"""This module parses rules and actions, and applies them to flight data."""

import logging
from flight import Flight
from stats import Stats
from typing import Callable

logger = logging.getLogger(__name__)
logger.level = logging.INFO

class Rules:
    """This class represents the rules and associated state from one yaml rule file."""

    def __init__(self, data: dict):
        self.yaml_data : dict = data
        self.rule_exection_log = RuleExecutionLog()
        self.callbacks : dict[str, Callable]= {}    # mapping from yaml name to fn
        # TODO add some sanity checks to rules...no duplicate rule names, at least...

    def process_flight(self, flight: Flight) -> None:
        """Apply rules and actions to the current position of a given flight. """

        rule_items = self.yaml_data['rules'].items()

        for rule_name, rule_value in rule_items:
            logger.debug("Checking rules %s", rule_name)

            if self.conditions_match(flight, rule_value['conditions'], rule_name):
                logger.info("MATCH for rule '%s' for flight %s", rule_name, flight.flight_id)

                self.do_actions(flight, rule_value['actions'], rule_name)
            else:
                logger.debug("NOMATCH for rule '%s' for flight %s", rule_name, flight.flight_id)


    def conditions_match(self, flight: Flight, conditions: dict,
                         rule_name: str) -> bool:
        """Determine if the given rule conditions match for the given flight."""

        logger.debug("condition_match checking rules: %s", str(conditions))
        Stats.condition_match_calls += 1
        match_count = 0
        result = True

        if 'aircraft_list' in conditions:
            match_count += 1
            condition_value = conditions['aircraft_list']
            ac_list = self.yaml_data['aircraft_lists'][condition_value]
            result &= flight.flight_id in ac_list

        if 'min_alt' in conditions:
            match_count += 1
            condition_value = conditions['min_alt']
            result &= flight.lastloc.alt_baro >= int(condition_value)

        if 'max_alt' in conditions:
            match_count += 1
            condition_value = conditions['max_alt']
            result &= flight.lastloc.alt_baro <= int(condition_value)

        if 'transition_regions' in conditions:
            match_count += 1
            condition_value = conditions['transition_regions']
            result &= (flight.was_in_bboxes([condition_value[0]]) and
                       flight.is_in_bboxes([condition_value[1]]))

        if 'regions' in conditions:
            match_count += 1
            condition_value = conditions['regions']
            result &= flight.is_in_bboxes(condition_value)

        if 'latlongring' in conditions:
            match_count += 1
            condition_value = conditions['latlongring']
            dist = flight.lastloc.distfrom(condition_value[1], condition_value[2])
            result &= condition_value[0] >= dist

        if 'proximity' in conditions:
            match_count += 1
            result = False  # handled asynchronously in handle_proximity_conditions

        if 'cooldown' in conditions:
            match_count += 1
            condition_value = conditions['cooldown']
            result &= not self.rule_exection_log.within_cooldown(rule_name,
                                                                flight.flight_id,
                                                                condition_value*60,
                                                                flight.lastloc.now)

        if match_count < len(conditions):
            logger.critical("unmatched condition: %s", conditions.keys())

        Stats.condition_matches_true += match_count if result else 0
        return result

    def do_actions(self, flight: Flight, action_items: dict, rule_name: str,
                   cb_arg = None) -> None:
        """Execute the actions for the given flight."""

        for action_name, action_value in action_items.items():
            self.rule_exection_log.log(rule_name, flight.flight_id, flight.lastloc.now)
            if 'webhook' == action_name:
                Stats.webhooks_fired += 1
                # TODO not implemented - see page.py for more info
                logger.critical("NOT IMPLEMENTED: webhook for %s", flight.flight_id)

            elif 'print' == action_name:
                print(f"Rule {rule_name} matched for {flight.flight_id}")

            elif 'callback' == action_name:
                Stats.callbacks_fired += 1
                Stats.last_callback_flight = flight
                logger.debug("Doing callback for %s", flight.flight_id)
                if cb_arg:
                    # this is used for proximity events when you want to 
                    # be able to refer to both flights that are near each other
                    self.callbacks[action_value](flight, cb_arg)
                else:
                    # all non-proximity events go here
                    self.callbacks[action_value](flight)

            elif 'note' == action_name:
                # Attach a note to this flight for later use, typically in 
                # another rule's callback.
                logger.debug("Setting note for %s to %s", flight.flight_id, action_value)
                flight.flags['note'] = action_value

            elif 'expire_callback' == action_name:
                pass # this is handled upon asynchronous expiration in do_expire()

            else:
                logger.warning("Unmatched action: %s", action_name)

    def do_expire(self, flight: Flight) -> None:
        """Handle flight expiration rules.
        
        The given flight is about to be evicted from the system, 
        see if any actions are needed.  This type of rule will be 
        needed for UI implementations at least.
        TODO: tests needed."""

        for rule_name, rule_value in self.yaml_data['rules'].items():
            actions = rule_value['actions']

            if ( "expire_callback" in actions and
                self.conditions_match(flight, rule_value['conditions'], rule_name)):
                logger.debug("doing expire callback for %s", flight.flight_id)

                self.callbacks[actions['expire_callback']](flight)

    def get_rules_with_condition(self, condition_type) -> list:
        """Returns a list of name/rule tuples that have a condition of the given type."""

        rules_list = self.yaml_data['rules']
        ret = []
        for rule_name, rule_body in rules_list.items():
            if condition_type in rule_body['conditions']:
                ret.append((rule_name, rule_body))
        return ret

    def handle_proximity_conditions(self, flights, last_read_time) -> None:
        """
        This is run periodically to check distance between all aircraft --
        to check for any matching proximity conditions.  
        It's O(n^2), can be expensive, but altitude and bbox limits help...

        NOTE: currently flights not in any bbox are not checked, to improve
        execution time.
        """

        prox_rules_list = self.get_rules_with_condition("proximity")
        if prox_rules_list == []:
            return

        for flight1 in flights.flight_dict.values():
            if not flight1.in_any_bbox():
                continue

            for rule_name, rule_body in prox_rules_list:
                # For each proximity rule, we want to check the rule conditions
                # here, first removing the prox part of the rule which will 
                # never match during the usual synchronous update.
                rule_conditions = rule_body['conditions'].copy() # XXX inefficient?
                prox_rule_element = rule_conditions['proximity']
                altsep, latsep = prox_rule_element
                del rule_conditions['proximity']

                if self.conditions_match(flight1, rule_conditions, rule_name):
                    # Satisfied prox rule found, now see if there are nearby aircraft.
                    # NOTE that this only returns one flight, so we won't always have
                    # two actions fired for every pair of close-proximity aircraft.
                    flight2 = flights.find_nearby_flight(flight1, altsep, latsep,
                                                         last_read_time)
                    if flight2:
                        logger.debug("Proximity match: %s %s", flight1.flight_id,
                                     flight2.flight_id)
                        self.do_actions(flight1, rule_body['actions'], rule_name,
                                        flight2)

class RuleExecutionLog:
    """Keep track of last execution times for each rule/aircraft.
    This enables the "cooldown" condition to inhibit rules that 
    shouldn't fire frequently (like a rule sending a pager alert)
    
    Basically a dict of (rulename, flight_id) -> last-execution-timestamp"""

    def __init__(self):
        # (rulename, flight_id) -> last-execution-timestamp
        self.log_entries: dict[tuple[str, str], int] = {}

    def generate_entry_key(self, rulename: str, flight_id: str) -> tuple:
        return rulename, flight_id

    def log(self, rulename: str, flight_id: str, now: int) -> None:
        entry_key = self.generate_entry_key(rulename, flight_id)
        self.log_entries[entry_key] = now

    def within_cooldown(self, rulename: str, flight_id: str, cooldown: int, 
                        now: int) -> bool:
        entry_key = self.generate_entry_key(rulename, flight_id)
        if entry_key in self.log_entries:
            if now - self.log_entries[entry_key] < cooldown:
                return True
        return False
