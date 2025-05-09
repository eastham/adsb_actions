"""This module parses rules and actions, and applies them to flight data."""

import datetime
import logging
from typing import Callable
from .flight import Flight
from .stats import Stats
from .ruleexecutionlog import RuleExecutionLog, ExecutionCounter
from .adsb_logger import Logger
from .page import send_page, send_slack

logger = logging.getLogger(__name__)
logger.level = logging.INFO
LOGGER = Logger()

class Rules:
    """
    This class represents the conditions, agctions, and associated state from one
    yaml rule file.

    Attributes:
        yaml_data (dict): A dictionary containing the data from the YAML file.
        rule_exection_log (RuleExecutionLog): Tracks when rules were last fired,
            to support the "cooldown" condition.
        callbacks (dict[str, Callable]): callbacks that were registered with
            AdsbActions.register_callback().  Mapping from callback name to 
            function.
    """

    def __init__(self, data):
        self.yaml_data : dict = data
        self.rule_execution_log = RuleExecutionLog()
        self.callbacks : dict[str, Callable]= {}    # mapping from yaml name to fn

        if self.get_rules() is {}:
            logger.warning("No rules found in YAML")
            return

        # YAML rules correctness checks
        for rule in self.get_rules().values():
            assert self.conditions_valid(rule['conditions']), "Invalid conditions"
            assert self.actions_valid(rule['actions']), "Invalid actions"

    def get_rules(self) -> list:
        if not 'rules' in self.yaml_data:
            return {}
        else:
            return self.yaml_data['rules']

    def process_flight(self, flight: Flight) -> None:
        """Apply rules and actions to the current position of a given flight. """

        rule_items = self.get_rules().items()

        for rule_name, rule_value in rule_items:
            logger.debug("Checking rules %s", rule_name)

            if self.conditions_match(flight, rule_value['conditions'], rule_name):
                logger.debug("MATCH for rule '%s' for flight %s", rule_name, flight.flight_id)

                self.do_actions(flight, rule_value['actions'], rule_name)
            else:
                logger.debug("NOMATCH for rule '%s' for flight %s", rule_name, flight.flight_id)

    def conditions_valid(self, conditions: dict):
        """Check for invalid or unknown conditions, return True if valid."""
        VALID_CONDITIONS = ['proximity', 'aircraft_list', 'exclude_aircraft_list',
                            'exclude_aircraft_substrs',
                            'min_alt', 'max_alt',
                            'transition_regions', 'changed_regions',
                            'regions', 'latlongring',
                            'cooldown', 'rule_cooldown', 'has_attr', 'min_time',
                            'max_time']

        try:
            for condition in conditions.keys():
                if condition not in VALID_CONDITIONS:
                    logger.error("Unknown condition: %s", condition)
                    return False
        except AttributeError:
            logger.error("Specify unconditional execution with '{}' in YAML")
            raise
        return True

    def conditions_match(self, flight: Flight, conditions: dict,
                         rule_name: str) -> bool:
        """Determine if the given rule conditions match for the given 
        flight.  Returns true on match for the specific rule given, false 
        otherwise.
        Note: Put expensive-to-evaluate conditions toward the bottom,
        for best performance."""

        #logger.debug("condition_match checking rules: %s", str(conditions))
        Stats.condition_match_calls += 1

        # TODO the approach below prevents us from having multiple rules of
        # the same type.  Do we need to support that?

        if 'proximity' in conditions:
            return False  # handled asynchronously in handle_proximity_conditions

        if 'aircraft_list' in conditions:
            condition_value = conditions['aircraft_list']
            try:
                ac_list = self.yaml_data['aircraft_lists'][condition_value]
            except KeyError:
                logger.critical("Aircraft list not found: %s", condition_value)
                return False
            result = flight.flight_id in ac_list
            if not result:
                return False

        if 'exclude_aircraft_list' in conditions:
            condition_value = conditions['exclude_aircraft_list']
            try:
                ac_list = self.yaml_data['aircraft_lists'][condition_value]
            except KeyError:
                logger.critical("Aircraft list not found: %s", condition_value)
                return False
            result = flight.flight_id not in ac_list
            if not result:
                return False

        if 'exclude_aircraft_substrs' in conditions:
            condition_value = conditions['exclude_aircraft_substrs']
            for value in condition_value:
                result = value in flight.flight_id
                if result:
                    return False

        if 'min_alt' in conditions:
            condition_value = conditions['min_alt']
            result = flight.lastloc.alt_baro >= int(condition_value)
            if not result:
                return False

        if 'max_alt' in conditions:
            condition_value = conditions['max_alt']
            result = flight.lastloc.alt_baro <= int(condition_value)
            if not result:
                return False

        if 'transition_regions' in conditions:
            # moved from one region to another.  None is a valid region.
            condition_value = conditions['transition_regions']
            result = (flight.was_in_bboxes([condition_value[0]]) and
                       flight.is_in_bboxes([condition_value[1]]))
            if not result:
                return False

        if 'changed_regions' in conditions:
            if condition_value == "strict":
                if not flight.was_in_any_bbox() or not flight.in_any_bbox():
                    return False
            result = flight.prev_inside_bboxes != flight.inside_bboxes
            if not result:
                return False

        if 'regions' in conditions:
            # KML region match
            condition_value = conditions['regions']
            result = flight.is_in_bboxes(condition_value)
            if not result:
                return False

        if 'cooldown' in conditions:
            # reduce firing rate to every n minutes
            cooldown_secs = int(conditions['cooldown'] * 60)  # convert to seconds
            result = not self.rule_execution_log.within_cooldown(rule_name,
                flight.flight_id, cooldown_secs, flight.lastloc.now)
            if not result:
                return False

        if 'rule_cooldown' in conditions:
            # reduce firing rate of this rule, regardless of aircraft
            cooldown_secs = int(conditions['rule_cooldown'] * 60)
            result = not self.rule_execution_log.within_cooldown(rule_name,
                                                                 "", cooldown_secs, 
                                                                 flight.lastloc.now)
            if not result:
                return False


        if 'latlongring' in conditions:
            condition_value = conditions['latlongring']
            dist = flight.lastloc.distfrom(
                condition_value[1], condition_value[2])
            result = condition_value[0] >= dist
            if not result:
                return False

        if 'has_attr' in conditions:
            condition_value = conditions['has_attr']
            if flight.lastloc.flightdict:
                result = condition_value in flight.lastloc.flightdict
            else:
                result = False
            if not result:
                return False

        if 'min_time' in conditions:
            condition_value = conditions['min_time']
            ts_24hr = int(datetime.datetime.utcfromtimestamp(
                flight.lastloc.now).strftime("%H%M"))
            result = ts_24hr >= condition_value
            if not result:
                return False

        if 'max_time' in conditions:
            condition_value = conditions['max_time']
            ts_24hr = int(datetime.datetime.utcfromtimestamp(
                flight.lastloc.now).strftime("%H%M"))
            result = ts_24hr <= condition_value
            if not result:
                return False

        return True

    def actions_valid(self, actions: dict):
        """Check for invalid or unknown actions, return True if valid."""
        VALID_ACTIONS = ['webhook', 'print', 'callback', 'note', 'track',
                         'expire_callback']

        for action in actions.keys():
            if action not in VALID_ACTIONS:
                logger.error("Unknown action: %s", action)
                return False
        return True

    def do_actions(self, flight: Flight, action_items: dict, rule_name: str,
                   cb_arg = None) -> None:
        """Rule matched, now execute the actions for the given flight."""

        self.rule_execution_log.log(rule_name, flight.flight_id,
                                    flight.lastloc.now,
                                    flight.flags.get('note', ''))

        for action_name, action_value in action_items.items():
            if 'webhook' == action_name:
                Stats.webhooks_fired += 1
                try:
                    [action_type, action_recipient] = action_value
                except Exception: # pylint: disable=broad-except
                    logger.error("Invalid webhook action: %s", action_value)
                    continue

                if 'slack' == action_type:
                    text = (f"Rule {rule_name} matched for: {flight.to_str()}\n"
                        f"LIVE LINK: {flight.to_link()}\n"
                        f"RECORDING: {flight.to_recording()}")
                    send_slack(action_recipient, text)
                elif 'page' == action_type:
                    text = (f"Rule {rule_name}: {flight.lastloc.to_short_str()} "
                            f"{str(flight.inside_bboxes)}")
                    send_page(action_recipient, text)
                else:
                    logger.error("Unknown webhook action type: %s", action_type)

            elif 'print' == action_name:
                ts_utc = datetime.datetime.utcfromtimestamp(
                    flight.lastloc.now).strftime('%m/%d/%y %H:%M')
                print(
                    f"Print action: {ts_utc} {flight.to_str()}",
                    f"{flight.flags.get('note', '')}")

            elif 'callback' == action_name:
                Stats.callbacks_fired += 1
                Stats.last_callback_flight = flight
                if not action_value in self.callbacks:
                    timestamp = datetime.datetime.fromtimestamp(
                        flight.lastloc.now).strftime("%m/%d/%y %H:%M")
                    other_flight_id = ""
                    if cb_arg:
                        other_flight_id = cb_arg.flight_id

                    logger.error("No callback defined: %s, %s, %s, %s",
                                 action_value, flight.flight_id, other_flight_id,
                                 timestamp)
                    continue

                logger.debug("Doing callback for %s", flight.flight_id)
                if cb_arg:
                    # this is used for proximity events where you need to
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
                # triggered on flight eviction from the system.
                pass # this is handled upon asynchronous expiration in do_expire()

            elif 'track' == action_name:
                # statistics gathering.
                pass # handled after execution is complete

            else:
                logger.warning("Unmatched action: %s", action_name)

    def do_expire(self, flight: Flight) -> None:
        """Handle flight expiration rules.
        
        The given flight is about to be evicted from the system, 
        see if any actions are needed.  This type of rule will be 
        needed for UI implementations at least.
        TODO: tests needed."""

        for rule_name, rule_value in self.get_rules().items():
            actions = rule_value['actions']

            if ( "expire_callback" in actions and
                self.conditions_match(flight, rule_value['conditions'], rule_name)):
                logger.debug("doing expire callback for %s", flight.flight_id)

                self.callbacks[actions['expire_callback']](flight)

    def get_rules_with_condition(self, condition_type) -> list:
        """Returns a list of name/rule tuples that have a condition of the given type."""

        rules_list = self.get_rules()
        ret = []
        for rule_name, rule_body in rules_list.items():
            if condition_type in rule_body['conditions']:
                ret.append((rule_name, rule_body))
        return ret

    def get_rules_with_action(self, action_type) -> list:
        """Returns a list of name/rule tuples that have an action of the given type."""

        rules_list = self.get_rules()
        ret = []
        for rule_name, rule_body in rules_list.items():
            if action_type in rule_body['actions']:
                ret.append((rule_name, rule_body))
        return ret

    def handle_proximity_conditions(self, flights, last_read_time) -> list:
        """
        This is run periodically to check distance between all aircraft --
        to check for any matching proximity conditions.  
        It's O(n^2), can be expensive, but altitude and bbox limits can help...

        NOTE: currently flights not in any bbox are not checked, to improve
        execution time.
        """

        prox_rules_list = self.get_rules_with_condition("proximity")
        found_prox_events = []

        if prox_rules_list == []:
            return

        for flight1 in flights.flight_dict.values():
            if flights.ignore_unboxed_flights and not flight1.in_any_bbox():
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
                        # Also check if flight2 matches the rule conditions
                        # This ensures excluded aircraft in flight2 won't
                        # trigger the rule.  XXX should exclude rule_cooldowns,
                        # probably -- they will always fire on the 2nd check
                        if self.conditions_match(flight2, rule_conditions, rule_name):
                            logger.debug("Proximity match: %s %s", flight1.flight_id,
                                        flight2.flight_id)
                            self.do_actions(flight1, rule_body['actions'], rule_name,
                                            flight2)
                            found_prox_events.append((flight1, flight2))
        return found_prox_events

    def print_final_report(self):
        """Print a report of rule execution statistics, for any rule
        that contains a "track" action."""

        tracked_rules = self.get_rules_with_action("track")
        for rule_name, _ in tracked_rules:
            log = self.rule_execution_log
            if rule_name in log.rule_execution_counters:
                counter = log.rule_execution_counters[rule_name]
            else:
                counter = ExecutionCounter(rule_name)

            counter.print_report()
