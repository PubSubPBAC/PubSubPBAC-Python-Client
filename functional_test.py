#!python3

import logging, coloredlogs
import time
from purpose_client import PurposeClient
import sys
import paho.mqtt.client as mqtt

import datetime

logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG")

server = sys.argv[1] if (len(sys.argv) == 2) else "localhost"
print("server: {}".format(server))

paho_client = mqtt.Client(client_id="purpose_paho_func", clean_session=True)
client = PurposeClient(paho_client)
client.connect(server, 1883, 60)
logging.debug("connected")


# client.connect("localhost", 1883, 60)

def tick():
    time.sleep(0.1)


def run(partial: bool = True):
    client.reset_broker()
    time.sleep(1)
    client.reset_connection()

    if all:

        # ensure correct working of reset function -- existing subscriptions should not exist anymore
        client.send_and_reject("test/reserved_before", "make sure no old subscriptions persist")

        tick()

        # make reservations for basic tests
        # verify that
        #   - reservations with AIP are processed
        client.reserve("test/reserved_before", ["research", "marketing"])
        client.reserve("test/nested", ["research"])
        client.reserve("test/no", ["covid"])
        client.reserve("test/wild/1/HASH", ["wild1", "testing"])
        client.reserve("test/wild/2/HASH", ["wild2", "testing"])
        client.reserve("test/wild/3/HASH", ["wild3", "testing"])
        client.reserve("test/wild/HASH", ["wildall", "testing"])

        # make subscriptions for basic tests
        # verify that
        #   - subscriptions with AP are processed
        client.subscribe("test/unreserved")
        client.subscribe_with_purpose("test/reserved_before", "research")
        client.subscribe_with_purpose("test/reserved_later", "research")
        client.subscribe_with_purpose("test/no", "identitytheft")
        client.subscribe_with_purpose("test/nested", "research/covid")
        client.subscribe_with_purpose("test/wild/ok", "testing")
        client.subscribe_with_purpose("test/wild/forbidden", "research")

        # test presubscriptions
        client.reserve("test/presub", ["testing"])
        client.subscribe_with_purpose("test/presub", "testing/compatibilty", presub=True)
        client.wait_for_subscriptions()
        client.send_and_expect("test/presub", b"message to presubscribed topic")

        # make reservation after subscribing, it has been unreserved before
        client.reserve("test/reserved_later", ["research", "dancingtutorials"])
        client.wait_for_subscriptions()

        # unreserved should work
        client.send_and_expect("test/unreserved", b"message to unreserved topic")

        tick()

        # forbid unreserved
        client.unsubscribe("test/unreserved")
        client.set_purpose_setting("allow_without_reservation", False)
        tick()

        client.subscribe("test/unreserved")
        client.wait_for_subscriptions()

        client.send_and_reject("test/unreserved", b"message to unreserved topic in strict mode")
        tick()
        client.set_purpose_setting("allow_without_reservation", True)


        #
        client.send_and_expect("test/reserved_before", b"message to reserved before with allowed AP")
        # client.send_and_expect("test/reserved_later", b"message to reserved later with allowed AP")
        client.send_and_expect("test/nested", b"message for nested subscription")
        client.send_and_reject("test/no", b"message to reserved no with forbidden AP")
        client.send_and_reject("test/wild/forbidden", b"message to channel forbidden by wildcard")
        client.send_and_expect("test/wild/ok", b"message to channel allowed by wildcard")
        tick()

        # PERSIST data to file, then clear, reservations are now removed:
        client.persist()  # it's blocking on server side and we're waiting for ack
        client.clear()
        tick()

        # subscriptions should be gone
        client.send_and_reject("test/wild/ok", b"message after deleting all subscriptions")

        # resubscription to forbidden topic should now work because reservation is gone
        client.subscribe_with_purpose("test/no", "identitytheft")
        client.wait_for_subscriptions()
        client.send_and_expect("test/no", b"message to channel forbidden by wildcard after clear")


        # reload reservations and subscriptionAPs (NOT SUBSCRIPTIONS!) from file
        tick()
        client.load()
        tick()
        # since reservations are recreated, subscriptions should be paused or messages filtered on P.
        client.send_and_reject("test/no", b"message to channel forbidden by wildcard after reload")

        tick()

        client.reserve("test/reserved_later", ["dancingtutorials"])  # remove the AP we used
        client.subscribe_with_purpose("test/wild/HASH", "wild1")
        client.wait_for_subscriptions()

        client.send_and_reject("test/wild/3434", b"message to wildcard that contains forbidden", ["wild1"])
        client.send_and_reject("test/reserved_later", b"message to reserved later with now forbidden AP")

        client.reserve("test/reserved_later", ["dancingtutorials"])
        #
        client.send_and_reject("test/reserved_later", b"message for newly forbidden channel")
        # client.send_and_expect("test/reserved_later", "message with explicit allowed AIP", ["research"])

        client.unsubscribe("test/reserved_later")
        client.send_and_reject("test/reserved_later", b"message for unsubscribed channel")

        tick()

        client.reserve("rr/1/#", ["asdf"])
        client.subscribe_with_purpose("rr/1/bla", "asdf")
        client.subscribe_with_purpose("rr/#", "asdf")
        client.wait_for_subscriptions()

        client.send_and_expect("rr/1/bla", b"message to unpaused sub")
        client.send_and_expect("rr/123", b"message by wildcard")
        client.send_and_expect("rr/2", b"message to unpaused sub (affected)")

        tick()

        client.reserve("rr/1/#", ["gggg"])

        # this is also affected by the rr/# subsciption which is only affected
        # but still valid in itself
        client.send_and_reject("rr/1/bla", b"message to paused sub")

        # this gets forbidden in FoS since the subscription is AFFECTED
        # client.send_and_reject("rr/2", b"message to paused sub wild")

        tick()

        client.reserve("rr/1/#", ["asdf"])
        tick()
        client.send_and_expect("rr/1/bla", b"message to re-allowed sub")
        client.send_and_expect("rr/2", b"message to re-allowed sub wild")


        client.reserve("tree/#", ["fostest"])
        client.subscribe_with_purpose("tree/#", "fostest")
        client.send_and_expect("tree/17", b"message to sub-topic of subscribed")

        tick()

        # test aip and pip in combination
        client.reserve("pa/#", ["research", "treatment"])
        client.reserve("pa/pip/#", [], pip=["treatment"])
        client.reserve("pa/pop/#", ["research"], pip=["treatment"])
        client.subscribe_with_purpose("pa/pop/a", "research/covid")
        client.subscribe_with_purpose("pa/pip/f", "treatment/covid")
        client.subscribe_with_purpose("pa/pop/f", "treatment/covid")

        client.wait_for_subscriptions()

        client.send_and_expect("pa/pop/a", b"allowed combined aip pip")
        client.send_and_reject("pa/pip/f", b"forbidden combined aip pip")
        client.send_and_reject("pa/pop/f", b"forbidden combined aip pip 2")

        client.reserve("plus/+/test", ["testing"])
        client.reserve("plus3/+/test", ["testing"])
        client.reserve("plus2/test/+", ["testing"])
        client.subscribe_with_purpose("plus/yo/+", "testing")
        client.subscribe_with_purpose("plus3/yo/test", "testing")
        client.subscribe_with_purpose("plus2/+/fb", "hacking")
        client.wait_for_subscriptions()

        client.send_and_expect("plus3/yo/test", b"normal subscription on plus reservation")
        client.send_and_expect("plus/yo/test", b"plus subscription on plus reservation")
        client.send_and_reject("plus2/test/fb", b"forbidden plus subscription")


        # test partially allowed subscriptions
        client.reserve("pa/#", ["treatment", "research"])
        client.reserve("pa/2", aip=["treatment"], pip=["research"])
        client.subscribe_with_purpose("pa/#", "research")

        client.wait_for_subscriptions()

        if partial:
            client.send_and_expect("pa/1", b"message to partially forbidden topic")
        else:
            client.send_and_reject("pa/1", b"message to partially forbidden topic")

        # combine aip and pip
        client.reserve("com/#", ["treatment", "billing/cash"], pip=["marketing/apps"])
        client.reserve("com/1/#", ["research", "billing"], pip=["marketing"])
        client.subscribe_with_purpose("com/1/heartrate", "marketing/pills")
        client.subscribe_with_purpose("com/1/spo2", "treatment/covid")
        client.wait_for_subscriptions()
        client.send_and_reject("com/1/heartrate", b"forbidden combined")
        client.send_and_expect("com/1/spo2", b"combined allowed")

        tick()

    client.send("SETTING/TREE", "")
    return client.show_stats()


client.loop_start()
logger.debug("started loop")


def run_modes(tree=False, cache=True):
    problems_per_run = 0

    client.set_purpose_setting("use_tree_store", tree)
    client.set_purpose_setting("cache_reservations", cache)
    client.set_purpose_setting("cache_subscriptions", cache)
    mode_string = "TREE" if tree else "FLAT"
    mode_string += " w/ CACHE" if cache else ""

    print("\n FILTER ON SUBSCRIBE " + mode_string)
    client.set_purpose_setting("filter_on_subscribe", True)
    client.set_purpose_setting("filter_on_publish", False)
    client.set_purpose_setting("filter_hybrid", False)
    client.set_purpose_setting("hybrid_tag", False)
    client.send("test/meta", "starting FILTER ON SUBSCRIBE " + mode_string)
    problems_per_run += run(partial=False)

    print("\n FILTER ON PUBLISH " + mode_string)
    client.set_purpose_setting("filter_on_subscribe", False)
    client.set_purpose_setting("filter_on_publish", True)
    client.set_purpose_setting("filter_hybrid", False)
    client.set_purpose_setting("hybrid_tag", False)
    client.send("test/meta", "starting FILTER ON PUBLISH " + mode_string)
    problems_per_run += run()

    print("\n FILTER HYBRID " + mode_string)
    client.set_purpose_setting("filter_on_subscribe", False)
    client.set_purpose_setting("filter_on_publish", False)
    client.set_purpose_setting("filter_hybrid", True)
    client.set_purpose_setting("hybrid_tag", False)
    client.send("test/meta", "starting HYBRID " + mode_string)
    problems_per_run += run()

    return problems_per_run


# run all tests or just newest
all = True

total_problems = 0

client.set_purpose_setting("cache_reservations", True)

for tree in [True, False]:
    for cache in [False, True]:
        total_problems += run_modes(tree=tree, cache=cache)

client.loop_stop()

print("total problems: {}".format(total_problems))
