from eventz.aggregate import Aggregate

from eventz_aws.subscription_registry_dummy import SubscriptionRegistryDummy


def test_subscriptions_can_be_registered_and_fetched(
    dynamodb_connection_with_empty_subscriptions_table,
):
    registry = SubscriptionRegistryDummy()
    # run test and make assertion
    game_id_1 = Aggregate.make_id()
    game_id_2 = Aggregate.make_id()
    subscription_1 = "subscription1"
    subscription_2 = "subscription2"
    subscription_3 = "subscription3"
    subscription_4 = "subscription4"
    assert registry.fetch(game_id_1) == tuple()
    assert registry.fetch(game_id_2) == tuple()
    registry.register(game_id_1, subscription_1)
    assert registry.fetch(game_id_1) == (subscription_1,)
    assert registry.fetch(game_id_2) == tuple()
    registry.register(game_id_1, subscription_2)
    assert registry.fetch(game_id_1) == (subscription_1, subscription_2,)
    assert registry.fetch(game_id_2) == tuple()
    registry.register(game_id_2, subscription_3)
    assert registry.fetch(game_id_1) == (subscription_1, subscription_2, )
    assert registry.fetch(game_id_2) == (subscription_3,)
    registry.register(game_id_2, subscription_4)
    assert registry.fetch(game_id_1) == (subscription_1, subscription_2, )
    assert registry.fetch(game_id_2) == (subscription_3, subscription_4, )
    registry.register(game_id_2, subscription_1)
    assert registry.fetch(game_id_1) == (subscription_1, subscription_2, )
    assert registry.fetch(game_id_2) == (subscription_3, subscription_4, subscription_1, )
    registry.register(game_id_1, subscription_1)
    registry.register(game_id_1, subscription_2)
    assert registry.fetch(game_id_1) == (subscription_1, subscription_2, )
    assert registry.fetch(game_id_2) == (subscription_3, subscription_4, subscription_1, )
