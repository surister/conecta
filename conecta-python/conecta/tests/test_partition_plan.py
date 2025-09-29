import pprint

from conecta import create_partition_plan, PartitionPlan


def test_partition_plan():
    partition = create_partition_plan(
        'postgres://postgres:postgres@192.168.88.251:5400/postgres',
        ['select * from lineitem'],
        'l_orderkey',
        partition_num=2,
    )

    assert isinstance(partition, PartitionPlan)
    assert partition.max_value == 1_200_000
    assert partition.min_value == 1
    assert len(partition.data_queries) == 2
    assert partition.partition_config.preallocation == True

def test_partition_plan_preallocation():
    partition = create_partition_plan(
        'postgres://postgres:postgres@192.168.88.251:5400/postgres',
        ['select * from lineitem'],
        'l_orderkey',
        partition_num=2,

        preallocation=False
    )

    assert isinstance(partition, PartitionPlan)
    assert partition.max_value == 1_200_000
    assert partition.min_value == 1
    assert len(partition.data_queries) == 2
    assert partition.partition_config.preallocation == False
