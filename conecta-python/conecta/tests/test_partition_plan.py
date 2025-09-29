from conecta import create_partition_plan, PartitionPlan


def test_partition_plan(pg_conn: tuple):
    partition = create_partition_plan(
        pg_conn,
        [f'select * from lineitem_small'],
        'l_orderkey',
        partition_num=2,
    )

    assert isinstance(partition, PartitionPlan)
    assert partition.max_value == 1_197_255
    assert partition.min_value == 1108353
    assert len(partition.data_queries) == 2
    assert partition.partition_config.preallocation == True

def test_partition_plan_preallocation(pg_conn: str):
    partition = create_partition_plan(
        pg_conn,
        [f'select * from lineitem_small'],
        'l_orderkey',
        partition_num=2,

        preallocation=False
    )

    assert isinstance(partition, PartitionPlan)
    assert partition.max_value == 1_197_255
    assert partition.min_value == 1_108_353
    assert len(partition.data_queries) == 2
    assert partition.partition_config.preallocation == False
