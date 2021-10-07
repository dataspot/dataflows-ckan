import logging
import os

log = logging.getLogger(__name__)


def test_dump_to_ckan():
    import dataflows as DF

    from dataflows_ckan import dump_to_ckan

    data = [
        dict(x=1, y='a'),
        dict(x=2, y='b'),
        dict(x=3, y='c'),
    ]

    host, api_key, owner_org = (
        os.environ['CKAN_TEST_HOST'],
        os.environ['CKAN_TEST_API_KEY'],
        os.environ['CKAN_TEST_OWNER_ORG'],
    )

    flow = DF.Flow(
        data,
        DF.update_package(name='ckan_test'),
        DF.printer(),
        dump_to_ckan(
            host,
            api_key,
            owner_org,
        ),
    )

    assert flow.process()
