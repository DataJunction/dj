const { DJClient } = require('./index')
var dockerNames = require('docker-names')

test('should return something', async () => {
    const dj = new DJClient('http://localhost:8000', 'integration.tests', 'dj', 'dj');
    await dj.login("dj", "dj")

    await dj.catalogs.create({ name: 'tpch' });
    await dj.engines.create({ name: 'trino', version: '451' });
    await dj.catalogs.addEngine('tpch', 'trino', '451');

    await dj.namespaces.create('integration.tests');
    await dj.namespaces.create('integration.tests.trino');

    const source = await dj.sources.create({
        name: 'integration.tests.source1',
        catalog: 'unknown',
        schema_: 'db',
        table: 'tbl',
        display_name: 'Test Source with Columns',
        description: 'A test source node with columns',
        columns: [
            { name: 'id', type: 'int' },
            { name: 'name', type: 'string' },
            { name: 'price', type: 'double' },
            { name: 'created_at', type: 'timestamp' },
        ],
        primary_key: ['id'],
        mode: 'published',
        update_if_exists: true,
    });

    await dj.register.table('tpch', 'sf1', 'orders');

    const transform = await dj.transforms.create({
        name: 'integration.tests.trino.transform1',
        display_name: 'Filter to last 1000 records',
        description: 'The last 1000 purchases',
        mode: 'published',
        query: 'select custkey, totalprice, orderdate from source.tpch.sf1.orders order by orderdate desc limit 1000',
        update_if_exists: true,
    });

    const dimension = await dj.dimensions.create({
        name: 'integration.tests.trino.dimension1',
        display_name: 'Customer keys',
        description: 'All custkey values in the source table',
        mode: 'published',
        primary_key: ['id'],
        tags: [],
        query: "select custkey as id, 'attribute' as foo from source.tpch.sf1.orders",
        update_if_exists: true,
    });

    await dj.dimensions.link(
        'integration.tests.trino.transform1',
        'custkey',
        'integration.tests.trino.dimension1',
        'id',
    );

    const metric = await dj.metrics.create({
        name: 'integration.tests.trino.metric1',
        display_name: 'Total of last 1000 purchases',
        description: 'This is the total amount from the last 1000 purchases',
        mode: 'published',
        query: 'select sum(totalprice) from integration.tests.trino.transform1',
        update_if_exists: true,
    });

    await dj.commonDimensions.list(['integration.tests.trino.metric1']);

    const query = await dj.sql.get(
        ['integration.tests.trino.metric1'],
        ['integration.tests.trino.dimension1.id'],
        []
    );

    expect(query.sql).toContain('SELECT');
}, 60000)
