const { DJClient } = require('./index')
var dockerNames = require('docker-names')

test('should return something', async () => {
    const randomName = dockerNames.getRandomName(true)
    const namespace = `integration.javascript.${randomName}`
    const dj = new DJClient('http://localhost:8000')

    // Create a namespace
    const namespace_created = await dj.namespaces.create(namespace)
    expect(namespace_created).toEqual({
        message: `Node namespace \`${namespace}\` has been successfully created`,
    })

    // List namespaces
    const existing_namespaces = await dj.namespaces.list()
    expect(existing_namespaces).toContainEqual({ namespace: namespace })

    // Create a source
    await dj.sources.create({
        name: `${namespace}.repair_orders`,
        description: 'Repair orders',
        catalog: 'warehouse',
        schema_: 'roads',
        table: 'repair_orders',
        mode: 'published',
        columns: [
            { name: 'repair_order_id', type: 'int' },
            { name: 'municipality_id', type: 'string' },
            { name: 'hard_hat_id', type: 'int' },
            { name: 'order_date', type: 'timestamp' },
            { name: 'required_date', type: 'timestamp' },
            { name: 'dispatched_date', type: 'timestamp' },
            { name: 'dispatcher_id', type: 'int' },
        ],
    })

    // Get source
    const source1 = await dj.nodes.get(`${namespace}.repair_orders`)
    expect(source1.name).toBe(`${namespace}.repair_orders`)
    expect(source1.status).toBe('valid')
    expect(source1.mode).toBe('published')

    // Create a transform
    await dj.transforms.create({
        name: `${namespace}.repair_orders_w_dispatchers`,
        description: 'Repair orders that have a dispatcher',
        mode: 'published',
        query: `SELECT
            repair_order_id,
            municipality_id,
            hard_hat_id,
            dispatcher_id
            FROM ${namespace}.repair_orders
            WHERE dispatcher_id IS NOT NULL`,
    })

    // Get transform
    const transform1 = await dj.nodes.get(
        `${namespace}.repair_orders_w_dispatchers`
    )
    expect(transform1.name).toBe(`${namespace}.repair_orders_w_dispatchers`)
    expect(transform1.status).toBe('valid')
    expect(transform1.mode).toBe('published')

    // Create a source and dimension node
    const source2 = await dj.sources.create({
        name: `${namespace}.dispatchers`,
        description:
            'Different third party dispatcher companies that coordinate repairs',
        catalog: 'warehouse',
        schema_: 'roads',
        table: 'dispatchers',
        mode: 'published',
        columns: [
            { name: 'dispatcher_id', type: 'int' },
            { name: 'company_name', type: 'string' },
            { name: 'phone', type: 'string' },
        ],
    })
    expect(source2.name).toBe(`${namespace}.dispatchers`)
    expect(source2.status).toBe('valid')
    expect(source2.mode).toBe('published')
    await dj.dimensions.create({
        name: `${namespace}.all_dispatchers`,
        description: 'All dispatchers',
        primary_key: ['dispatcher_id'],
        mode: 'published',
        query: `SELECT
        dispatcher_id,
        company_name,
        phone
        FROM ${namespace}.dispatchers`,
    })
    const dimension1 = await dj.nodes.get(`${namespace}.all_dispatchers`)
    expect(dimension1.name).toBe(`${namespace}.all_dispatchers`)
    expect(dimension1.status).toBe('valid')
    expect(dimension1.mode).toBe('published')

    // Create metrics
    await dj.metrics.create({
        name: `${namespace}.num_repair_orders`,
        description: 'Number of repair orders',
        mode: 'published',
        query: `SELECT
        count(repair_order_id)
        FROM ${namespace}.repair_orders`,
    })
    const metric1 = await dj.nodes.get(`${namespace}.num_repair_orders`)
    expect(metric1.name).toBe(`${namespace}.num_repair_orders`)
    expect(metric1.status).toBe('valid')
    expect(metric1.mode).toBe('published')

    // List metrics
    const list_of_metrics = await dj.metrics.all()
    expect(list_of_metrics).toContain(`${namespace}.num_repair_orders`)

    // Create a dimension link
    const dimension_linked_message = await dj.dimensions.link(
        `${namespace}.repair_orders`,
        'dispatcher_id',
        `${namespace}.all_dispatchers`,
        'dispatcher_id'
    )
    expect(dimension_linked_message).toEqual({
        message: `Dimension node ${namespace}.all_dispatchers has been successfully linked to column dispatcher_id on node ${namespace}.repair_orders`,
    })

    const common_dimensions = await dj.commonDimensions.list([
        'default.num_repair_orders',
        'default.avg_repair_price',
        'default.total_repair_cost',
    ])

    expect(common_dimensions).toEqual([
        { name: 'default.repair_orders.dispatcher_id', type: 'int', path: [] },
        { name: 'default.repair_orders.hard_hat_id', type: 'int', path: [] },
        {
            name: 'default.repair_orders.municipality_id',
            type: 'string',
            path: [],
        },
        {
            name: 'default.us_state.state_abbr',
            type: 'string',
            path: [
                'default.repair_orders.hard_hat_id',
                'default.hard_hat.state',
            ],
        },
        {
            name: 'default.us_state.state_id',
            type: 'int',
            path: [
                'default.repair_orders.hard_hat_id',
                'default.hard_hat.state',
            ],
        },
        {
            name: 'default.us_state.state_name',
            type: 'string',
            path: [
                'default.repair_orders.hard_hat_id',
                'default.hard_hat.state',
            ],
        },
        {
            name: 'default.us_state.state_region',
            type: 'int',
            path: [
                'default.repair_orders.hard_hat_id',
                'default.hard_hat.state',
            ],
        },
        {
            name: 'default.us_state.state_region_description',
            type: 'string',
            path: [
                'default.repair_orders.hard_hat_id',
                'default.hard_hat.state',
            ],
        },
    ])

    const query = await dj.sql.get(
        [
            'default.num_repair_orders',
            'default.avg_repair_price',
            'default.total_repair_cost',
        ],
        [
            'default.us_state.state_abbr',
            'default.us_state.state_id',
            'default.us_state.state_name',
        ],
        []
    )
    const trimmedQuery = query.sql.replace(/\s+/g, '')
    expect(trimmedQuery).toBe(
        `    WITH
        m0_default_DOT_num_repair_orders AS (SELECT  default_DOT_us_state.state_abbr,
                default_DOT_us_state.state_id,
                default_DOT_us_state.state_name,
                count(default_DOT_repair_orders.repair_order_id) default_DOT_num_repair_orders
        FROM roads.repair_orders AS default_DOT_repair_orders LEFT OUTER JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
                default_DOT_hard_hats.state
        FROM roads.hard_hats AS default_DOT_hard_hats)
        AS default_DOT_hard_hat ON default_DOT_repair_orders.hard_hat_id = default_DOT_hard_hat.hard_hat_id
        LEFT OUTER JOIN (SELECT  default_DOT_us_states.state_abbr,
                default_DOT_us_states.state_id,
                default_DOT_us_states.state_name
        FROM roads.us_states AS default_DOT_us_states LEFT  JOIN roads.us_region AS default_DOT_us_region ON default_DOT_us_states.state_region = default_DOT_us_region.us_region_description)
        AS default_DOT_us_state ON default_DOT_hard_hat.state = default_DOT_us_state.state_abbr
        GROUP BY  default_DOT_us_state.state_abbr, default_DOT_us_state.state_id, default_DOT_us_state.state_name
        ),
        m1_default_DOT_avg_repair_price AS (SELECT  default_DOT_us_state.state_abbr,
                default_DOT_us_state.state_id,
                default_DOT_us_state.state_name,
                avg(default_DOT_repair_order_details.price) default_DOT_avg_repair_price
        FROM roads.repair_order_details AS default_DOT_repair_order_details LEFT OUTER JOIN (SELECT  default_DOT_repair_orders.dispatcher_id,
                default_DOT_repair_orders.hard_hat_id,
                default_DOT_repair_orders.municipality_id,
                default_DOT_repair_orders.repair_order_id
        FROM roads.repair_orders AS default_DOT_repair_orders)
        AS default_DOT_repair_order ON default_DOT_repair_order_details.repair_order_id = default_DOT_repair_order.repair_order_id
        LEFT OUTER JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
                default_DOT_hard_hats.state
        FROM roads.hard_hats AS default_DOT_hard_hats)
        AS default_DOT_hard_hat ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
        LEFT OUTER JOIN (SELECT  default_DOT_us_states.state_abbr,
                default_DOT_us_states.state_id,
                default_DOT_us_states.state_name
        FROM roads.us_states AS default_DOT_us_states LEFT  JOIN roads.us_region AS default_DOT_us_region ON default_DOT_us_states.state_region = default_DOT_us_region.us_region_description)
        AS default_DOT_us_state ON default_DOT_hard_hat.state = default_DOT_us_state.state_abbr
        GROUP BY  default_DOT_us_state.state_abbr, default_DOT_us_state.state_id, default_DOT_us_state.state_name
        ),
        m2_default_DOT_total_repair_cost AS (SELECT  default_DOT_us_state.state_abbr,
                default_DOT_us_state.state_id,
                default_DOT_us_state.state_name,
                sum(default_DOT_repair_order_details.price) default_DOT_total_repair_cost
        FROM roads.repair_order_details AS default_DOT_repair_order_details LEFT OUTER JOIN (SELECT  default_DOT_repair_orders.dispatcher_id,
                default_DOT_repair_orders.hard_hat_id,
                default_DOT_repair_orders.municipality_id,
                default_DOT_repair_orders.repair_order_id
        FROM roads.repair_orders AS default_DOT_repair_orders)
        AS default_DOT_repair_order ON default_DOT_repair_order_details.repair_order_id = default_DOT_repair_order.repair_order_id
        LEFT OUTER JOIN (SELECT  default_DOT_hard_hats.hard_hat_id,
                default_DOT_hard_hats.state
        FROM roads.hard_hats AS default_DOT_hard_hats)
        AS default_DOT_hard_hat ON default_DOT_repair_order.hard_hat_id = default_DOT_hard_hat.hard_hat_id
        LEFT OUTER JOIN (SELECT  default_DOT_us_states.state_abbr,
                default_DOT_us_states.state_id,
                default_DOT_us_states.state_name
        FROM roads.us_states AS default_DOT_us_states LEFT  JOIN roads.us_region AS default_DOT_us_region ON default_DOT_us_states.state_region = default_DOT_us_region.us_region_description)
        AS default_DOT_us_state ON default_DOT_hard_hat.state = default_DOT_us_state.state_abbr
        GROUP BY  default_DOT_us_state.state_abbr, default_DOT_us_state.state_id, default_DOT_us_state.state_name
        )SELECT  m0_default_DOT_num_repair_orders.default_DOT_num_repair_orders,
                m1_default_DOT_avg_repair_price.default_DOT_avg_repair_price,
                m2_default_DOT_total_repair_cost.default_DOT_total_repair_cost,
                COALESCE(m0_default_DOT_num_repair_orders.state_abbr, m1_default_DOT_avg_repair_price.state_abbr, m2_default_DOT_total_repair_cost.state_abbr) state_abbr,
                COALESCE(m0_default_DOT_num_repair_orders.state_id, m1_default_DOT_avg_repair_price.state_id, m2_default_DOT_total_repair_cost.state_id) state_id,
                COALESCE(m0_default_DOT_num_repair_orders.state_name, m1_default_DOT_avg_repair_price.state_name, m2_default_DOT_total_repair_cost.state_name) state_name
        FROM m0_default_DOT_num_repair_orders FULL OUTER JOIN m1_default_DOT_avg_repair_price ON m0_default_DOT_num_repair_orders.state_abbr = m1_default_DOT_avg_repair_price.state_abbr AND m0_default_DOT_num_repair_orders.state_id = m1_default_DOT_avg_repair_price.state_id AND m0_default_DOT_num_repair_orders.state_name = m1_default_DOT_avg_repair_price.state_name
        FULL OUTER JOIN m2_default_DOT_total_repair_cost ON m0_default_DOT_num_repair_orders.state_abbr = m2_default_DOT_total_repair_cost.state_abbr AND m0_default_DOT_num_repair_orders.state_id = m2_default_DOT_total_repair_cost.state_id AND m0_default_DOT_num_repair_orders.state_name = m2_default_DOT_total_repair_cost.state_name
    `.replace(/\s+/g, '')
    )

    const data = await dj.data.get(
        [
            'default.num_repair_orders',
            'default.avg_repair_price',
            'default.total_repair_cost',
        ],
        [
            'default.us_state.state_abbr',
            'default.us_state.state_id',
            'default.us_state.state_name',
        ],
        []
    )

    expect(data).toEqual({
        columns: [
            {
                name: 'default_DOT_num_repair_orders',
                type: 'bigint',
            },
            {
                name: 'default_DOT_avg_repair_price',
                type: 'double',
            },
            {
                name: 'default_DOT_total_repair_cost',
                type: 'double',
            },
            {
                name: 'state_abbr',
                type: 'string',
            },
            {
                name: 'state_id',
                type: 'int',
            },
            {
                name: 'state_name',
                type: 'string',
            },
        ],
        data: [
            [162, 65682.0, 31921452.0, 'AZ', 3, 'Arizona'],
            [162, 39301.5, 19100529.0, 'CT', 7, 'Connecticut'],
            [243, 65595.66666666667, 47819241.0, 'GA', 11, 'Georgia'],
            [243, 76555.33333333333, 55808838.0, 'MA', 22, 'Massachusetts'],
            [405, 64190.6, 77991579.0, 'MI', 23, 'Michigan'],
            [324, 54672.75, 53141913.0, 'NJ', 31, 'New Jersey'],
            [81, 53374.0, 12969882.0, 'NY', 33, 'New York'],
            [81, 70418.0, 17111574.0, 'OK', 37, 'Oklahoma'],
            [324, 54083.5, 52569162.0, 'PA', 39, 'Pennsylvania'],
        ],
    })

    // Create a transform 2 downstream from a transform 1
    await dj.transforms.create({
        name: `${namespace}.repair_orders_w_hard_hats`,
        description: 'Repair orders that have a hard hat assigned',
        mode: 'published',
        query: `SELECT
            repair_order_id,
            municipality_id,
            hard_hat_id,
            dispatcher_id
            FROM ${namespace}.repair_orders_w_dispatchers
            WHERE hard_hat_id IS NOT NULL`,
    })

    // Get transform 2 that's downstream from transform 1 and make sure it's valid
    const transform2 = await dj.nodes.get(
        `${namespace}.repair_orders_w_hard_hats`
    )
    expect(transform2.name).toBe(`${namespace}.repair_orders_w_hard_hats`)
    expect(transform2.status).toBe('valid')
    expect(transform2.mode).toBe('published')

    // Create a draft transform 4 that's downstream from a not yet created transform 3
    await dj.transforms.create({
        name: `${namespace}.repair_orders_w_repair_order_id`,
        description: 'Repair orders without a null ID',
        mode: 'draft',
        query: `SELECT
            repair_order_id,
            municipality_id,
            hard_hat_id,
            dispatcher_id
            FROM ${namespace}.repair_orders_w_municipalities
            WHERE repair_order_id IS NOT NULL`,
    })
    const transform4 = await dj.nodes.get(
        `${namespace}.repair_orders_w_repair_order_id`
    )
    // Check that transform 4 is invalid because transform 3 does not exist
    expect(transform4.name).toBe(`${namespace}.repair_orders_w_repair_order_id`)
    expect(transform4.status).toBe('invalid')
    expect(transform4.mode).toBe('draft')

    // Create a draft transform 3 that's downstream from transform 2
    await dj.transforms.create({
        name: `${namespace}.repair_orders_w_municipalities`,
        description: 'Repair orders that have a municipality listed',
        mode: 'draft',
        query: `SELECT
        repair_order_id,
        municipality_id,
        hard_hat_id,
        dispatcher_id
        FROM ${namespace}.repair_orders_w_hard_hats
        WHERE municipality_id IS NOT NULL`,
    })
    const transform3 = await dj.nodes.get(
        `${namespace}.repair_orders_w_municipalities`
    )
    expect(transform3.name).toBe(`${namespace}.repair_orders_w_municipalities`)
    expect(transform3.status).toBe('valid')
    expect(transform3.mode).toBe('draft')

    // Check that transform 4 is now valid after transform 3 was created
    const transform4_now_valid = await dj.nodes.get(
        `${namespace}.repair_orders_w_repair_order_id`
    )
    expect(transform4_now_valid.status).toBe('valid')

    // Check that publishing transform 3 works
    await dj.nodes.publish(transform3.name)
}, 60000)
