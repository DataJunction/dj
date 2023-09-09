export const mocks = {
  mockMetricNode: {
    namespace: 'default',
    node_revision_id: 23,
    node_id: 23,
    type: 'metric',
    name: 'default.num_repair_orders',
    display_name: 'Default: Num Repair Orders',
    version: 'v1.0',
    status: 'valid',
    mode: 'published',
    catalog: {
      id: 1,
      uuid: '0fc18295-e1a2-4c3c-b72a-894725c12488',
      created_at: '2023-08-21T16:48:51.146121+00:00',
      updated_at: '2023-08-21T16:48:51.146122+00:00',
      extra_params: {},
      name: 'warehouse',
    },
    schema_: null,
    table: null,
    description: 'Number of repair orders',
    query:
      'SELECT count(repair_order_id) default_DOT_num_repair_orders FROM default.repair_orders',
    availability: null,
    columns: [
      {
        name: 'default_DOT_num_repair_orders',
        type: 'bigint',
        attributes: [],
        dimension: null,
      },
    ],
    primary_key: ['repair_order_id', 'country'],
    updated_at: '2023-08-21T16:48:56.841704+00:00',
    materializations: [],
    parents: [
      {
        name: 'default.repair_orders',
      },
    ],
    created_at: '2023-08-21T16:48:56.841631+00:00',
    tags: [],
    dimensions: [
      {
        value: 'default.date_dim.dateint',
        label: 'default.date_dim.dateint (timestamp)',
      },
      {
        value: 'default.date_dim.dateint',
        label: 'default.date_dim.dateint (timestamp)',
      },
      {
        value: 'default.date_dim.day',
        label: 'default.date_dim.day (int)',
      },
      {
        value: 'default.date_dim.day',
        label: 'default.date_dim.day (int)',
      },
      {
        value: 'default.date_dim.month',
        label: 'default.date_dim.month (int)',
      },
      {
        value: 'default.date_dim.month',
        label: 'default.date_dim.month (int)',
      },
      {
        value: 'default.date_dim.year',
        label: 'default.date_dim.year (int)',
      },
      {
        value: 'default.date_dim.year',
        label: 'default.date_dim.year (int)',
      },
      {
        value: 'default.dispatcher.company_name',
        label: 'default.dispatcher.company_name (string)',
      },
      {
        value: 'default.dispatcher.dispatcher_id',
        label: 'default.dispatcher.dispatcher_id (int)',
      },
      {
        value: 'default.dispatcher.phone',
        label: 'default.dispatcher.phone (string)',
      },
      {
        value: 'default.hard_hat.address',
        label: 'default.hard_hat.address (string)',
      },
      {
        value: 'default.hard_hat.birth_date',
        label: 'default.hard_hat.birth_date (date)',
      },
      {
        value: 'default.hard_hat.city',
        label: 'default.hard_hat.city (string)',
      },
      {
        value: 'default.hard_hat.contractor_id',
        label: 'default.hard_hat.contractor_id (int)',
      },
      {
        value: 'default.hard_hat.country',
        label: 'default.hard_hat.country (string)',
      },
      {
        value: 'default.hard_hat.first_name',
        label: 'default.hard_hat.first_name (string)',
      },
      {
        value: 'default.hard_hat.hard_hat_id',
        label: 'default.hard_hat.hard_hat_id (int)',
      },
      {
        value: 'default.hard_hat.hire_date',
        label: 'default.hard_hat.hire_date (date)',
      },
      {
        value: 'default.hard_hat.last_name',
        label: 'default.hard_hat.last_name (string)',
      },
      {
        value: 'default.hard_hat.manager',
        label: 'default.hard_hat.manager (int)',
      },
      {
        value: 'default.hard_hat.postal_code',
        label: 'default.hard_hat.postal_code (string)',
      },
      {
        value: 'default.hard_hat.state',
        label: 'default.hard_hat.state (string)',
      },
      {
        value: 'default.hard_hat.title',
        label: 'default.hard_hat.title (string)',
      },
      {
        value: 'default.municipality_dim.contact_name',
        label: 'default.municipality_dim.contact_name (string)',
      },
      {
        value: 'default.municipality_dim.contact_title',
        label: 'default.municipality_dim.contact_title (string)',
      },
      {
        value: 'default.municipality_dim.local_region',
        label: 'default.municipality_dim.local_region (string)',
      },
      {
        value: 'default.municipality_dim.municipality_id',
        label: 'default.municipality_dim.municipality_id (string)',
      },
      {
        value: 'default.municipality_dim.municipality_type_desc',
        label: 'default.municipality_dim.municipality_type_desc (string)',
      },
      {
        value: 'default.municipality_dim.municipality_type_id',
        label: 'default.municipality_dim.municipality_type_id (string)',
      },
      {
        value: 'default.municipality_dim.state_id',
        label: 'default.municipality_dim.state_id (int)',
      },
      {
        value: 'default.repair_order.dispatcher_id',
        label: 'default.repair_order.dispatcher_id (int)',
      },
      {
        value: 'default.repair_order.hard_hat_id',
        label: 'default.repair_order.hard_hat_id (int)',
      },
      {
        value: 'default.repair_order.municipality_id',
        label: 'default.repair_order.municipality_id (string)',
      },
      {
        value: 'default.repair_order.repair_order_id',
        label: 'default.repair_order.repair_order_id (int)',
      },
      {
        value: 'default.repair_order_details.repair_order_id',
        label: 'default.repair_order_details.repair_order_id (int)',
      },
      {
        value: 'default.us_state.state_abbr',
        label: 'default.us_state.state_abbr (string)',
      },
      {
        value: 'default.us_state.state_id',
        label: 'default.us_state.state_id (int)',
      },
      {
        value: 'default.us_state.state_name',
        label: 'default.us_state.state_name (string)',
      },
      {
        value: 'default.us_state.state_region',
        label: 'default.us_state.state_region (int)',
      },
      {
        value: 'default.us_state.state_region_description',
        label: 'default.us_state.state_region_description (string)',
      },
    ],
  },
  metricNodeColumns: [
    {
      name: 'default_DOT_avg_repair_price',
      type: 'double',
      attributes: [],
      dimension: null,
    },
  ],
  metricNodeHistory: [
    {
      id: 1,
      entity_type: 'node',
      entity_name: 'default.avg_repair_price',
      node: 'default.avg_repair_price',
      activity_type: 'create',
      user: null,
      pre: {},
      post: {},
      details: {},
      created_at: '2023-08-21T16:48:56.950482+00:00',
    },
    {
      id: 2,
      entity_type: 'column_attribute',
      entity_name: 'default.avg_repair_price',
      node: 'default.avg_repair_price',
      activity_type: 'set_attribute',
      user: null,
      pre: {},
      post: {},
      details: {
        attributes: [
          { attribute_type_name: 'primary_key', column_name: 'col1' },
          { attribute_type_name: 'dimension', column_name: 'col1' },
        ],
      },
      created_at: '2023-08-21T16:48:56.950482+00:00',
    },
    {
      id: 3,
      entity_type: 'link',
      entity_name: 'default.avg_repair_price',
      node: 'default.avg_repair_price',
      activity_type: 'create',
      user: null,
      pre: {},
      post: {},
      details: {
        column: 'col1',
        dimension: 'default.hard_hat',
        dimension_column: 'hard_hat_id',
      },
      created_at: '2023-08-21T16:48:56.950482+00:00',
    },
    {
      id: 4,
      entity_type: 'materialization',
      entity_name: 'default.avg_repair_price',
      node: 'default.avg_repair_price',
      activity_type: 'create',
      user: null,
      pre: {},
      post: {},
      details: {
        materialization: 'some_random_materialization',
      },
      created_at: '2023-08-21T16:48:56.950482+00:00',
    },
    {
      id: 5,
      entity_type: 'availability',
      entity_name: 'default.avg_repair_price',
      node: 'default.avg_repair_price',
      activity_type: 'create',
      user: null,
      pre: {},
      post: {},
      details: {
        materialization: 'some_random_materialization',
      },
      created_at: '2023-08-21T16:48:56.950482+00:00',
    },
    {
      id: 6,
      entity_type: 'node',
      entity_name: 'default.avg_repair_price',
      node: 'default.avg_repair_price',
      activity_type: 'status_change',
      user: null,
      pre: { status: 'valid' },
      post: { status: 'invalid' },
      details: {
        upstream_node: 'default.repair_order_details',
      },
      created_at: '2023-08-21T16:48:56.950482+00:00',
    },
  ],
  nodeMaterializations: [
    {
      name: 'country_birth_date_contractor_id_379232101',
      engine: { name: 'spark', version: '2.4.4', uri: null, dialect: 'spark' },
      config: {
        partitions: [
          {
            name: 'country',
            values: ['DE', 'MY'],
            range: null,
            expression: null,
            type_: 'categorical',
          },
          {
            name: 'birth_date',
            values: null,
            range: [20010101, 20020101],
            expression: null,
            type_: 'temporal',
          },
          {
            name: 'contractor_id',
            values: null,
            range: [1, 10],
            expression: null,
            type_: 'categorical',
          },
        ],
        spark: {},
        query:
          "SELECT  default_DOT_hard_hats.address,\n\tdefault_DOT_hard_hats.birth_date,\n\tdefault_DOT_hard_hats.city,\n\tdefault_DOT_hard_hats.contractor_id,\n\tdefault_DOT_hard_hats.country,\n\tdefault_DOT_hard_hats.first_name,\n\tdefault_DOT_hard_hats.hard_hat_id,\n\tdefault_DOT_hard_hats.hire_date,\n\tdefault_DOT_hard_hats.last_name,\n\tdefault_DOT_hard_hats.manager,\n\tdefault_DOT_hard_hats.postal_code,\n\tdefault_DOT_hard_hats.state,\n\tdefault_DOT_hard_hats.title \n FROM roads.hard_hats AS default_DOT_hard_hats \n WHERE  default_DOT_hard_hats.country IN ('DE', 'MY') AND default_DOT_hard_hats.contractor_id BETWEEN 1 AND 10\n\n",
        upstream_tables: ['default.roads.hard_hats'],
      },
      schedule: '0 * * * *',
      job: 'SparkSqlMaterializationJob',
      output_tables: ['common.a', 'common.b'],
      urls: ['http://fake.url/job'],
      clientCode: '',
    },
  ],
  metricNodeRevisions: [
    {
      node_revision_id: 24,
      node_id: 24,
      type: 'metric',
      name: 'default.avg_repair_price',
      display_name: 'Default: Avg Repair Price',
      version: 'v1.0',
      status: 'valid',
      mode: 'published',
      catalog: {
        id: 1,
        uuid: '0fc18295-e1a2-4c3c-b72a-894725c12488',
        created_at: '2023-08-21T16:48:51.146121+00:00',
        updated_at: '2023-08-21T16:48:51.146122+00:00',
        extra_params: {},
        name: 'warehouse',
      },
      schema_: null,
      table: null,
      description: 'Average repair price',
      query:
        'SELECT  avg(price) default_DOT_avg_repair_price \n FROM default.repair_order_details\n\n',
      availability: null,
      columns: [
        {
          name: 'default_DOT_avg_repair_price',
          type: 'double',
          attributes: [],
          dimension: null,
        },
      ],
      updated_at: '2023-08-21T16:48:56.932231+00:00',
      materializations: [],
      parents: [
        {
          name: 'default.repair_order_details',
        },
      ],
    },
  ],
  mockCubeNode: {
    namespace: 'default',
    node_revision_id: 33,
    node_id: 33,
    type: 'cube',
    name: 'default.repair_orders_cube',
    display_name: 'Default: Repair Orders Cube',
    version: 'v1.0',
    status: 'valid',
    mode: 'published',
    catalog: {
      id: 1,
      uuid: '0fc18295-e1a2-4c3c-b72a-894725c12488',
      created_at: '2023-08-21T16:48:51.146121+00:00',
      updated_at: '2023-08-21T16:48:51.146122+00:00',
      extra_params: {},
      name: 'warehouse',
    },
    schema_: null,
    table: null,
    description: 'Repair Orders Cube',
    query: 'SELECT ...',
    availability: null,
    columns: [
      {
        name: 'default_DOT_num_repair_orders',
        type: 'bigint',
        attributes: [],
        dimension: null,
      },
      {
        name: 'default_DOT_avg_repair_price',
        type: 'double',
        attributes: [],
        dimension: null,
      },
      {
        name: 'default_DOT_total_repair_cost',
        type: 'double',
        attributes: [],
        dimension: null,
      },
      {
        name: 'default_DOT_total_repair_order_discounts',
        type: 'double',
        attributes: [],
        dimension: null,
      },
      {
        name: 'company_name',
        type: 'string',
        attributes: [
          {
            attribute_type: {
              namespace: 'system',
              name: 'dimension',
            },
          },
        ],
        dimension: null,
      },
      {
        name: 'state',
        type: 'string',
        attributes: [
          {
            attribute_type: {
              namespace: 'system',
              name: 'dimension',
            },
          },
        ],
        dimension: null,
      },
    ],
    updated_at: '2023-08-21T16:49:02.040424+00:00',
    materializations: [
      {
        name: 'default',
        engine: {
          name: 'duckdb',
          version: '0.7.1',
          uri: null,
          dialect: null,
        },
        config: {
          partitions: [],
          spark: null,
          query: 'SELECT',
          upstream_tables: [
            'warehouse.roads.dispatchers',
            'warehouse.roads.hard_hats',
            'warehouse.roads.repair_order_details',
            'warehouse.roads.repair_orders',
          ],
          dimensions: ['company_name', 'state'],
          measures: {
            default_DOT_num_repair_orders: {
              metric: 'default_DOT_num_repair_orders',
              measures: [
                {
                  name: 'repair_order_id_count',
                  field_name:
                    'm0_default_DOT_num_repair_orders_repair_order_id_count',
                  agg: 'count',
                  type: 'bigint',
                },
              ],
              combiner: 'count(repair_order_id_count)',
            },
            default_DOT_avg_repair_price: {
              metric: 'default_DOT_avg_repair_price',
              measures: [
                {
                  name: 'price_count',
                  field_name: 'm1_default_DOT_avg_repair_price_price_count',
                  agg: 'count',
                  type: 'bigint',
                },
                {
                  name: 'price_sum',
                  field_name: 'm1_default_DOT_avg_repair_price_price_sum',
                  agg: 'sum',
                  type: 'double',
                },
              ],
              combiner: 'sum(price_sum) / count(price_count)',
            },
            default_DOT_total_repair_cost: {
              metric: 'default_DOT_total_repair_cost',
              measures: [
                {
                  name: 'price_sum',
                  field_name: 'm2_default_DOT_total_repair_cost_price_sum',
                  agg: 'sum',
                  type: 'double',
                },
              ],
              combiner: 'sum(price_sum)',
            },
            default_DOT_total_repair_order_discounts: {
              metric: 'default_DOT_total_repair_order_discounts',
              measures: [
                {
                  name: 'price_discount_sum',
                  field_name:
                    'm3_default_DOT_total_repair_order_discounts_price_discount_sum',
                  agg: 'sum',
                  type: 'double',
                },
              ],
              combiner: 'sum(price_discount_sum)',
            },
          },
        },
        schedule: '@daily',
        job: 'DefaultCubeMaterialization',
      },
    ],
    parents: [
      {
        name: 'default.hard_hat',
      },
      {
        name: 'default.dispatcher',
      },
      {
        name: 'default.num_repair_orders',
      },
      {
        name: 'default.avg_repair_price',
      },
      {
        name: 'default.total_repair_cost',
      },
      {
        name: 'default.total_repair_order_discounts',
      },
    ],
    created_at: '2023-08-21T16:49:01.671395+00:00',
    tags: [],
  },
  mockCubesCube: {
    node_revision_id: 33,
    node_id: 33,
    type: 'cube',
    name: 'default.repair_orders_cube',
    display_name: 'Default: Repair Orders Cube',
    version: 'v1.0',
    description: 'Repair Orders Cube',
    availability: null,
    cube_elements: [
      {
        name: 'default_DOT_num_repair_orders',
        node_name: 'default.num_repair_orders',
        type: 'metric',
      },
      {
        name: 'default_DOT_avg_repair_price',
        node_name: 'default.avg_repair_price',
        type: 'metric',
      },
      {
        name: 'default_DOT_total_repair_cost',
        node_name: 'default.total_repair_cost',
        type: 'metric',
      },
      {
        name: 'default_DOT_total_repair_order_discounts',
        node_name: 'default.total_repair_order_discounts',
        type: 'metric',
      },
      {
        name: 'state',
        node_name: 'default.hard_hat',
        type: 'dimension',
      },
      {
        name: 'company_name',
        node_name: 'default.dispatcher',
        type: 'dimension',
      },
    ],
    query: 'SELECT',
    columns: [
      {
        name: 'default_DOT_num_repair_orders',
        type: 'bigint',
        attributes: [],
        dimension: null,
      },
      {
        name: 'default_DOT_avg_repair_price',
        type: 'double',
        attributes: [],
        dimension: null,
      },
      {
        name: 'default_DOT_total_repair_cost',
        type: 'double',
        attributes: [],
        dimension: null,
      },
      {
        name: 'default_DOT_total_repair_order_discounts',
        type: 'double',
        attributes: [],
        dimension: null,
      },
      {
        name: 'company_name',
        type: 'string',
        attributes: [
          {
            attribute_type: {
              namespace: 'system',
              name: 'dimension',
            },
          },
        ],
        dimension: null,
      },
      {
        name: 'state',
        type: 'string',
        attributes: [
          {
            attribute_type: {
              namespace: 'system',
              name: 'dimension',
            },
          },
        ],
        dimension: null,
      },
    ],
    updated_at: '2023-08-21T16:49:02.040424+00:00',
    materializations: [
      {
        name: 'default',
        engine: {
          name: 'duckdb',
          version: '0.7.1',
          uri: null,
          dialect: null,
        },
        config: {
          partitions: [],
          spark: null,
          query: 'SELECT',
          upstream_tables: [
            'warehouse.roads.dispatchers',
            'warehouse.roads.hard_hats',
            'warehouse.roads.repair_order_details',
            'warehouse.roads.repair_orders',
          ],
          dimensions: ['company_name', 'state'],
          measures: {
            default_DOT_num_repair_orders: {
              metric: 'default_DOT_num_repair_orders',
              measures: [
                {
                  name: 'repair_order_id_count',
                  field_name:
                    'm0_default_DOT_num_repair_orders_repair_order_id_count',
                  agg: 'count',
                  type: 'bigint',
                },
              ],
              combiner: 'count(repair_order_id_count)',
            },
            default_DOT_avg_repair_price: {
              metric: 'default_DOT_avg_repair_price',
              measures: [
                {
                  name: 'price_count',
                  field_name: 'm1_default_DOT_avg_repair_price_price_count',
                  agg: 'count',
                  type: 'bigint',
                },
                {
                  name: 'price_sum',
                  field_name: 'm1_default_DOT_avg_repair_price_price_sum',
                  agg: 'sum',
                  type: 'double',
                },
              ],
              combiner: 'sum(price_sum) / count(price_count)',
            },
            default_DOT_total_repair_cost: {
              metric: 'default_DOT_total_repair_cost',
              measures: [
                {
                  name: 'price_sum',
                  field_name: 'm2_default_DOT_total_repair_cost_price_sum',
                  agg: 'sum',
                  type: 'double',
                },
              ],
              combiner: 'sum(price_sum)',
            },
            default_DOT_total_repair_order_discounts: {
              metric: 'default_DOT_total_repair_order_discounts',
              measures: [
                {
                  name: 'price_discount_sum',
                  field_name:
                    'm3_default_DOT_total_repair_order_discounts_price_discount_sum',
                  agg: 'sum',
                  type: 'double',
                },
              ],
              combiner: 'sum(price_discount_sum)',
            },
          },
        },
        schedule: '@daily',
        job: 'DefaultCubeMaterialization',
      },
    ],
  },
  mockNodeLineage: [
    {
      column_name: 'default_DOT_avg_repair_price',
      node_name: 'default.avg_repair_price',
      node_type: 'metric',
      display_name: 'Default: Avg Repair Price',
      lineage: [
        {
          column_name: 'price',
          node_name: 'default.repair_order_details',
          node_type: 'source',
          display_name: 'Default: Repair Order Details',
          lineage: [],
        },
      ],
    },
  ],
  mockMetricNodeJson: {
    namespace: 'default',
    node_revision_id: 24,
    node_id: 24,
    type: 'metric',
    name: 'default.avg_repair_price',
    display_name: 'Default: Avg Repair Price',
    version: 'v1.0',
    status: 'valid',
    mode: 'published',
    catalog: {
      id: 1,
      uuid: '0fc18295-e1a2-4c3c-b72a-894725c12488',
      created_at: '2023-08-21T16:48:51.146121+00:00',
      updated_at: '2023-08-21T16:48:51.146122+00:00',
      extra_params: {},
      name: 'warehouse',
    },
    schema_: null,
    table: null,
    description: 'Average repair price',
    query:
      'SELECT  avg(price) default_DOT_avg_repair_price \n FROM default.repair_order_details\n\n',
    availability: null,
    columns: [
      {
        name: 'default_DOT_avg_repair_price',
        type: 'double',
        attributes: [],
        dimension: null,
      },
    ],
    updated_at: '2023-08-21T16:48:56.932231+00:00',
    materializations: [],
    parents: [
      {
        name: 'default.repair_order_details',
      },
    ],
    created_at: '2023-08-21T16:48:56.932162+00:00',
    tags: [],
    primary_key: [],
    createNodeClientCode:
      'dj = DJBuilder(DJ_URL)\n\navg_repair_price = dj.create_metric(\n    description="Average repair price",\n    display_name="Default: Avg Repair Price",\n    name="default.avg_repair_price",\n    primary_key=[],\n    query="""SELECT  avg(price) default_DOT_avg_repair_price \n FROM default.repair_order_details\n\n"""\n)',
    dimensions: [
      {
        name: 'default.date_dim.dateint',
        type: 'timestamp',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
          'default.hard_hat.birth_date',
        ],
      },
      {
        name: 'default.date_dim.dateint',
        type: 'timestamp',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
          'default.hard_hat.hire_date',
        ],
      },
      {
        name: 'default.date_dim.day',
        type: 'int',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
          'default.hard_hat.birth_date',
        ],
      },
      {
        name: 'default.date_dim.day',
        type: 'int',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
          'default.hard_hat.hire_date',
        ],
      },
      {
        name: 'default.date_dim.month',
        type: 'int',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
          'default.hard_hat.birth_date',
        ],
      },
      {
        name: 'default.date_dim.month',
        type: 'int',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
          'default.hard_hat.hire_date',
        ],
      },
      {
        name: 'default.date_dim.year',
        type: 'int',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
          'default.hard_hat.birth_date',
        ],
      },
      {
        name: 'default.date_dim.year',
        type: 'int',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
          'default.hard_hat.hire_date',
        ],
      },
      {
        name: 'default.hard_hat.address',
        type: 'string',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
        ],
      },
      {
        name: 'default.hard_hat.birth_date',
        type: 'date',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
        ],
      },
      {
        name: 'default.hard_hat.city',
        type: 'string',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
        ],
      },
      {
        name: 'default.hard_hat.contractor_id',
        type: 'int',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
        ],
      },
      {
        name: 'default.hard_hat.country',
        type: 'string',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
        ],
      },
      {
        name: 'default.hard_hat.first_name',
        type: 'string',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
        ],
      },
      {
        name: 'default.hard_hat.hard_hat_id',
        type: 'int',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
        ],
      },
      {
        name: 'default.hard_hat.hire_date',
        type: 'date',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
        ],
      },
      {
        name: 'default.hard_hat.last_name',
        type: 'string',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
        ],
      },
      {
        name: 'default.hard_hat.manager',
        type: 'int',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
        ],
      },
      {
        name: 'default.hard_hat.postal_code',
        type: 'string',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
        ],
      },
      {
        name: 'default.hard_hat.state',
        type: 'string',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
        ],
      },
      {
        name: 'default.hard_hat.title',
        type: 'string',
        path: [
          'default.repair_order_details.repair_order_id',
          'default.repair_order.hard_hat_id',
        ],
      },
    ],
  },
  mockNodeDAG: [
    {
      namespace: 'default',
      node_revision_id: 2,
      node_id: 2,
      type: 'source',
      name: 'default.repair_order_details',
      display_name: 'Default: Repair Order Details',
      version: 'v1.0',
      status: 'valid',
      mode: 'published',
      catalog: {
        id: 1,
        uuid: '0fc18295-e1a2-4c3c-b72a-894725c12488',
        created_at: '2023-08-21T16:48:51.146121+00:00',
        updated_at: '2023-08-21T16:48:51.146122+00:00',
        extra_params: {},
        name: 'warehouse',
      },
      schema_: 'roads',
      table: 'repair_order_details',
      description: 'Details on repair orders',
      query: null,
      availability: null,
      columns: [
        {
          name: 'repair_order_id',
          type: 'int',
          attributes: [],
          dimension: {
            name: 'default.repair_order',
          },
        },
        {
          name: 'repair_type_id',
          type: 'int',
          attributes: [],
          dimension: null,
        },
        {
          name: 'price',
          type: 'float',
          attributes: [],
          dimension: null,
        },
        {
          name: 'quantity',
          type: 'int',
          attributes: [],
          dimension: null,
        },
        {
          name: 'discount',
          type: 'float',
          attributes: [],
          dimension: null,
        },
      ],
      updated_at: '2023-08-21T16:48:52.981201+00:00',
      materializations: [],
      parents: [],
      created_at: '2023-08-21T16:48:52.970554+00:00',
      tags: [],
    },
    {
      namespace: 'default',
      node_revision_id: 14,
      node_id: 14,
      type: 'dimension',
      name: 'default.date_dim',
      display_name: 'Default: Date Dim',
      version: 'v1.0',
      status: 'valid',
      mode: 'published',
      catalog: {
        id: 1,
        uuid: '0fc18295-e1a2-4c3c-b72a-894725c12488',
        created_at: '2023-08-21T16:48:51.146121+00:00',
        updated_at: '2023-08-21T16:48:51.146122+00:00',
        extra_params: {},
        name: 'warehouse',
      },
      schema_: null,
      table: null,
      description: 'Date dimension',
      query:
        '\n            SELECT\n              dateint,\n              month,\n              year,\n              day\n            FROM default.date\n        ',
      availability: null,
      columns: [
        {
          name: 'dateint',
          type: 'timestamp',
          attributes: [
            {
              attribute_type: {
                namespace: 'system',
                name: 'primary_key',
              },
            },
          ],
          dimension: null,
        },
        {
          name: 'month',
          type: 'int',
          attributes: [],
          dimension: null,
        },
        {
          name: 'year',
          type: 'int',
          attributes: [],
          dimension: null,
        },
        {
          name: 'day',
          type: 'int',
          attributes: [],
          dimension: null,
        },
      ],
      updated_at: '2023-08-21T16:48:54.726980+00:00',
      materializations: [],
      parents: [
        {
          name: 'default.date',
        },
      ],
      created_at: '2023-08-21T16:48:54.726871+00:00',
      tags: [],
    },
    {
      namespace: 'default',
      node_revision_id: 18,
      node_id: 18,
      type: 'dimension',
      name: 'default.hard_hat',
      display_name: 'Default: Hard Hat',
      version: 'v1.0',
      status: 'valid',
      mode: 'published',
      catalog: {
        id: 1,
        uuid: '0fc18295-e1a2-4c3c-b72a-894725c12488',
        created_at: '2023-08-21T16:48:51.146121+00:00',
        updated_at: '2023-08-21T16:48:51.146122+00:00',
        extra_params: {},
        name: 'warehouse',
      },
      schema_: null,
      table: null,
      description: 'Hard hat dimension',
      query:
        '\n            SELECT\n            hard_hat_id,\n            last_name,\n            first_name,\n            title,\n            birth_date,\n            hire_date,\n            address,\n            city,\n            state,\n            postal_code,\n            country,\n            manager,\n            contractor_id\n            FROM default.hard_hats\n        ',
      availability: null,
      columns: [
        {
          name: 'hard_hat_id',
          type: 'int',
          attributes: [
            {
              attribute_type: {
                namespace: 'system',
                name: 'primary_key',
              },
            },
          ],
          dimension: null,
        },
        {
          name: 'last_name',
          type: 'string',
          attributes: [],
          dimension: null,
        },
        {
          name: 'first_name',
          type: 'string',
          attributes: [],
          dimension: null,
        },
        {
          name: 'title',
          type: 'string',
          attributes: [],
          dimension: null,
        },
        {
          name: 'birth_date',
          type: 'date',
          attributes: [],
          dimension: {
            name: 'default.date_dim',
          },
        },
        {
          name: 'hire_date',
          type: 'date',
          attributes: [],
          dimension: {
            name: 'default.date_dim',
          },
        },
        {
          name: 'address',
          type: 'string',
          attributes: [],
          dimension: null,
        },
        {
          name: 'city',
          type: 'string',
          attributes: [],
          dimension: null,
        },
        {
          name: 'state',
          type: 'string',
          attributes: [],
          dimension: {
            name: 'default.us_state',
          },
        },
        {
          name: 'postal_code',
          type: 'string',
          attributes: [],
          dimension: null,
        },
        {
          name: 'country',
          type: 'string',
          attributes: [],
          dimension: null,
        },
        {
          name: 'manager',
          type: 'int',
          attributes: [],
          dimension: null,
        },
        {
          name: 'contractor_id',
          type: 'int',
          attributes: [],
          dimension: null,
        },
      ],
      updated_at: '2023-08-21T16:48:55.594603+00:00',
      materializations: [],
      parents: [
        {
          name: 'default.hard_hats',
        },
      ],
      created_at: '2023-08-21T16:48:55.594537+00:00',
      tags: [],
    },
    {
      namespace: 'default',
      node_revision_id: 24,
      node_id: 24,
      type: 'metric',
      name: 'default.avg_repair_price',
      display_name: 'Default: Avg Repair Price',
      version: 'v1.0',
      status: 'valid',
      mode: 'published',
      catalog: {
        id: 1,
        uuid: '0fc18295-e1a2-4c3c-b72a-894725c12488',
        created_at: '2023-08-21T16:48:51.146121+00:00',
        updated_at: '2023-08-21T16:48:51.146122+00:00',
        extra_params: {},
        name: 'warehouse',
      },
      schema_: null,
      table: null,
      description: 'Average repair price',
      query:
        'SELECT  avg(price) default_DOT_avg_repair_price \n FROM default.repair_order_details\n\n',
      availability: null,
      columns: [
        {
          name: 'default_DOT_avg_repair_price',
          type: 'double',
          attributes: [],
          dimension: null,
        },
      ],
      updated_at: '2023-08-21T16:48:56.932231+00:00',
      materializations: [],
      parents: [
        {
          name: 'default.repair_order_details',
        },
      ],
      created_at: '2023-08-21T16:48:56.932162+00:00',
      tags: [],
    },
  ],
  mockDimensionNode: {
    namespace: 'default',
    node_revision_id: 21,
    node_id: 21,
    type: 'dimension',
    name: 'default.dispatcher',
    display_name: 'Default: Dispatcher',
    version: 'v1.0',
    status: 'valid',
    mode: 'published',
    catalog: {
      id: 1,
      uuid: '0fc18295-e1a2-4c3c-b72a-894725c12488',
      created_at: '2023-08-21T16:48:51.146121+00:00',
      updated_at: '2023-08-21T16:48:51.146122+00:00',
      extra_params: {},
      name: 'warehouse',
    },
    schema_: null,
    table: null,
    description: 'Dispatcher dimension',
    query:
      '\n            SELECT\n            dispatcher_id,\n            company_name,\n            phone\n            FROM default.dispatchers\n        ',
    availability: null,
    columns: [
      {
        name: 'dispatcher_id',
        type: 'int',
        attributes: [
          {
            attribute_type: {
              namespace: 'system',
              name: 'primary_key',
            },
          },
        ],
        dimension: null,
      },
      {
        name: 'company_name',
        type: 'string',
        attributes: [],
        dimension: null,
      },
      {
        name: 'phone',
        type: 'string',
        attributes: [],
        dimension: null,
      },
    ],
    updated_at: '2023-08-21T16:48:56.399124+00:00',
    materializations: [],
    parents: [
      {
        name: 'default.dispatchers',
      },
    ],
    created_at: '2023-08-21T16:48:56.399025+00:00',
    tags: [],
  },
  mockLinkedNodes: [
    {
      node_revision_id: 2,
      node_id: 2,
      type: 'source',
      name: 'default.repair_order_details',
      display_name: 'Default: Repair Order Details',
    },
    {
      node_revision_id: 3,
      node_id: 3,
      type: 'source',
      name: 'default.num_repair_orders',
      display_name: 'Default: Num Repair Orders',
    },
  ],
};
