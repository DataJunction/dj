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
      id: 33,
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
};
