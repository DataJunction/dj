import HttpClient from './httpclient.js'

export class DJClient extends HttpClient {
  constructor (baseURL, namespace, engineName = null, engineVersion = null) {
    super({
      baseURL
    })
    this.namespace = namespace
    this.engineName = engineName
    this.engineVersion = engineVersion
  }

  get catalogs () {
    return {
      list: () => this.get('/catalogs/'),
      get: (catalog) => this.get(`/catalogs/${catalog}/`),
      create: (catalog) => this.get('/catalogs/', catalog)
    }
  }

  get engines () {
    return {
      list: () => this.get('/engines/'),
      get: (engineName, engineVersion) => this.get(`/engines/${engineName}/${engineVersion}/`),
      create: (engine) => this.post('/engines/', engine)
    }
  }

  get attachEngineToCatalog () {
    return {
      get: (engineName, engineVersion) => this.get(`/engines/${engineName}/${engineVersion}/`),
      create: (engine) => this.post('/engines/', engine)
    }
  }

  get namespaces () {
    return {
      list: () => this.get('/namespaces/'),
      nodes: (namespace) => this.get(`/namespaces/${namespace}/`),
      create: (namespace) => this.post(`/namespaces/${namespace}/`)
    }
  }

  get commonDimensions () {
    return {
      list: (metrics) => this.get(`/metrics/common/dimensions/?metrics=${metrics}`)
    }
  }

  get nodes () {
    return {
      get: (name) => this.get(`/nodes/${name}/`),
      validate: (node) => this.post('/nodes/validate/', node),
      update: (name, node) => this.patch(`/nodes/${name}/`, node),
      revisions: (name) => this.get(`/nodes/${name}/revisions/`),
      downstream: (name) => this.get(`/nodes/${name}/downstream/`),
      upstream: (name) => this.get(`/nodes/${name}/upstream/`)
    }
  }

  get sources () {
    return {
      create: (source) => this.post('/nodes/source/', source),
      list: () => this.get(`/namespaces/${this.namespace}/?type_=source`)
    }
  }

  get transforms () {
    return {
      create: (transform) => this.post('/nodes/transform/', transform),
      list: () => this.get(`/namespaces/${this.namespace}/?type_=transform`)
    }
  }

  get dimensions () {
    return {
      create: (dimension) => this.post('/nodes/dimension/', dimension),
      list: () => this.get(`/namespaces/${this.namespace}/?type_=dimension`),
      link: (nodeName, column, dimension, dimensionColumn) => this.post(`/nodes/${nodeName}/columns/${column}/?dimension=${dimension}&dimension_column=${dimensionColumn}`)
    }
  }

  get metrics () {
    return {
      create: (metric) => this.post('/nodes/metric/', metric),
      list: () => this.get(`/namespaces/${this.namespace}/?type_=metric`)
    }
  }

  get cubes () {
    return {
      get: (cube) => this.post(`/cubes/${cube}/`),
      create: (cube) => this.post('/nodes/cube/', cube)
    }
  }

  get tags () {
    return {
      list: () => this.get('/tags/'),
      get: (tag) => this.get(`/tags/${tag}/nodes/`),
      create: (tagData) => this.post('/tags/', tagData),
      update: (tag, tagData) => this.patch(`/tags/${tag}/`, tagData),
      set: (nodeName, tagName) => this.post(`/nodes/${nodeName}/tag/?tag_name=${tagName}`)
    }
  }

  get attributes () {
    return {
      list: () => this.get('/attributes/'),
      create: (attributeData) => this.post('/attributes/', attributeData)
    }
  }

  get materializationConfigs () {
    return {
      update: (nodeName, materializationConfig) => this.patch(`/nodes/${nodeName}/materialization/`, materializationConfig)
    }
  }

  get columnAttributes () {
    return {
      set: (nodeName, columnAttribute) => this.post(`/nodes/${nodeName}/attributes/`, columnAttribute)
    }
  }

  get availabilityState () {
    return {
      set: (nodeName, availabilityState) => this.post(`/data/${nodeName}/availability/`, availabilityState)
    }
  }

  get sql () {
    return {
      get: (
        metrics,
        dimensions,
        filters,
        async_,
        engineName,
        engineVersion
      ) => this.post(`/sql/?metrics=${metrics}&dimensions=${dimensions}&filters=${filters}&async_=${async_}&engine_name=${engineName}&engine_version=${engineVersion}`)
    }
  }

  get data () {
    return {
      get: (
        metrics,
        dimensions,
        filters,
        async_,
        engineName,
        engineVersion
      ) => this.post(`/data/?metrics=${metrics}&dimensions=${dimensions}&filters=${filters}&async_=${async_}&engine_name=${engineName}&engine_version=${engineVersion}`)
    }
  }
}
