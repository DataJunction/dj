import HttpClient from './httpclient.js'

export class DJClient extends HttpClient {
    constructor(
        baseURL,
        namespace,
        engineName = null,
        engineVersion = null,
        httpAgent = null
    ) {
        super(
            {
                baseURL,
            },
            httpAgent
        )
        this.namespace = namespace
        this.engineName = engineName
        this.engineVersion = engineVersion
    }

    get healthcheck() {
        return {
            get: () => this.get('/health/'),
        }
    }

    get catalogs() {
        return {
            list: () => this.get('/catalogs/'),
            get: (catalog) => this.get(`/catalogs/${catalog}/`),
            create: (catalog) =>
                this.setHeader('Content-Type', 'application/json').post(
                    `/catalogs/`,
                    catalog
                ),
            addEngine: (catalog, engineName, engineVersion) =>
                this.setHeader('Content-Type', 'application/json').post(
                    `/catalogs/${catalog}/engines/`,
                    [
                        {
                            name: engineName,
                            version: engineVersion,
                        },
                    ]
                ),
        }
    }

    get engines() {
        return {
            list: () => this.get('/engines/'),
            get: (engineName, engineVersion) =>
                this.get(`/engines/${engineName}/${engineVersion}/`),
            create: (engine) => this.post('/engines/', engine),
        }
    }

    get addEngineToCatalog() {
        return {
            set: (catalogName, engine) =>
                this.setHeader('Content-Type', 'application/json').post(
                    `/catalogs/${catalogName}/engines/`,
                    [engine]
                ),
        }
    }

    get namespaces() {
        return {
            list: () => this.get('/namespaces/'),
            nodes: (namespace) => this.get(`/namespaces/${namespace}/`),
            create: (namespace) =>
                this.setHeader('Content-Type', 'application/json').post(
                    `/namespaces/${namespace}/`
                ),
        }
    }

    get commonDimensions() {
        return {
            list: (metrics) => {
                const metricsQuery =
                    '?' + metrics.map((m) => `metric=${m}`).join('&')
                return this.get('/metrics/common/dimensions/' + metricsQuery)
            },
        }
    }

    get nodes() {
        return {
            get: (nodeName) => this.get(`/nodes/${nodeName}/`),
            validate: (nodeDetails) =>
                this.setHeader('Content-Type', 'application/json').post(
                    '/nodes/validate/',
                    nodeDetails
                ),
            update: (nodeName, nodeDetails) =>
                this.setHeader('Content-Type', 'application/json').patch(
                    `/nodes/${nodeName}/`,
                    nodeDetails
                ),
            revisions: (nodeName) => this.get(`/nodes/${nodeName}/revisions/`),
            downstream: (nodeName) =>
                this.get(`/nodes/${nodeName}/downstream/`),
            upstream: (nodeName) => this.get(`/nodes/${nodeName}/upstream/`),
            publish: (nodeName) => this.patch(`/nodes/${nodeName}/`, {'mode': 'published'})
        }
    }

    get sources() {
        return {
            create: (sourceDetails) =>
                this.setHeader('Content-Type', 'application/json').post(
                    '/nodes/source/',
                    sourceDetails
                ),
            list: () => this.get(`/namespaces/${this.namespace}/?type_=source`),
        }
    }

    get transforms() {
        return {
            create: (transformDetails) =>
                this.setHeader('Content-Type', 'application/json').post(
                    '/nodes/transform/',
                    transformDetails
                ),
            list: () =>
                this.get(`/namespaces/${this.namespace}/?type_=transform`),
        }
    }

    get dimensions() {
        return {
            create: (dimensionDetails) =>
                this.setHeader('Content-Type', 'application/json').post(
                    '/nodes/dimension/',
                    dimensionDetails
                ),
            list: () =>
                this.get(`/namespaces/${this.namespace}/?type_=dimension`),
            link: (nodeName, nodeColumn, dimension, dimensionColumn) =>
                this.post(
                    `/nodes/${nodeName}/columns/${nodeColumn}/?dimension=${dimension}&dimension_column=${dimensionColumn}`
                ),
        }
    }

    get metrics() {
        return {
            get: (metricName) => this.get(`/metrics/${metricName}/`),
            create: (metricDetails) =>
                this.setHeader('Content-Type', 'application/json').post(
                    '/nodes/metric/',
                    metricDetails
                ),
            list: () => this.get(`/namespaces/${this.namespace}/?type_=metric`),
            all: () => this.get(`/metrics/`),
        }
    }

    get cubes() {
        return {
            get: (cubeName) => this.get(`/cubes/${cubeName}/`),
            create: (cubeDetails) =>
                this.setHeader('Content-Type', 'application/json').post(
                    '/nodes/cube/',
                    cubeDetails
                ),
        }
    }

    get tags() {
        return {
            list: () => this.get('/tags/'),
            get: (tagName) => this.get(`/tags/${tagName}/`),
            create: (tagData) =>
                this.setHeader('Content-Type', 'application/json').post(
                    '/tags/',
                    tagData
                ),
            update: (tagName, tagData) =>
                this.setHeader('Content-Type', 'application/json').patch(
                    `/tags/${tagName}/`,
                    tagData
                ),
            set: (nodeName, tagName) =>
                this.post(`/nodes/${nodeName}/tag/?tag_name=${tagName}`),
            listNodes: (tagName) => this.get(`/tags/${tagName}/nodes/`),
        }
    }

    get attributes() {
        return {
            list: () => this.get('/attributes/'),
            create: (attributeData) =>
                this.setHeader('Content-Type', 'application/json').post(
                    '/attributes/',
                    attributeData
                ),
        }
    }

    get materializationConfigs() {
        return {
            update: (nodeName, materializationDetails) =>
                this.setHeader('Content-Type', 'application/json').post(
                    `/nodes/${nodeName}/materialization/`,
                    materializationDetails
                ),
        }
    }

    get columnAttributes() {
        return {
            set: (nodeName, columnAttribute) =>
                this.setHeader('Content-Type', 'application/json').post(
                    `/nodes/${nodeName}/attributes/`,
                    [columnAttribute]
                ),
        }
    }

    get availabilityState() {
        return {
            set: (nodeName, availabilityState) =>
                this.setHeader('Content-Type', 'application/json').post(
                    `/data/${nodeName}/availability/`,
                    availabilityState
                ),
        }
    }

    get sql() {
        return {
            get: (
                metrics,
                dimensions,
                filters,
                engineName = null,
                engineVersion = null
            ) => {
                const metricsQuery =
                    '?' + metrics.map((m) => `metrics=${m}`).join('&')
                const dimensionsQuery = dimensions
                    .map((d) => `dimensions=${d}`)
                    .join('&')
                const filtersQuery = filters
                    .map((f) => `filters=${f}`)
                    .join('&')
                const engineNameP = engineName ? `&engine=${engineName}` : ''
                const engineVersionP = engineVersion
                    ? `&engine_version=${engineVersion}`
                    : ''
                return this.get(
                    `/sql/${metricsQuery}&${dimensionsQuery}${filtersQuery}${engineNameP}${engineVersionP}`
                )
            },
        }
    }

    get data() {
        return {
            get: (
                metrics,
                dimensions,
                filters,
                async_ = false,
                engineName = null,
                engineVersion = null
            ) => {
                const metricsQuery =
                    '?' + metrics.map((m) => `metrics=${m}`).join('&')
                const dimensionsQuery = dimensions
                    .map((d) => `dimensions=${d}`)
                    .join('&')
                const filtersQuery = filters
                    .map((f) => `filters=${f}`)
                    .join('&')
                const asyncP = async_ ? `&async_=${async_}` : ''
                const engineNameP = engineName ? `&engine=${engineName}` : ''
                const engineVersionP = engineVersion
                    ? `&engine_version=${engineVersion}`
                    : ''
                const data = this.get(
                    `/data/${metricsQuery}&${dimensionsQuery}${filtersQuery}${asyncP}${engineNameP}${engineVersionP}`
                ).then((data) => {
                    return {
                        columns: data.results[0].columns,
                        data: data.results[0].rows,
                    }
                })
                return data
            },
        }
    }
}
