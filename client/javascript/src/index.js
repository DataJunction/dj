import HttpClient from './httpclient.js'

export class DJClient extends HttpClient {
    constructor(baseURL, namespace, engineName = null, engineVersion = null) {
        super({
            baseURL,
        })
        this.namespace = namespace
        this.engineName = engineName
        this.engineVersion = engineVersion
    }

    get catalog() {
        return {
            list: () => this.get('/catalogs/'),
            get: (catalog) => this.get(`/catalogs/${catalog}/`),
            create: (name, engines) =>
                this.setHeader('Content-Type', 'application/json').post(
                    '/catalogs/',
                    {
                        name,
                        engines,
                    }
                ),
        }
    }

    get engines() {
        return {
            list: () => this.get('/engines/'),
            get: (engineName, engineVersion) =>
                this.get(`/engines/${engineName}/${engineVersion}/`),
            create: (engineName, engineVersion, engineUri, engineDialect) =>
                this.setHeader('Content-Type', 'application/json').post(
                    '/engines/',
                    {
                        name: engineName,
                        version: engineVersion,
                        uri: engineUri,
                        dialect: engineDialect
                    }
                ),
        }
    }

    get addEngineToCatalog() {
        return {
            set: (catalogName, engineName, engineVersion, engineUri, engineDialect) =>
                this.setHeader('Content-Type', 'application/json').post(
                    `/catalogs/${catalogName}/engines/`,
                    [
                        {
                            name: engineName,
                            version: engineVersion,
                            uri: engineUri,
                            dialect: engineDialect
                        }
                    ]
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
            list: (metrics) =>
                this.get(`/metrics/common/dimensions/?metrics=${metrics}`),
        }
    }

    get nodes() {
        return {
            get: (name) => this.get(`/nodes/${name}/`),
            validate: (node) =>
                this.setHeader('Content-Type', 'application/json').post(
                    '/nodes/validate/',
                    node
                ),
            update: (name, node) =>
                this.setHeader('Content-Type', 'application/json').patch(
                    `/nodes/${name}/`,
                    node
                ),
            revisions: (name) => this.get(`/nodes/${name}/revisions/`),
            downstream: (name) => this.get(`/nodes/${name}/downstream/`),
            upstream: (name) => this.get(`/nodes/${name}/upstream/`),
        }
    }

    get sources() {
        return {
            create: (source) =>
                this.setHeader('Content-Type', 'application/json').post(
                    '/nodes/source/',
                    source
                ),
            list: () => this.get(`/namespaces/${this.namespace}/?type_=source`),
        }
    }

    get transforms() {
        return {
            create: (transform) =>
                this.setHeader('Content-Type', 'application/json').post(
                    '/nodes/transform/',
                    transform
                ),
            list: () =>
                this.get(`/namespaces/${this.namespace}/?type_=transform`),
        }
    }

    get dimensions() {
        return {
            create: (dimension) =>
                this.setHeader('Content-Type', 'application/json').post(
                    '/nodes/dimension/',
                    dimension
                ),
            list: () =>
                this.get(`/namespaces/${this.namespace}/?type_=dimension`),
            link: (nodeName, column, dimension, dimensionColumn) =>
                this.post(
                    `/nodes/${nodeName}/columns/${column}/?dimension=${dimension}&dimension_column=${dimensionColumn}`
                ),
        }
    }

    get metrics() {
        return {
            create: (metric) =>
                this.setHeader('Content-Type', 'application/json').post(
                    '/nodes/metric/',
                    metric
                ),
            list: () => this.get(`/namespaces/${this.namespace}/?type_=metric`),
        }
    }

    get cubes() {
        return {
            get: (cube) => this.post(`/cubes/${cube}/`),
            create: (cube) =>
                this.setHeader('Content-Type', 'application/json').post(
                    '/nodes/cube/',
                    cube
                ),
        }
    }

    get tags() {
        return {
            list: () => this.get('/tags/'),
            get: (tag) => this.get(`/tags/${tag}/nodes/`),
            create: (tagData) =>
                this.setHeader('Content-Type', 'application/json').post(
                    '/tags/',
                    tagData
                ),
            update: (tag, tagData) =>
                this.setHeader('Content-Type', 'application/json').patch(
                    `/tags/${tag}/`,
                    tagData
                ),
            set: (nodeName, tagName) =>
                this.post(`/nodes/${nodeName}/tag/?tag_name=${tagName}`),
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
            update: (nodeName, materializationConfig) =>
                this.setHeader('Content-Type', 'application/json').patch(
                    `/nodes/${nodeName}/materialization/`,
                    materializationConfig
                ),
        }
    }

    get columnAttributes() {
        return {
            set: (nodeName, columnAttribute) =>
                this.setHeader('Content-Type', 'application/json').post(
                    `/nodes/${nodeName}/attributes/`,
                    columnAttribute
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
                async_,
                engineName,
                engineVersion
            ) =>
                this.post(
                    `/sql/?metrics=${metrics}&dimensions=${dimensions}&filters=${filters}&async_=${async_}&engine_name=${engineName}&engine_version=${engineVersion}`
                ),
        }
    }

    get data() {
        return {
            get: (
                metrics,
                dimensions,
                filters,
                async_,
                engineName,
                engineVersion
            ) =>
                this.post(
                    `/data/?metrics=${metrics}&dimensions=${dimensions}&filters=${filters}&async_=${async_}&engine_name=${engineName}&engine_version=${engineVersion}`
                ),
        }
    }
}
