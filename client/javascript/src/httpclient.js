export default class HttpClient {
    constructor(options = {}) {
        this._baseURL = options.baseURL || ''
        this._headers = options.headers || {}
    }

    async _fetchJSON(endpoint, options = {}) {
        const res = await fetch(this._baseURL + endpoint, {
            ...options,
            headers: this._headers,
        })

        if (!res.ok) throw new Error(res.statusText)

        if (options.parseResponse !== false && res.status !== 204) {
            return res.json()
        }

        return undefined
    }

    setHeader(key, value) {
        this._headers[key] = value
        return this
    }

    getHeader(key) {
        return this._headers[key]
    }

    setBasicAuth(username, password) {
        this._headers.Authorization = `Basic ${btoa(`${username}:${password}`)}`
        return this
    }

    setBearerAuth(token) {
        this._headers.Authorization = `Bearer ${token}`
        return this
    }

    async get(endpoint, options = {}) {
        return this._fetchJSON(endpoint, {
            ...options,
            method: 'GET',
        })
    }

    async post(endpoint, body, options = {}) {
        return this._fetchJSON(endpoint, {
            ...options,
            body: body ? JSON.stringify(body) : undefined,
            method: 'POST',
        })
    }

    async put(endpoint, body, options = {}) {
        return this._fetchJSON(endpoint, {
            ...options,
            body: body ? JSON.stringify(body) : undefined,
            method: 'PUT',
        })
    }

    async patch(endpoint, operations, options = {}) {
        return this._fetchJSON(endpoint, {
            parseResponse: false,
            ...options,
            body: JSON.stringify(operations),
            method: 'PATCH',
        })
    }

    async delete(endpoint, options = {}) {
        return this._fetchJSON(endpoint, {
            parseResponse: false,
            ...options,
            method: 'DELETE',
        })
    }
}
