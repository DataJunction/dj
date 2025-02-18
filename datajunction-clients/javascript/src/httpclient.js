export default class HttpClient {
    constructor(options = {}) {
        this._baseURL = options.baseURL || '';
        this._headers = options.headers || {};
        this._cookie = '';
    }

    async _fetchJSON(endpoint, options = {}) {
        const res = await fetch(this._baseURL + endpoint, {
            ...options,
            headers: {
                ...this._headers,
                'Cookie': this._cookie,
            },
            credentials: 'include',
        });

        const setCookieHeader = res.headers.get('Set-Cookie');
        if (setCookieHeader) {
            this._cookie = setCookieHeader;
            this._headers['Cookie'] = this._cookie;
        }

        if (!res.ok) {
            const errorText = await res.text();
            throw new Error(`Request failed: ${res.status} ${errorText}`);
        }

        if (options.parseResponse !== false && res.status !== 204) {
            return res.json();
        }

        return undefined;
    }

    async login(username, password) {
        const body = new URLSearchParams({
            grant_type: 'password',
            username: username,
            password: password,
        });
    
        const response = await fetch(this._baseURL + '/basic/login', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                ...this._headers,
            },
            body: body.toString(),
            credentials: 'include',
        });
    
        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Login failed: ${response.status} ${errorText}`);
        }
    
        const setCookieHeader = response.headers.get('Set-Cookie');
        if (setCookieHeader) {
            this._cookie = setCookieHeader;
            this._headers['Cookie'] = this._cookie;
        }
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
