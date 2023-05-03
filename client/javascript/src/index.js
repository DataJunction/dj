import HttpClient from './httpclient.js'

export class DJClient extends HttpClient {
  constructor (baseURL) {
    super({
      baseURL
    })
  }

  get catalogs () {
    return {
      get: () => this.get('/catalogs/')
    }
  }

  get engines () {
    return {
      get: () => this.get('/engines/')
    }
  }

  get namespaces () {
    return {
      get: () => this.get('/namespaces/')
    }
  }

  get metrics () {
    return {
      get: () => this.get('/metrics/')
    }
  }

  get nodes () {
    return {
      get: () => this.get('/nodes/')
    }
  }

  get tags () {
    return {
      get: () => this.get('/tags/')
    }
  }

  get attributes () {
    return {
      get: () => this.get('/attributes/')
    }
  }
}
