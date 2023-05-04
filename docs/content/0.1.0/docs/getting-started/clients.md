---
weight: 5
---

# Clients

## Python

The DataJunction python client can be installed using [pip](https://pip.pypa.io/en/stable/).

```sh
pip install datajunction
```

```py
from datajunction import DJClient
dj = DJClient("http://localhost:8000")
print(dj.metrics())
```

## Javascript

The DataJunction javascript client can be installed using [npm](https://www.npmjs.com/) for use in a node project
or using the [UNPKG](https://www.unpkg.com/) CDN for client-side use.

{{< tabs "javascript client" >}}
{{< tab "CommonJS" >}}
```sh
npm install datajunction
```
```js
const { DJClient } = require('datajunction')

const dj = new DJClient('http://localhost:8000')
dj.metrics.get().then(data => console.log(data))
```
{{< /tab >}}
{{< tab "ES6" >}}
```html
<script src="https://unpkg.com/datajunction/dist/datajunction.js"></script>
<script>
    const dj = new window.datajunction.DJClient("http://localhost:8000");
    dj.metrics.get().then(data => console.log(data));
</script>
```
{{< /tab >}}
{{< /tabs >}}