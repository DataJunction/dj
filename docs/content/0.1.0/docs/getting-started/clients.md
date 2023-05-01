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

The DataJunction javascript client can be installed using [npm](https://www.npmjs.com/).
```sh
npm install datajunction
```
```js
dj = new DJClient("http://localhost:8000")
dj.metrics.get().then(data => console.log(data))
```
