---
weight: 100
title: "Releasing New Version"
---

DataJunction project publishes all of its backend services and the Python client library to [PyPI](https://pypi.org/):

- datajunction-clients/python
- datajunction-query
- datajunction-server
- datajunction-reflection (NOT YET IMPLEMENTED)

Javascript client and the UI component go to [NPM](https://www.npmjs.com/):

- datajunction-clients/javascript
- datajunction-ui

JAVA client (TODO):

- datajunction-clients/java

Documentation (TODO).

- docs

### Dev Releases

If you'd like make a dev release of any DataJunction component switch to a corresponding component directory and run:

```sh
make dev-release
```

Look for the version and a target release url in the output. It will differ between the languages we use. 

{{< alert icon="👉" text="Please note that you should not checkin the dev-release version file into the DataJunction repository." />}}

Example from Pyhton client:

```sh
% make dev-release
hatch version dev
Old: 0.0.1a20
New: 0.0.1a20.dev0
hatch build
[sdist]
dist/datajunction-0.0.1a20.dev0.tar.gz

[wheel]
dist/datajunction-0.0.1a20.dev0-py3-none-any.whl
hatch publish
dist/datajunction-0.0.1a20.dev0.tar.gz ... success
dist/datajunction-0.0.1a20.dev0-py3-none-any.whl ... success

[datajunction]
https://pypi.org/project/datajunction/0.0.1a20.dev0/
```

### Official Releases

In order to simplify version management we decided to release all the components together. Releasing all components with the same version carries some challanges with it, but we we'll try to use this approach for some time and reconsider if necessary.

Three steps:

1. To make a new official release for all the components simply run the [Version Bump](https://github.com/DataJunction/dj/actions/workflows/bump-version.yml) Github action with the default settings. The default setting will make sure you are staying with the current version cycle, which at the moment is **ALPHA**.

2. Once the above workflow runs succesfully we should recive a PR for the version updates. This will allow us to double-check the versions before the final release.

3. After the above PR is merged a [Version Release](https://github.com/DataJunction/dj/actions/workflows/bump-version.yml) Github action will build and publish all the versions to its corresponding repositories.