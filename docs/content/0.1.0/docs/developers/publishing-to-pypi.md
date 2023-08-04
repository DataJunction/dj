---
weight: 100
title: "Publishing to PyPI"
---

The DataJunction project publishes server and client libraries to [PyPI](https://pypi.org/) using [poetry](https://python-poetry.org/).

{{< alert icon="👉" >}}
To create an API token, go to [PyPI](https://pypi.org/account/login/), navigate to the account settings page,
and scroll to the API tokens section.
{{< /alert >}}

Configure poetry to use your PyPI API token.
```sh
poetry config pypi-token.pypi $PYPI_API_TOKEN
```

Build and publish the project.
```sh
poetry publish --build
```
{{< alert icon="👉" >}}
To publish the `djclient`, run the above command in the `djclient/` directory.
{{< /alert >}}
