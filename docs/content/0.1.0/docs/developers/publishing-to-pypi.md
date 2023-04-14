---
weight: 100
---

# Publishing to PyPI

The DataJunction project publishes server and client libraries to [PyPI](https://pypi.org/) using [poetry](https://python-poetry.org/).

{{< hint info >}}
To create an API token, go to [PyPI](https://pypi.org/account/login/), navigate to the account settings page,
and scroll to the API tokens section. 
{{< /hint >}}

Configure poetry to use your PyPI API token.
```sh
poetry config pypi-token.pypi $PYPI_API_TOKEN
```

Build and publish the project.
```sh
poetry publish --build
```
{{< hint info >}}
To publish the `djclient`, run the above command in the `djclient/` directory.
{{< /hint >}}
