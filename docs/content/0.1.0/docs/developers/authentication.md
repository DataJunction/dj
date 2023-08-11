---
weight: 10
title: "Authentication"
---

The open-source DJ project comes with support for multiple forms of authentication. Which forms of authentication are
enabled is determined by the server configuration.

|OAuth Provider|Server Configuration Variables|
|---|---|
|Basic|`SECRET`|
|GitHub|`SECRET`, `GITHUB_OAUTH_CLIENT_ID`, `GITHUB_OAUTH_CLIENT_SECRET`|

## Basic

To enable basic authentication, you simply need to set `SECRET` in the server configuration. The value
must be a 16-bit string. The secret will be used for hashing and verifying user passwords as well as encrypting
and decrypting JWT cookies.

## GitHub

To enable the GitHub OAuth flow, you need to set `SECRET`, `GITHUB_OAUTH_CLIENT_ID`, and `GITHUB_OAUTH_CLIENT_SECRET`
in the server configuration.

- `SECRET`: Used for encrypting and decrypting JWT cookies
- `GITHUB_OAUTH_CLIENT_ID`: The client ID for the OAuth application registered with GitHub
- `GITHUB_OAUTH_CLIENT_SECRET`: A secret key for the OAuth application registered with GitHub

To learn more about creating a GitHub OAuth app, see the
[Creating an OAuth app](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/creating-an-oauth-app)
page in the GitHub documentation.

## Disabling Authentication

{{< alert icon="❗" >}}
Disabling authentication is useful for demo and testing environments, however it's not recommended to run a
production deployment with authentication disabled.
{{< /alert >}}

To disable authentication entirely, simply exclude all auth-related variables from the server configuration.
All endpoints will no longer be fronted by any authentication middleware and the username attached to all user
activity will be `unknown`.