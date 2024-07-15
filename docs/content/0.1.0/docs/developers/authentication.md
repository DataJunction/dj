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

## How DataJunction Stores Users

The metadata database for the core DataJunction service contains a `users` table that stores user information such as
`username` and `email`. This table is used for all authorization related behavior such as storing which users have
authored a node and setting access controls. As the authorization features continue to mature, the core service will
rely even more on this table and so the service leads with an assumption that all authenticated users can be found in
this table.

When using the open source basic auth or google OAuth implementation, those implementations ensure that authenticated
users are stored in the `users` table. However, custom auth implementations that override the `get_current_user`
dependency may or may not directly store the user. To handle the scenario where it does not directly store the user,
the dependency chain ends with a `get_and_update_current_user` method that will upsert the authenticated user. This
means that if your custom `get_current_user` implementation relies on an entirely separate authentication service that
holds your user information, it's completely optional to have it additionally handle creating the user record in
DataJunction since the core DataJunction service will handle that.
