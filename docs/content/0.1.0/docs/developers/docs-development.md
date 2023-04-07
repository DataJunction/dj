---
weight: 100
---

# Docs Development

The DJ project uses the [Hugo](https://gohugo.io/) framework for building the docs site and all of the pages are defined as
markdown files. The deployed docs site is actually a combination of multiple sites for different versions
of DJ. This page will help to understand how to modify, locally test, and deploy DJ docs.

## Running the Docs Site Locally

Clone the DJ repo locally.

```sh
git clone https://github.com/datajunction/dj
```

Change into the docs directory.

```sh
cd dj/docs
```

Using hugo, start a local server for the specific DJ docs version you want to view.

```sh
hugo serve --contentDir content/0.1.0/
```

{{< hint info >}}
By default, the docs site will be launched at http://localhost:1313/
{{< /hint >}}

In addition to different versions of the docs site, there is a `Versions` page that is deployed as a
small sub-site to the `/versions/` route. This is a connecting page that all versions of the site
link to and lists release notes for each DJ release. The following command will serve the versions page.

```sh
hugo serve --contentDir content/versions/
```

## Updating Content

Content for all pages are stored in reStructuredText files located at `docs/content/`. A sub-directory can
be found for each version of DJ. In order to update content for an existing docs page, open a PR against that page.
In most cases, it's enough to add updates for the latest version, however when necessary, you can additionally make
changes to previous versions of an existing page.

Here are some tips on deciding on whether you should backport changes to previous versions.

* Open a PR against the latest version of a page to...

  * clarify or extend an existing section

  * add a page or section for a new feature

  * add a tutorial

* Open a PR against all versions of a page to...

  * fix broken links

  * annotate feature descriptions with version-specific peculiarities

{{< hint info >}}
The DJ docs site uses a Hugo theme called [hugo-book](https://github.com/alex-shpak/hugo-book). The readme for the
theme includes many configurations that can be used in the [front matter](https://gohugo.io/content-management/front-matter/)
for content pages.
{{< /hint >}}

## Adding New Pages

A new page can be added by creating an `.md` file anywhere within the `docs/content` directory. By default, the
page will appear in the side navigation menu and the title will be the file name converted to a display format. For
example, the content for this page is defined in a file named `docs-development.md` and the titel automatically
appears in the menu as `Docs Development`. The title can also be overriden by setting it explicitly in the pages
front-matter.

*example.md*

```sh
---
title: My Docs Page
---
```

A weight can also be added to position the page in the side menu. Items with higher weights are displayed below items
with lower weights.

```sh
---
title: My Docs Page
weight: 10
---
```

## Deployment

In order to make it easier to build the entire docs-site (including multiple versions) a build script can be found
at `docs/build-docs.sh`. The script takes two positional arguments. You can run the script without any arguments
to see the help output.

```sh
./build-docs.sh
```
*output*
```sh
Build a specified version of the DJ docs

Syntax: ./build-docs.sh <VERSION> <SET_AS_LATEST>

VERSION: The version of the docs site to build, i.e. 0.1.0
SET_AS_LATEST: Additionally build the specified version as the default/latest docs site, i.e. true
```

For example, by running the following you can build the docs for versions `0.1.0`, `0.1.1`, and
`0.1.2`, building `0.1.2` as the latest version that's shown when navigating to the root site.

```sh
./build-docs.sh 0.1.2 true

Start building sites …
Total in 2496 ms

./build-docs.sh 0.1.1 false

Start building sites …
Total in 2472 ms

./build-docs.sh 0.1.0 false

Start building sites …
Total in 2549 ms
```

{{< hint warning >}}
The important thing to note is that only the latest version should have the SET_AS_LATEST flag set to true and
that build should be performed first. This is because the latest site is built at the root and starts by cleaning
out the root directory. Subsequent non-latest builds will happen in sub-directories.
{{< /hint >}}

After the builds are completed, the entire site will be deployed to the `docs/public` directory. Any local server
can be used to render the entire site from the public directory locally such as the popular
[Live Server](https://marketplace.visualstudio.com/items?itemName=ritwickdey.LiveServer) extension for VSCode.