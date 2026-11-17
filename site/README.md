# The Orcapod documentation site

An [Astro Starlight](https://starlight.astro.build) site, built from `../docs/` and the
source code. It is published from `main` at
<https://metamorphic-labs.github.io/orcapod-python/>, the default address (see [The site's address](#the-sites-address)).

## Build and preview

You need Node (the version in `engines.node` of `package.json`), pnpm (the version in
`packageManager`) and [uv](https://docs.astral.sh/uv/), with the repository's Python
environment (`uv sync --dev` at the repository root).

```sh
cd site
pnpm install --frozen-lockfile   # once, and after package.json or the lockfile changes
pnpm build                       # build the site into site/dist
pnpm preview                     # serve site/dist at http://localhost:4321<base>, by default /orcapod-python/
pnpm dev                         # live preview while editing docs/ or the source
```

`pnpm build` fails on a broken link or anchor, a page that is neither published nor
excluded, an object an API page names that does not exist, or a docstring problem.
Search works in `pnpm preview`, not in `pnpm dev`.

Only one preview server runs per project. If `pnpm preview` reports that another one is
already running, use that one (`pnpm astro preview status` names its address), stop it
with `pnpm astro preview stop` (or Ctrl-C in its terminal), or start a second one on
another port with `pnpm preview --port 4322 --ignore-lock`.

## How the site is made

`docs/` is the only source of the pages: the documentation tests execute its samples as
they are. The site holds no copy in git. Before every `astro build` and `astro dev`, the
`orcapod-docs-content` integration in `astro.config.mjs` runs
`../scripts/docs_site_content.py`, which writes `src/content/docs/` and `public/` (both
ignored by git) from `docs/`:

- the `# <title>` first line of a page becomes Starlight's `title` front matter;
- an admonition (`!!! note "Title"`) becomes a Starlight aside, `warning` becoming
  `caution`; a collapsible one (`??? success "Solution"`) becomes a `<details>` block;
- a relative link to a page becomes the page's URL, an mkdocs-style anchor the anchor
  Starlight gives the same heading, and a link to another file under `docs/` copies the
  file into the site;
- the `<!-- docs-test: skip -->` markers of the sample tests are removed.

Which parts of `docs/` are published is decided in that script (`PUBLISHED_FILES`,
`PUBLISHED_FOLDERS`, `EXCLUDED`); `docs/dev/` is never published.

### The API reference

The API pages are generated from the source on every build by
`../scripts/docs_site_api.py`, with [griffe](https://mkdocstrings.github.io/griffe/)
(static analysis: the package is not imported). Each `docs/api/*.md` file is the
generator's input: its title, an introduction, and one `::: <object path>` line per
object to document. Each such line becomes the object's signature, its Google-style
docstring (sections rendered, notes and warnings as asides), its base classes, and its
public documented members in source order. Every object has the anchor of its path
(`#orcapod.core.operators.Join`), and a type in a parameter list links to the object's
reference when that object is documented.

Planned with the v0.4 content update (ITL-1170): API pages generated from each public
module's `__all__` instead of these lists.

## The site's address

The site's origin and base path are taken from two environment variables, each used only
when it is set and not empty: `DOCS_SITE_URL`, the origin (`https://docs.example.org`; an
`http` or `https` URL with no path), and `DOCS_SITE_BASE`, the base path (`/`,
`/orcapod-python/`; a leading and a trailing `/` are added where missing). Unset or empty,
they are `https://metamorphic-labs.github.io` and `/orcapod-python/`. One place decides
them, `site_settings` in `../scripts/docs_site_content.py`: `astro.config.mjs` reads them
with `--site-settings` and passes the base back to the content build, and a value that
script refuses (a base with a scheme, a query, `..` or whitespace; a site URL that is not
`http` or `https`, or that has a path) fails the build.

```sh
DOCS_SITE_URL=https://docs.example.org DOCS_SITE_BASE=/ pnpm build
```

In CI, the build step takes them from the repository variables of the same names
(Settings > Secrets and variables > Actions > Variables).

A custom domain is set in two places, both repository settings:

1. the variables: `DOCS_SITE_URL=https://<domain>` and `DOCS_SITE_BASE=/`;
2. Settings > Pages > Custom domain: `<domain>`, with the DNS record GitHub asks for
   (a `CNAME` to `metamorphic-labs.github.io` for a subdomain). A site deployed by
   GitHub Actions needs no `CNAME` file.

Then run the deploy by hand (see [CI and publishing](#ci-and-publishing)) so that the
site's links use the new address.

## What is committed

`package.json`, `pnpm-lock.yaml` (exact versions), `pnpm-workspace.yaml`,
`astro.config.mjs` (site settings, sidebar), `src/content.config.ts`,
`src/routeData.ts` (the banner every page shows) and this file. `node_modules/`, `dist/`, `.astro/`, `src/content/docs/` and `public/` are
generated.

Astro's optional image optimizer, `sharp`, is not installed (`pnpm-workspace.yaml`): the
site has no images, and its `libvips` is under a license the project does not allow.

## CI and publishing

`.github/workflows/docs-site.yml` builds the site on every pull request and push to `main`,
and checks the npm packages' licenses (`../scripts/check_site_licenses.py`).

The same workflow publishes the site to GitHub Pages. Its `deploy` job
([`actions/deploy-pages`](https://github.com/actions/deploy-pages)) runs after the build:

- on every push to `main`;
- by hand: Actions > Documentation site > Run workflow, with the branch `main` (or
  `gh workflow run docs-site.yml --ref main`);
- never on a pull request, from another branch, or in a fork.

Only a run that deploys uploads the built site (`site/dist`) as the Pages artifact. The
job deploys in the `github-pages` environment and needs Settings > Pages > Source =
GitHub Actions. Two deploys never run at once, and a running one is never cancelled.

The site documents the development version: what is on `main`, not the latest release.
Every page says so in a banner (`src/routeData.ts`), which links to PyPI and the changelog
and never names a release.
