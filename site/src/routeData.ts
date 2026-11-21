import { defineRouteMiddleware } from '@astrojs/starlight/route-data';

// Every page says that the site documents the development version: the site is
// published from main (.github/workflows/docs-site.yml), not from a release. Which
// release is current is never written here; the links lead to it.
const DEVELOPMENT_VERSION_NOTE =
	'This site documents the development version of Orcapod (the <code>main</code> branch), ' +
	'which may differ from the latest release. For the releases, see ' +
	'<a href="https://pypi.org/project/orcapod/#history">PyPI</a> and the ' +
	'<a href="https://github.com/metamorphic-labs/orcapod-python/blob/main/CHANGELOG.md">changelog</a>.';

export const onRequest = defineRouteMiddleware((context) => {
	const { entry } = context.locals.starlightRoute;
	entry.data.banner ??= { content: DEVELOPMENT_VERSION_NOTE };
});
