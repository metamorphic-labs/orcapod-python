// @ts-check
import { spawnSync } from 'node:child_process';
import { watch } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { defineConfig, passthroughImageService } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightLinksValidator from 'starlight-links-validator';

const repoRoot = fileURLToPath(new URL('..', import.meta.url));

/**
 * The site's address: DOCS_SITE_URL (origin) and DOCS_SITE_BASE (base path) from the
 * environment, or the defaults (https://metamorphic-labs.github.io/orcapod-python/).
 * The rule is site_settings in scripts/docs_site_content.py; a value it refuses fails
 * the build.
 */
function siteSettings() {
	const result = spawnSync(
		'uv',
		['run', 'python', 'scripts/docs_site_content.py', '--site-settings'],
		{ cwd: repoRoot, encoding: 'utf-8', stdio: ['ignore', 'pipe', 'inherit'] },
	);
	if (result.status !== 0) {
		throw new Error('invalid site address (DOCS_SITE_URL, DOCS_SITE_BASE; scripts/docs_site_content.py)');
	}
	return JSON.parse(result.stdout);
}

const { site, base } = siteSettings();

/** Write src/content/docs and public from ../docs (pages and generated API reference). */
function buildContent() {
	const result = spawnSync(
		'uv',
		['run', 'python', 'scripts/docs_site_content.py', '--base', base],
		{ cwd: repoRoot, stdio: 'inherit' },
	);
	if (result.status !== 0) {
		throw new Error('docs content build failed (scripts/docs_site_content.py)');
	}
}

/** Runs the content build before `astro build` and `astro dev`; in dev, again on every change under docs/ and src/. */
function orcapodDocsContent() {
	return {
		name: 'orcapod-docs-content',
		hooks: {
			'astro:config:setup': ({ command }) => {
				if (command === 'build' || command === 'dev') buildContent();
			},
			'astro:server:setup': ({ logger }) => {
				let timer;
				const rebuild = () => {
					clearTimeout(timer);
					timer = setTimeout(() => {
						try {
							buildContent();
						} catch (error) {
							logger.error(String(error));
						}
					}, 200);
				};
				for (const dir of ['docs', 'src/orcapod']) {
					watch(new URL(`../${dir}`, import.meta.url), { recursive: true }, rebuild);
				}
			},
		},
	};
}

const page = (label, slug) => ({ label, slug });

export default defineConfig({
	site,
	base,
	// mkdocs rendered '--' and quotes as written; so does the site.
	markdown: { smartypants: false },
	// sharp is not installed (pnpm-workspace.yaml): images are served as they are.
	image: { service: passthroughImageService() },
	integrations: [
		orcapodDocsContent(),
		starlight({
			title: 'Orcapod',
			description:
				'Intuitive and powerful library for highly reproducible scientific data pipelines',
			// orcapod.org's logo and favicon, unmodified (src/assets). The content build
			// writes the favicon into public/ (SITE_PUBLIC_FILES in docs_site_content.py).
			logo: { src: './src/assets/orcapod-icon.png' },
			favicon: '/favicon.ico',
			// The brand fonts, bundled with the site (OFL-1.1; served from the site itself,
			// never from a font host): Asap for the text, Lexend Exa bold for the name.
			// Then the colour scheme (src/styles/orcapod.css).
			customCss: [
				'@fontsource-variable/asap',
				'@fontsource/lexend-exa/700.css',
				'./src/styles/orcapod.css',
			],
			plugins: [starlightLinksValidator()],
			// The banner on every page: the site documents main, not a release.
			routeMiddleware: './src/routeData.ts',
			sidebar: [
				{ label: 'Home', link: '/' },
				page('Getting Started', 'getting-started'),
				{
					label: 'Tutorial',
					items: [
						page('Overview', 'tutorial'),
						page('0. Setup', 'tutorial/00-setup'),
						page('1. Your first stream', 'tutorial/01-first-stream'),
						page('2. Computing with function pods', 'tutorial/02-function-pods'),
						page('3. Reshaping with operators', 'tutorial/03-operators'),
						page('4. Many rows into one', 'tutorial/04-many-rows-into-one'),
						page('5. Keeping results', 'tutorial/05-pipeline-job'),
						page('6. Re-runs and caching', 'tutorial/06-reruns-and-caching'),
						page('7. Working with files', 'tutorial/07-files'),
						page('8. Inspecting and tracing', 'tutorial/08-inspecting-and-tracing'),
						page('9. When things go wrong', 'tutorial/09-when-things-go-wrong'),
						page('10. Where next', 'tutorial/10-where-next'),
					],
				},
				{
					label: 'Concepts',
					items: [
						page('Sources', 'concepts/sources'),
						page('Streams', 'concepts/streams'),
						page('Operators', 'concepts/operators'),
						page('Function Pods', 'concepts/function-pods'),
						page('Data-tags', 'concepts/data-tags'),
						page('Pipelines and Jobs', 'concepts/pipeline-job'),
						page('Provenance', 'concepts/provenance'),
						page('Working in Notebooks', 'concepts/notebooks'),
						page('Your Own Data Types', 'concepts/your-own-data-types'),
						page('Arrays and Tensors', 'concepts/arrays'),
						page('Logical Type Identifiers', 'concepts/logical-type-identifiers'),
						page('Identity & Hashing', 'concepts/identity'),
						page('File and Path Types', 'concepts/file-types'),
						page('File Hash Caching', 'concepts/file-hash-caching'),
					],
				},
				{
					label: 'API Reference',
					items: [
						page('Overview', 'api'),
						page('Sources', 'api/sources'),
						page('Streams', 'api/streams'),
						page('Operators', 'api/operators'),
						page('Function Pods', 'api/function-pods'),
						page('Databases', 'api/databases'),
						page('Nodes', 'api/nodes'),
						page('Pipeline', 'api/pipeline'),
						page('Types', 'api/types'),
					],
				},
				{
					label: 'Reference',
					items: [
						page('Hashing', 'reference/hashing'),
						page('Golden hashes', 'reference/golden-hashes'),
						page('Data function types', 'reference/data-function-types'),
						page('Data types per database', 'reference/database-types'),
					],
				},
				page('Troubleshooting', 'troubleshooting'),
			],
		}),
	],
});
