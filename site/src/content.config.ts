import { defineCollection } from 'astro:content';
import { docsLoader } from '@astrojs/starlight/loaders';
import { docsSchema } from '@astrojs/starlight/schema';

// The pages are generated from ../docs by ../scripts/docs_site_content.py
// (run by the `orcapod-docs-content` integration in astro.config.mjs).
export const collections = {
	docs: defineCollection({ loader: docsLoader(), schema: docsSchema() }),
};
