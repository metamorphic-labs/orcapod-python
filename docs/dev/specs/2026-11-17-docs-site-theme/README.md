# The documentation site's theme (ITL-1170)

Developer request, November 17, 2026: the documentation site follows orcapod.org for the
official logo, the fonts and the colour scheme, with a bluer theme. The theme itself is
`site/src/styles/orcapod.css`; `site/README.md` ("The theme") describes it.

## What orcapod.org uses

- Logo: `images/iconOutlined.png` (986 x 894, black on transparent, white details) in its
  navigation and `images/orcapodlogofull.png` (icon and wordmark) on its home page; the
  favicon is `favicon.ico` (48 x 48 and 32 x 32). It has no SVG and no dark variant.
- Fonts (SIL Open Font License 1.1): Asap for the text, Lexend Exa bold for the name
  ("orcapod", lower case) in the navigation, Asap Condensed in the footer.
- Colours: background `#fcfafc`, text `#0a0a0a`, navigation and footer bars `#0a0a0a`
  with white text. There is no brand blue and no dark mode.

## The site's theme

The site takes `iconOutlined.png` (header logo, inverted in dark mode) and `favicon.ico`
unmodified, the background and text colours, the name in lower case, and the fonts Asap
and Lexend Exa (bold), bundled from the `@fontsource-variable/asap` and
`@fontsource/lexend-exa` packages (OFL-1.1, allowed for the site by the developer on
November 17, 2026) and served by the site itself. The blue accent
family (hue about 212) is the site's own.

The home page has a hero (developer request, November 17, 2026): Starlight's `splash`
template with the logo, the name, the tagline "A Python library for building highly
reproducible scientific data pipelines." (the first sentence of `docs/index.md`,
shortened) and three buttons, Get started, Tutorial and GitHub. Choices:

- `splash`, as Starlight's own landing pages: the home page has no sidebar and no table
  of contents; at phone width it has no menu button, and the buttons, the search and
  the page's "Next steps" lead on.
- The image is `iconOutlined.png`, the header's logo, inverted in dark mode as there, and
  the title beside it is the name in Lexend Exa bold, lower case: together they read as
  orcapod.org's full logo. The full logo (`orcapodlogofull.png`, 3001 x 3001, 203 KB)
  would repeat the name under the title, and Astro resizes images only with `sharp`,
  which the site does not install (libvips is LGPL-3.0-or-later), so it would be served
  at full size. Astro's passthrough service writes the hero's copy of the 80 KB icon at
  a URL of its own, so the home page loads it twice (160 KB).
- `docs/index.md` is unchanged: `scripts/docs_site_content.py` writes the hero into the
  generated page's front matter (`HOME_HERO`), and resolves its buttons like any link.

| | Light | Dark |
|---|---|---|
| Background | `#fcfafc` | `#0b0f16` |
| Body text | `#2b3440` | `#c8d0da` |
| Strongest text | `#0a0a0a` | `#ffffff` |
| Links (`--sl-color-text-accent`) | `#1565c0` | `#a8d0ff` |
| Accent low / accent / accent high | `#d6e6fa` / `#1565c0` / `#0b3d73` | `#10294a` / `#3d8fe8` / `#a8d0ff` |

WCAG contrast ratios (AA asks 4.5:1 for body text): light body text 12.1:1, links 5.5:1
on the background and 4.9:1 on the inline-code background, banner text 5.5:1; dark body
text 12.3:1, links 12.0:1 on the background and 9.4:1 on the inline-code background,
banner text 9.1:1.

## Screenshots

Taken from a local `pnpm build`, served statically, with headless Chrome at 1280 x 900
(colour scheme set through the DevTools protocol, after `document.fonts.ready`), scaled to
960 x 675. They show the bundled brand fonts.

- `home-light.png`, `home-dark.png`: the home page before the hero: header, banner,
  sidebar, a code block.
- `home-hero-light.png`, `home-hero-dark.png`: the home page with the hero, 1280 wide;
  `home-hero-phone-light.png`, `home-hero-phone-dark.png`: at 390 x 844 (a phone).
- `concept-function-pods-light.png`, `concept-function-pods-dark.png`: code, a caution and
  a note aside.
- `in-development-notice-light.png`, `in-development-notice-dark.png`: the "In
  development" notice.
- `api-operators-light.png`, `api-operators-dark.png`: the generated API reference.
