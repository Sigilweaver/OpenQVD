import { themes as prismThemes } from 'prism-react-renderer';
import type { Config } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
    title: 'OpenQVD',
    tagline: 'Clean-room Rust reader and writer for Qlik QVD files, with Python bindings',
    favicon: 'img/favicon.ico',

    markdown: {
        mermaid: true,
        hooks: {
            onBrokenMarkdownLinks: 'warn',
        },
    },
    plugins: ['docusaurus-plugin-llms-txt'],
    themes: ['@docusaurus/theme-mermaid'],

    url: 'https://sigilweaver.app',
    baseUrl: '/openqvd/docs/',

    organizationName: 'Sigilweaver',
    projectName: 'OpenQVD',

    onBrokenLinks: 'throw',

    i18n: {
        defaultLocale: 'en',
        locales: ['en'],
    },

    presets: [
        [
            'classic',
            {
                docs: {
                    routeBasePath: '/',
                    sidebarPath: './sidebars.ts',
                    editUrl: 'https://github.com/Sigilweaver/OpenQVD/tree/main/docs/',
                },
                blog: false,
                sitemap: {
                    changefreq: 'weekly',
                    priority: 0.5,
                    filename: 'sitemap.xml',
                },
                theme: {
                    customCss: './src/css/custom.css',
                },
            } satisfies Preset.Options,
        ],
    ],

    themeConfig: {
        metadata: [
            { name: 'keywords', content: 'OpenQVD, QVD, Qlik, QlikView, Qlik Sense, data format, parser, Rust, Python, PyArrow, Polars, Pandas, DuckDB' },
            { name: 'description', content: 'OpenQVD is a clean-room Rust reader and writer for Qlik QVD files, with a CLI and Python bindings for PyArrow, Polars, Pandas, and DuckDB.' },
        ],
        colorMode: {
            defaultMode: 'dark',
            disableSwitch: false,
            respectPrefersColorScheme: true,
        },
        navbar: {
            title: 'Sigilweaver',
            logo: {
                alt: 'Sigilweaver logo',
                src: 'img/logo.svg',
                href: 'https://sigilweaver.app',
                target: '_self',
            },
            items: [
                {
                    type: 'dropdown',
                    label: 'Projects',
                    position: 'left',
                    items: [
                        { label: 'OpenQVD', href: 'https://sigilweaver.app/openqvd/docs/' },
                        { label: 'OpenQBW', href: 'https://sigilweaver.app/openqbw/docs/' },
                        { label: 'OpenKSpace', href: 'https://sigilweaver.app/openkspace/docs/' },
                        { label: 'All projects', href: 'https://sigilweaver.app/docs/' },
                    ],
                },
                {
                    href: 'https://github.com/Sigilweaver/OpenQVD',
                    label: 'GitHub',
                    position: 'right',
                },
            ],
        },
        footer: {
            style: 'dark',
            links: [
                {
                    title: 'Project',
                    items: [
                        { label: 'GitHub', href: 'https://github.com/Sigilweaver/OpenQVD' },
                        { label: 'Issues', href: 'https://github.com/Sigilweaver/OpenQVD/issues' },
                        { label: 'crates.io', href: 'https://crates.io/crates/openqvd' },
                        { label: 'PyPI', href: 'https://pypi.org/project/openqvd/' },
                    ],
                },
                {
                    title: 'Related',
                    items: [
                        { label: 'OpenQBW', href: 'https://sigilweaver.app/openqbw/docs/' },
                        { label: 'OpenKSpace', href: 'https://sigilweaver.app/openkspace/docs/' },
                        { label: 'All projects', href: 'https://sigilweaver.app/docs/' },
                    ],
                },
                {
                    title: 'Legal',
                    items: [
                        { label: 'Terms of Use', href: 'https://sigilweaver.app/terms' },
                        { label: 'Privacy Policy', href: 'https://sigilweaver.app/privacy' },
                    ],
                },
            ],
            copyright: `Copyright ${new Date().getFullYear()} Sigilweaver Holdings LLC. OpenQVD is Apache-2.0 licensed; the specification is licensed under <a href="https://creativecommons.org/licenses/by-sa/4.0/" target="_blank" rel="noopener noreferrer">CC-BY-SA 4.0</a>. Qlik, QlikView, and Qlik Sense are trademarks of QlikTech International AB; this project is not affiliated with or endorsed by Qlik.`,
        },
        prism: {
            theme: prismThemes.github,
            darkTheme: prismThemes.dracula,
            additionalLanguages: ['rust', 'toml', 'bash', 'python'],
        },
    } satisfies Preset.ThemeConfig,
};

export default config;
