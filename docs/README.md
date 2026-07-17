# OpenQVD docs site

[Docusaurus](https://docusaurus.io/) site for OpenQVD, deploying to
<https://sigilweaver.app/openqvd/docs/> via Cloudflare Workers.

## Develop

```sh
bun install
bun run dev          # http://localhost:25820/openqvd/docs/
```

## Build and deploy

Cloudflare deploys automatically via the GitHub App on push to `main`.
To verify the build locally:

```sh
bun run build:cloudflare
```
