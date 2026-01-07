# openapi-gen

Generate Effect http clients from openapi specification.

# Develop

Install:
```sh
pnpm install
```

Build:
```sh
pnpm build
```

Generate sample client based on `./openapi.json`:
```sh
pnpm sample-gen
```

# Publish

Create a changeset:
```sh
pnpm changeset
```

Increment `package.json` version:
```sh
pnpm changeset version
```

Publish: commit to repo