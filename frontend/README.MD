# Content Atlas Frontend (Development)

<div align="center" style="margin: 30px;">
    <a href="https://refine.dev">
    <img alt="refine logo" src="https://refine.ams3.cdn.digitaloceanspaces.com/readme/refine-readme-banner.png">
    </a>
</div>
<br/>

This is the **development version** of the Content Atlas frontend. Changes made here are automatically synced to the deployment repository.

## Repository Separation

- **Development**: This repository (monorepo)
- **Deployment**: [content-atlas-frontend](https://github.com/thefoundry-app/content-atlas-frontend)
- **Sync**: Automatic via GitHub Actions on push to `main` branch

## Getting Started

A React Framework for building internal tools, admin panels, dashboards & B2B apps with unmatched flexibility ✨

Refine's hooks and components simplifies the development process and eliminates the repetitive tasks by providing industry-standard solutions for crucial aspects of a project, including authentication, access control, routing, networking, state management, and i18n.

## Available Scripts

### Running the development server.

```bash
    npm run dev
```

### Building for production.

```bash
    npm run build
```

### Running the production server.

```bash
    npm run start
```

### Linting and type-checking.

```bash
    npm run lint        # ESLint over TS/TSX
    npm run typecheck   # TypeScript without emitting
    npm test            # runs lint then typecheck
```

## Environment Variables

- `VITE_API_URL`: URL of the Content Atlas API (default: http://localhost:8000)

## Deployment

This frontend is automatically deployed when changes are pushed to the `main` branch. The deployment repository contains the production-ready build.

## Learn More

To learn more about **Refine**, please check out the [Documentation](https://refine.dev/docs)

- **REST Data Provider** [Docs](https://refine.dev/docs/core/providers/data-provider/#overview)
- **Ant Design** [Docs](https://refine.dev/docs/ui-frameworks/antd/tutorial/)
- **React Router** [Docs](https://refine.dev/docs/core/providers/router-provider/)
- **Custom Auth Provider** [Docs](https://refine.dev/docs/core/providers/auth-provider/)

## License

MIT
