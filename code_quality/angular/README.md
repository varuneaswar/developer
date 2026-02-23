# Angular Code Quality — Future Plan

Angular support is **planned** for a future iteration of this module.

## Intended tools

| Tool       | Purpose                                   |
|------------|-------------------------------------------|
| ESLint     | TypeScript / Angular-specific linting     |
| Prettier   | Code formatting                           |
| stylelint  | CSS / SCSS style linting                  |
| audit      | `npm audit` for dependency vulnerabilities|

## Planned structure

```
code_quality/angular/
├── .eslintrc.json      ← Angular ESLint ruleset
├── .prettierrc         ← Prettier config
└── .stylelintrc.json   ← stylelint config
```

## Integration approach

- Add `lint`, `format:check` scripts to `package.json`.
- GitHub Actions: a new workflow `code_quality_angular.yml` following the same
  pattern as `code_quality_python.yml`.
- Pre-commit hooks: use `pre-commit` with `language: node` hooks or rely on
  `husky` + `lint-staged`.
