# Java Code Quality — Future Plan

Java support is **planned** for a future iteration of this module.

## Intended tools

| Tool        | Purpose                          |
|-------------|----------------------------------|
| Checkstyle  | Code style enforcement           |
| SpotBugs    | Static bug analysis              |
| PMD         | Copy-paste & bad-practice checks |
| OWASP DC    | Dependency vulnerability scan    |

## Planned structure

```
code_quality/java/
├── checkstyle.xml      ← Google / Sun style ruleset
├── spotbugs-exclude.xml
└── pmd-ruleset.xml
```

## Integration approach

- Maven: configure plugins in `pom.xml` (or a shared parent POM).
- Gradle: configure in `build.gradle` using the relevant plugins.
- GitHub Actions: a new workflow `code_quality_java.yml` following the same
  pattern as `code_quality_python.yml`.
