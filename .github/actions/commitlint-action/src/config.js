const CONFIG_SOURCE_URL =
  "https://raw.githubusercontent.com/datalens-tech/datalens/main/.github/workflows/scripts/changelog/changelog_config.json";

export const DEFAULT_TYPE_ENUM = [
  "breaking-change",
  "new-feature",
  "bug-fix",
  "sec",
  "deprecation",
  "dev",
  "tests",
  "ci",
  "chore",
  "build",
  "docs",
];

export const DEFAULT_SCOPE_ENUM = [
  "charts",
  "connectors",
  "navigation",
  "general",
  "dashboards",
  "datasets",
  "auth",
  "optimization",
  "role-model",
  "docs",
  "formula",
];

export const getChangelogConfig = async () => {
  const response = await fetch(CONFIG_SOURCE_URL);
  const data = await response.json();
  return {
    typeEnum: data.section_tags.tags.map((tag) => tag.id),
    scopeEnum: data.component_tags.tags.map((tag) => tag.id),
  };
};

export const getTypeEnum = async () => {
  try {
    const { typeEnum } = await getChangelogConfig();
    return typeEnum;
  } catch (error) {
    console.error(`ERROR::Failed to fetch typeEnum, using default`);
    return DEFAULT_TYPE_ENUM;
  }
};

export const getScopeEnum = async () => {
  try {
    const { scopeEnum } = await getChangelogConfig();
    return scopeEnum;
  } catch (error) {
    console.error(`ERROR::Failed to fetch scopeEnum, using default`);
    return DEFAULT_SCOPE_ENUM;
  }
};
