"use strict";

import { RuleConfigSeverity } from "@commitlint/types";

const CONFIG_SOURCE_URL =
  "https://raw.githubusercontent.com/datalens-tech/datalens/main/.github/workflows/scripts/changelog/changelog_config.json";

const DEFAULT_TYPE_ENUM = [
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

const DEFAULT_SCOPE_ENUM = [
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

const getChangelogConfig = async () => {
  const response = await fetch(CONFIG_SOURCE_URL);
  const data = await response.json();

  console.log(`INFO::Received data from config file: ${JSON.stringify(data)}`);

  const typeEnum = data.section_tags.tags.map((tag) => tag.id);
  const scopeEnum = data.component_tags.tags.map((tag) => tag.id);

  return { typeEnum, scopeEnum };
};

const getEnums = async () => {
  try {
    const { typeEnum, scopeEnum } = await getChangelogConfig();
    return { typeEnum, scopeEnum };
  } catch (error) {
    console.error(`ERROR::Failed to fetch config file: ${error}`);
    return { typeEnum: DEFAULT_TYPE_ENUM, scopeEnum: DEFAULT_SCOPE_ENUM };
  }
};

const { typeEnum, scopeEnum } = await getEnums();

const subjectPrefixRule = ({ subject }) => {
  const PREFIX_REGEX = /\W*-\d*/;
  return [subject.match(PREFIX_REGEX), `Subject must start with a prefix like "BI-1234"`];
};

const Configuration = {
  extends: ["@commitlint/config-conventional"],
  rules: {
    "type-enum": [RuleConfigSeverity.Error, "always", typeEnum],
    "scope-enum": [RuleConfigSeverity.Error, "always", scopeEnum],
    "subject-case": [RuleConfigSeverity.Error, "never", ["start-case", "pascal-case", "upper-case"]],
    "subject-prefix": [RuleConfigSeverity.Error, "always"],
  },
  plugins: [
    {
      rules: {
        "subject-prefix": subjectPrefixRule,
      },
    },
  ],
};

export default Configuration;
