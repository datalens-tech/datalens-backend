"use strict";

import { RuleConfigSeverity } from "@commitlint/types";

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

const subjectPrefixRule = ({ subject }) => {
  const PREFIX_REGEX = /\W*-\d*/;
  return [subject.match(PREFIX_REGEX), `Subject must start with a prefix like "BI-1234"`];
};

const Configuration = {
  extends: ["@commitlint/config-conventional"],
  rules: {
    "type-enum": [RuleConfigSeverity.Error, "always", DEFAULT_TYPE_ENUM],
    "scope-enum": [RuleConfigSeverity.Error, "always", DEFAULT_SCOPE_ENUM],
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
