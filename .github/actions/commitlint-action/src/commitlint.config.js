import { RuleConfigSeverity } from "@commitlint/types";
import { getScopeEnum, getTypeEnum } from "./config.js";

const subjectPrefixRule = (data) => {
  if (!data.subject) {
    return [false, "Subject must not be empty"];
  }
  const PREFIX_REGEX = /^[A-Z]+-\d+/;
  return [data.subject.match(PREFIX_REGEX), `Subject must start with a prefix like "BI-1234"`];
};

const Configuration = {
  extends: ["@commitlint/config-conventional"],
  rules: {
    "type-enum": async () => {
      return [RuleConfigSeverity.Error, "always", await getTypeEnum()];
    },
    "scope-enum": async () => {
      return [RuleConfigSeverity.Error, "always", await getScopeEnum()];
    },
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
  parserPreset: {
    parserOpts: {
      headerPattern: /^([a-z\-]+)(?:\(([^)]+)\))?: (.+)$/,
      headerCorrespondence: ["type", "scope", "subject"],
    },
  },
};

export default Configuration;
