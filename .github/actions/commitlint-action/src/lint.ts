import commitlintLoad from "@commitlint/load";
import commitlintLint from "@commitlint/lint";
import type { LintOutcome, LintOptions } from "@commitlint/types";
import commitlintFormat from "@commitlint/format";

export async function lint(
  message: string,
  configFile: string,
): Promise<LintOutcome> {
  const config = await commitlintLoad({}, { file: configFile });

  return commitlintLint(
    message,
    config.rules,
    config.parserPreset
      ? ({ parserOpts: config.parserPreset.parserOpts } as LintOptions)
      : ({} as LintOptions),
  );
}

export function formatResult(lintResult: LintOutcome): string {
  return commitlintFormat(
    {
      results: [
        {
          warnings: lintResult.warnings,
          errors: lintResult.errors,
          input: lintResult.input,
        },
      ],
    },
    {},
  );
}
