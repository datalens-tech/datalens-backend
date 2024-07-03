import { getInput, info, setFailed } from "@actions/core";
import { lint, formatResult } from "./lint.js";

async function run(): Promise<void> {
  const input = getInput("input");
  const configFile = getInput("commitlintConfigFile");

  info(`🔎 Checking if input(${input}) meets the requirements ...`);
  info(`📄 Using commitlint config file: ${configFile}`);

  try {
    const lintResult = await lint(input, configFile);
    if (!lintResult.valid) {
      setFailed(`\\n ${formatResult(lintResult)}`);
    } else {
      info(`✔️ All good`);
    }
  } catch (error) {
    setFailed(error as Error);
  }
}

run();
