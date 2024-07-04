import lint from "@commitlint/lint";
import load from "@commitlint/load";
import config from "./commitlint.config.js";

jest.mock("./config", () => ({
  getTypeEnum: jest.fn().mockResolvedValue(["type-test"]),
  getScopeEnum: jest.fn().mockResolvedValue(["scope-test"]),
}));

test("commitlint default message", async () => {
  const { rules, parserPreset, plugins } = await load(config);
  const report = await lint("type-test(scope-test): BI-123 add new feature", rules, {
    plugins: plugins,
    parserOpts: parserPreset.parserOpts,
  });
  expect(report.valid).toBe(true);
});

test("commitlint config should fail if type is not in type enum", async () => {
  const { rules, parserPreset, plugins } = await load(config);
  const report = await lint("type(scope-test): BI-123 add new feature", rules, {
    plugins: plugins,
    parserOpts: parserPreset.parserOpts,
  });
  expect(report.valid).toBe(false);
});

test("commitlint config should fail if scope is not in scope enum", async () => {
  const { rules, parserPreset, plugins } = await load(config);
  const report = await lint("type-test(scope): BI-123 add new feature", rules, {
    plugins: plugins,
    parserOpts: parserPreset.parserOpts,
  });
  expect(report.valid).toBe(false);
});

test("commitlint config should fail if subject is empty", async () => {
  const { rules, parserPreset, plugins } = await load(config);
  const report = await lint("type-test(scope-test):", rules, {
    plugins: plugins,
    parserOpts: parserPreset.parserOpts,
  });
  expect(report.valid).toBe(false);
});

test("commitlint config should fail if subject does not start with a prefix", async () => {
  const { rules, parserPreset, plugins } = await load(config);
  const report = await lint("type-test(scope-test): add new feature", rules, {
    plugins: plugins,
    parserOpts: parserPreset.parserOpts,
  });
  expect(report.valid).toBe(false);
});
