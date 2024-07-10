import * as config from "./config";

const MOCK_DATA = {
  section_tags: {
    tags: [{ id: "feat" }, { id: "fix" }],
  },
  component_tags: {
    tags: [{ id: "charts" }, { id: "connectors" }],
  },
};

test("getChangelogConfig should return correct data", async () => {
  global.fetch = jest.fn().mockResolvedValue({
    json: jest.fn().mockResolvedValue(MOCK_DATA),
  });

  const result = await config.getChangelogConfig();
  expect(result).toEqual({ typeEnum: ["feat", "fix"], scopeEnum: ["charts", "connectors"] });
});

test("getTypeEnum should return correct data", async () => {
  jest.spyOn(config, "getChangelogConfig").mockResolvedValue(MOCK_DATA);

  const result = await config.getTypeEnum();
  expect(result).toEqual(["feat", "fix"]);
});

test("getScopeEnum should return correct data", async () => {
  jest.spyOn(config, "getChangelogConfig").mockResolvedValue(MOCK_DATA);

  const result = await config.getScopeEnum();
  expect(result).toEqual(["charts", "connectors"]);
});
