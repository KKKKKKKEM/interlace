import { test, expect } from "@playwright/test";

/** 从项目和定义列表进入示例图，验证管理层级与工作台衔接。 */
async function openExample(page) {
  await page.goto("/");
  await page.getByRole("link", { name: "默认项目", exact: true }).click();
  await page.getByRole("link", { name: "text.pipeline", exact: true }).click();
  await page.getByRole("tab", { name: "图编排", exact: true }).click();
  await expect(page.locator(".graph-node")).toHaveCount(3);
}

test("工作台导航、缩放和运行面板保持可操作的空间", async ({ page }) => {
  await page.setViewportSize({ width: 1366, height: 768 });
  await openExample(page);
  await expect(page.locator(".graph-node")).toHaveCount(3);
  const canvas = page.getByTestId("graph-canvas");
  const compact = await canvas.boundingBox();
  await page.getByRole("button", { name: "展开导航", exact: true }).click();
  await expect(page.getByRole("combobox", { name: "切换项目" })).toBeVisible();
  const expanded = await canvas.boundingBox();
  expect(compact.width - expanded.width).toBeGreaterThan(100);
  await page.getByRole("button", { name: "收起导航", exact: true }).click();
  await expect(
    page
      .locator(".definition-heading")
      .getByRole("button", { name: "运行", exact: true }),
  ).toBeVisible();
  const zoom = await page.locator(".zoom-value").innerText();
  await page.getByRole("button", { name: "缩小画布" }).click();
  await expect(page.locator(".zoom-value")).not.toHaveText(zoom);
  await page.getByRole("button", { name: "适应画布" }).click();
  await page.getByRole("button", { name: "运行", exact: true }).click();
  await page
    .getByRole("dialog")
    .locator("textarea")
    .first()
    .fill('{"text":"panel check"}');
  await page.getByRole("button", { name: "开始运行" }).click();
  await expect(page.locator(".active-run")).toContainText("已完成");
  await page.locator(".graph-node").first().click();
  const inspector = await page.locator(".inspector").boundingBox();
  expect(inspector.y + inspector.height).toBeCloseTo(768, 0);
  const panel = page.locator(".execution-panel");
  const initial = await panel.boundingBox();
  const handle = page.getByRole("separator", { name: "调整运行面板高度" });
  await handle.focus();
  await page.keyboard.press("ArrowDown");
  await expect(handle).toHaveAttribute("aria-valuenow", "236");
  expect((await panel.boundingBox()).height).toBeLessThan(initial.height);
  const grip = await handle.boundingBox();
  await page.mouse.move(grip.x + grip.width / 2, grip.y + grip.height / 2);
  await page.mouse.down();
  await page.mouse.move(grip.x + grip.width / 2, grip.y + grip.height / 2 - 30);
  await page.mouse.up();
  await expect(handle).toHaveAttribute("aria-valuenow", "266");
  const before = await canvas.boundingBox();
  await page.getByRole("button", { name: "收起运行面板" }).click();
  expect((await canvas.boundingBox()).height - before.height).toBeGreaterThan(
    150,
  );
  await page.getByRole("tab", { name: /^终端输出/ }).click();
  await expect(page.locator(".panel-body")).toBeVisible();
  await expect(page.locator(".output-row")).toHaveCount(1);
  await handle.focus();
  await page.keyboard.press("End");
  const maximum = Number(await handle.getAttribute("aria-valuemax"));
  expect((await panel.boundingBox()).height).toBe(maximum);
  await page.keyboard.press("ArrowDown");
  expect((await panel.boundingBox()).height).toBe(maximum - 20);
  await page.screenshot({ path: "test-results/workbench-modern-laptop.png" });
});

test("节点详情按触发次数显示参数、输出和日志", async ({ page }) => {
  await openExample(page);
  await expect(
    page.getByRole("heading", { name: "text.pipeline" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "运行", exact: true }).click();
  await page
    .getByText("入口数据", { exact: true })
    .locator("..")
    .locator("textarea")
    .fill(JSON.stringify({ text: "first\nsecond" }));
  await page.getByText("执行选项", { exact: true }).click();
  await page
    .getByText("领域配置", { exact: true })
    .locator("..")
    .locator("textarea")
    .fill(JSON.stringify({ test: { tag: "node-details" } }));
  await page.getByRole("button", { name: "开始运行" }).click();
  await expect(page.locator(".active-run")).toContainText("已完成");
  await page.locator(".panel-body tbody tr").nth(1).click();
  const detail = page.locator(".run-inspector");
  await expect(detail).toContainText('"text": "first"');
  await expect(detail).toContainText("node-details");
  await page.getByLabel("节点触发", { exact: true }).selectOption("4");
  await expect(detail).toContainText('"text": "second"');
  await page.getByRole("tab", { name: "输出", exact: true }).click();
  await expect(detail).toContainText('"SECOND"');
  await page.getByRole("tab", { name: "日志", exact: true }).click();
  await expect(detail).toContainText("开始处理文本：second");
  await expect(detail.locator(".log-entry")).toHaveCount(2);
  await page.getByLabel("搜索节点日志").fill("完成");
  await expect(detail.locator(".log-entry")).toHaveCount(1);
  await page.getByLabel("搜索节点日志").fill("");
  await page.screenshot({
    path: "test-results/desktop-node-details.png",
    fullPage: true,
  });
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(detail).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await page.screenshot({
    path: "test-results/mobile-node-details.png",
    fullPage: true,
  });
});

test("节点超时详情显示异常堆栈", async ({ page }) => {
  await openExample(page);
  await expect(
    page.getByRole("heading", { name: "text.pipeline" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "运行", exact: true }).click();
  await page
    .getByText("入口数据", { exact: true })
    .locator("..")
    .locator("textarea")
    .fill(JSON.stringify({ text: "timeout" }));
  await page.getByText("执行选项", { exact: true }).click();
  await page
    .getByText("时限（秒）", { exact: true })
    .locator("..")
    .locator("input")
    .fill("0.15");
  await page.getByRole("button", { name: "开始运行" }).click();
  await expect(page.locator(".active-run")).toContainText("已超时");
  await page.locator(".panel-body tbody tr").nth(1).click();
  await page.getByRole("tab", { name: "异常", exact: true }).click();
  await expect(page.locator(".run-inspector")).toContainText(
    "ExecutionTimeoutError",
  );
  await expect(page.locator(".run-inspector pre")).toContainText("Traceback");
});

test("代码图调用、运行时间线和输出", async ({ page }) => {
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await openExample(page);
  await expect(
    page.getByRole("heading", { name: "text.pipeline" }),
  ).toBeVisible();
  await expect(page.locator(".graph-node")).toHaveCount(3);
  await page.getByRole("button", { name: "运行", exact: true }).click();
  await page
    .getByText("入口数据", { exact: true })
    .locator("..")
    .locator("textarea")
    .fill(JSON.stringify({ text: "interlace\nworkflow" }));
  await page.getByRole("button", { name: "开始运行" }).click();
  await expect(page.locator(".active-run")).toContainText("已完成", {
    timeout: 10000,
  });
  await expect(page.locator(".panel-body tbody tr")).toHaveCount(5);
  await page.getByRole("tab", { name: "终端输出" }).click();
  await expect(page.locator(".output-list")).toContainText("READY: INTERLACE");
  await expect(page.locator(".output-list")).toContainText("READY: WORKFLOW");
  await page.screenshot({
    path: "test-results/desktop-runtime.png",
    fullPage: true,
  });
  expect(errors).toEqual([]);
});

test("画布建图、配置、连线、发布与重新编辑", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("link", { name: "默认项目", exact: true }).click();
  await page.getByRole("button", { name: "新建定义", exact: true }).click();
  await page.getByRole("dialog").locator("input").fill("visual-test");
  await page.getByRole("button", { name: "创建定义", exact: true }).click();
  await page
    .getByRole("button", { name: "添加节点", exact: true })
    .first()
    .click();
  await page.getByRole("button", { name: "文本转换 text.transform" }).click();
  await page
    .getByText("文本前缀", { exact: true })
    .locator("..")
    .locator("input")
    .fill("FIRST: ");
  await page.getByRole("button", { name: "应用配置" }).click();
  await expect(page.getByRole("status")).toContainText("节点配置已应用");
  await page.getByRole("button", { name: "关闭节点详情" }).click();
  await page
    .getByRole("button", { name: "添加节点", exact: true })
    .first()
    .click();
  await page.getByRole("button", { name: "文本转换 text.transform" }).click();
  await page.getByRole("button", { name: "关闭节点详情" }).click();
  const source = page.locator(
    '[data-id="transform"] .react-flow__handle.source',
  );
  const target = page.locator(
    '[data-id="transform_2"] .react-flow__handle.target',
  );
  await expect(source).toBeVisible();
  await expect(target).toBeVisible();
  await page.getByRole("button", { name: "适应画布" }).click();
  await source.dragTo(target);
  await expect(page.locator(".react-flow__edge")).toHaveCount(1);
  await page.getByRole("button", { name: "保存草稿" }).click();
  await expect(page.getByRole("status")).toContainText("草稿已保存");
  await page.getByRole("button", { name: "校验图" }).click();
  await expect(page.getByRole("status")).toContainText("图校验通过");
  await page.getByRole("button", { name: "发布", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "visual-test", exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "运行", exact: true }).click();
  await page
    .getByText("入口数据", { exact: true })
    .locator("..")
    .locator("textarea")
    .fill('{"text":"hello"}');
  await page.getByRole("button", { name: "开始运行" }).click();
  await expect(page.locator(".active-run")).toContainText("已完成");
  await page.getByRole("tab", { name: "终端输出" }).click();
  await expect(page.locator(".output-list")).toContainText("FIRST: HELLO");
  await page.getByRole("button", { name: "编辑草稿" }).click();
  await expect(
    page.getByRole("heading", { name: "visual-test", exact: true }),
  ).toBeVisible();
  await expect(page.locator(".graph-node")).toHaveCount(2);
  await page.screenshot({
    path: "test-results/desktop-editor.png",
    fullPage: true,
  });
  const point = await page
    .locator(".react-flow__edge-interaction")
    .evaluate((path) => {
      const p = path.getPointAtLength(path.getTotalLength() / 2);
      return new DOMPoint(p.x, p.y)
        .matrixTransform(path.getScreenCTM())
        .toJSON();
    });
  await page.mouse.click(point.x, point.y);
  await page.getByRole("button", { name: "删除连线" }).click();
  await expect(page.locator(".react-flow__edge")).toHaveCount(0);
  await page.getByRole("button", { name: "撤销", exact: true }).click();
  await expect(page.locator(".react-flow__edge")).toHaveCount(1);
});

test("对话框报告输入错误并支持取消运行", async ({ page }) => {
  await openExample(page);
  await expect(
    page.getByRole("heading", { name: "text.pipeline" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "运行", exact: true }).click();
  const input = page
    .getByText("入口数据", { exact: true })
    .locator("..")
    .locator("textarea");
  await input.fill("{");
  await page.getByRole("button", { name: "开始运行" }).click();
  await expect(page.getByRole("dialog").getByRole("alert")).toBeVisible();
  await input.fill(
    JSON.stringify({ text: Array(20).fill("cancel").join("\n") }),
  );
  await page.getByRole("button", { name: "开始运行" }).click();
  await page.getByRole("button", { name: "取消执行", exact: true }).click();
  await expect(page.locator(".active-run")).toContainText("已取消");
});

test("移动端画布和运行控件不溢出", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await openExample(page);
  await expect(
    page.getByRole("heading", { name: "text.pipeline" }),
  ).toBeVisible();
  await expect(page.locator(".graph-node")).toHaveCount(3);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await page.getByRole("button", { name: "运行", exact: true }).click();
  await page
    .getByText("入口数据", { exact: true })
    .locator("..")
    .locator("textarea")
    .fill('{"text":"mobile"}');
  await page.getByRole("button", { name: "开始运行" }).click();
  await expect(page.locator(".active-run")).toContainText("已完成");
  await page.getByRole("tab", { name: "终端输出" }).click();
  await expect(page.locator(".output-list")).toContainText("READY: MOBILE");
  await page.screenshot({
    path: "test-results/mobile-runtime.png",
    fullPage: true,
  });
});
