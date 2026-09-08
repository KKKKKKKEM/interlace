import { test, expect } from "@playwright/test";

/** 通过管理界面创建项目并返回稳定项目地址。 */
async function createProject(page, name) {
  await page.goto("/#/projects");
  await page.getByRole("button", { name: "新建项目", exact: true }).click();
  await page
    .getByText("项目名称", { exact: true })
    .locator("..")
    .locator("input")
    .fill(name);
  await page
    .getByText("项目描述", { exact: true })
    .locator("..")
    .locator("textarea")
    .fill("项目管理交互验证");
  await page.getByRole("button", { name: "创建项目", exact: true }).click();
  await expect(page.getByRole("heading", { name, exact: true })).toBeVisible();
  return page.url();
}

test("项目列表、定义列表、详情和直接链接形成完整层级", async ({ page }) => {
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "项目", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("link", { name: "默认项目", exact: true }),
  ).toBeVisible();
  await expect(page.locator(".graph-node")).toHaveCount(0);
  await page.screenshot({
    path: "test-results/projects-desktop.png",
    fullPage: true,
  });
  const projectUrl = await createProject(page, "数据采集项目");
  await expect(page.getByText("此项目暂无定义", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "新建定义", exact: true }).click();
  await page
    .getByText("定义标识", { exact: true })
    .locator("..")
    .locator("input")
    .fill("managed.flow");
  await page.getByRole("button", { name: "创建定义", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "managed.flow", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "添加节点", exact: true }).first(),
  ).toBeVisible();
  await page.getByRole("tab", { name: "概览", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "定义信息", exact: true }),
  ).toBeVisible();
  await expect(page.locator(".graph-node")).toHaveCount(0);
  const detailUrl = page.url();
  await page.reload();
  await expect(
    page.getByRole("heading", { name: "managed.flow", exact: true }),
  ).toBeVisible();
  await page.screenshot({
    path: "test-results/definition-overview-desktop.png",
    fullPage: true,
  });
  await page.getByRole("link", { name: "定义列表", exact: true }).click();
  await expect(
    page.getByRole("link", { name: "managed.flow", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("link", { name: "text.pipeline", exact: true }),
  ).toHaveCount(0);
  await page.screenshot({
    path: "test-results/project-definitions-desktop.png",
    fullPage: true,
  });
  await page.goBack();
  await expect(
    page.getByRole("heading", { name: "managed.flow", exact: true }),
  ).toBeVisible();
  await page.goForward();
  await expect(
    page.getByRole("link", { name: "managed.flow", exact: true }),
  ).toBeVisible();
  await page.goto(detailUrl);
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(
    page.getByRole("heading", { name: "managed.flow", exact: true }),
  ).toBeVisible();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await page.screenshot({
    path: "test-results/definition-overview-mobile.png",
    fullPage: true,
  });
  await page.goto(projectUrl);
  await expect(
    page.getByRole("link", { name: "managed.flow", exact: true }),
  ).toBeVisible();
  await page.getByLabel("搜索定义").fill("not-present");
  await expect(page.getByText("没有匹配的定义", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "清空搜索", exact: true }).click();
  await expect(
    page.getByRole("link", { name: "managed.flow", exact: true }),
  ).toBeVisible();
  await page.screenshot({
    path: "test-results/project-definitions-mobile.png",
    fullPage: true,
  });
  expect(errors).toEqual([]);
});

test("移动定义、项目设置和空项目删除持久保存", async ({ page }) => {
  const sourceUrl = await createProject(page, "移动来源项目");
  await page.getByRole("button", { name: "新建定义", exact: true }).click();
  await page
    .getByText("定义标识", { exact: true })
    .locator("..")
    .locator("input")
    .fill("moved.flow");
  await page.getByRole("button", { name: "创建定义", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "moved.flow", exact: true }),
  ).toBeVisible();
  const definitionUrl = page.url();
  const targetUrl = await createProject(page, "移动目标项目");
  await page.goto(definitionUrl);
  await page.getByRole("button", { name: "移动到项目", exact: true }).click();
  await page
    .getByLabel("目标项目", { exact: true })
    .selectOption({ label: "移动目标项目" });
  await page.getByRole("button", { name: "确认移动", exact: true }).click();
  await expect(page.locator(".breadcrumbs")).toContainText("移动目标项目");
  await page.reload();
  await expect(page.locator(".breadcrumbs")).toContainText("移动目标项目");
  await page.goto(sourceUrl);
  await expect(page.getByText("此项目暂无定义", { exact: true })).toBeVisible();
  await page.getByRole("link", { name: "项目设置", exact: true }).click();
  page.once("dialog", (dialog) => dialog.accept());
  await page.getByRole("button", { name: "删除项目", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "项目", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("link", { name: "移动来源项目", exact: true }),
  ).toHaveCount(0);
  await page.goto(targetUrl);
  await page.getByRole("link", { name: "项目设置", exact: true }).click();
  await page
    .getByText("项目名称", { exact: true })
    .locator("..")
    .locator("input")
    .fill("正式目标项目");
  await page.getByRole("button", { name: "保存信息", exact: true }).click();
  await expect(page.getByRole("status")).toContainText("项目信息已保存");
  await page.reload();
  await expect(
    page.getByRole("heading", { name: "正式目标项目", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("button", { name: "删除项目", exact: true }),
  ).toBeDisabled();
});

test("草稿未保存时阻止返回项目列表", async ({ page }) => {
  await createProject(page, "编辑保护项目");
  await page.getByRole("button", { name: "新建定义", exact: true }).click();
  await page
    .getByText("定义标识", { exact: true })
    .locator("..")
    .locator("input")
    .fill("guard.flow");
  await page.getByRole("button", { name: "创建定义", exact: true }).click();
  await page
    .getByRole("button", { name: "添加节点", exact: true })
    .first()
    .click();
  await page.getByRole("button", { name: "文本转换 text.transform" }).click();
  await expect(page.locator(".definition-heading")).toContainText("未保存");
  const dismissed = page
    .waitForEvent("dialog")
    .then((dialog) => dialog.dismiss());
  await page.getByRole("link", { name: "项目管理", exact: true }).click();
  await dismissed;
  await expect(
    page.getByRole("heading", { name: "guard.flow", exact: true }),
  ).toBeVisible();
  await expect(page.locator(".graph-node")).toHaveCount(1);
  const accepted = page
    .waitForEvent("dialog")
    .then((dialog) => dialog.accept());
  await page.getByRole("link", { name: "项目管理", exact: true }).click();
  await accepted;
  await expect(
    page.getByRole("heading", { name: "项目", exact: true }),
  ).toBeVisible();
});
