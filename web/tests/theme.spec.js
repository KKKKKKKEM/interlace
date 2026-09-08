import { test, expect } from "@playwright/test";

/** 从可见外观菜单选择主题，保留原生单选与弹出层行为。 */
async function selectTheme(page, name) {
  await page.getByRole("button", { name: "切换主题" }).click();
  await page.getByRole("radio", { name, exact: true }).check();
}

test("主题跟随系统，显式选择持久保存且不受系统变化覆盖", async ({ page }) => {
  await page.emulateMedia({ colorScheme: "dark" });
  await page.goto("/");
  const root = page.locator("html");
  await expect(root).toHaveAttribute("data-theme", "dark");
  await expect(root).toHaveAttribute("data-theme-preference", "system");
  await page.emulateMedia({ colorScheme: "light" });
  await expect(root).toHaveAttribute("data-theme", "light");
  await selectTheme(page, "深色");
  await expect(page.getByRole("button", { name: "切换主题" })).toBeFocused();
  await expect(root).toHaveAttribute("data-theme", "dark");
  await page.reload();
  await expect(root).toHaveAttribute("data-theme", "dark");
  await expect(root).toHaveAttribute("data-theme-preference", "dark");
  await selectTheme(page, "浅色");
  await page.emulateMedia({ colorScheme: "dark" });
  await expect(root).toHaveAttribute("data-theme", "light");
  await selectTheme(page, "跟随系统");
  await expect(root).toHaveAttribute("data-theme", "dark");
  await page.emulateMedia({ colorScheme: "light" });
  await expect(root).toHaveAttribute("data-theme", "light");
  await page.getByRole("button", { name: "切换主题" }).click();
  await page.getByRole("radio", { name: "跟随系统" }).focus();
  await page.keyboard.press("ArrowUp");
  await expect(root).toHaveAttribute("data-theme-preference", "dark");
  await page.getByRole("button", { name: "切换主题" }).click();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("radiogroup", { name: "外观模式" })).toBeHidden();
});

test("持久主题在应用脚本加载前生效", async ({ page }) => {
  await page.addInitScript(() =>
    localStorage.setItem("interlace.theme", "dark"),
  );
  await page.emulateMedia({ colorScheme: "light" });
  await page.route("**/assets/index-*.js", (route) => route.abort());
  await page.goto("/", { waitUntil: "domcontentloaded" });
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await expect(page.locator("html")).toHaveCSS("color-scheme", "dark");
  await expect(page.locator("#root")).toBeEmpty();
});

test("禁用本地存储仍能切换主题，深色表格与主按钮保持文字对比度", async ({
  page,
}) => {
  await page.addInitScript(() => {
    // 只限制主题使用的 localStorage，不干扰已有认证 sessionStorage。
    Object.defineProperty(window, "localStorage", {
      value: {
        getItem: () => {
          throw new DOMException("Storage blocked", "SecurityError");
        },
        setItem: () => {
          throw new DOMException("Storage blocked", "SecurityError");
        },
      },
    });
  });
  await page.emulateMedia({ colorScheme: "light" });
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "项目", exact: true }),
  ).toBeVisible();
  await selectTheme(page, "深色");
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  const contrasts = await page
    .locator(".primary, .management-table th")
    .evaluateAll((elements) => {
      const luminance = (color) => {
        const channels = color
          .match(/[\d.]+/g)
          .slice(0, 3)
          .map(Number)
          .map((value) => {
            const component = value / 255;
            return component <= 0.04045
              ? component / 12.92
              : ((component + 0.055) / 1.055) ** 2.4;
          });
        return (
          channels[0] * 0.2126 + channels[1] * 0.7152 + channels[2] * 0.0722
        );
      };
      return elements
        .filter((element) => element.textContent.trim())
        .map((element) => {
          const style = getComputedStyle(element);
          const foreground = luminance(style.color);
          const background = luminance(style.backgroundColor);
          return (
            (Math.max(foreground, background) + 0.05) /
            (Math.min(foreground, background) + 0.05)
          );
        });
    });
  expect(contrasts.length).toBeGreaterThan(1);
  for (const contrast of contrasts)
    expect(contrast).toBeGreaterThanOrEqual(4.5);
});

test("深色编排与运行详情在手机上切换主题时保持当前执行", async ({ page }) => {
  await page.emulateMedia({ colorScheme: "dark" });
  await page.goto("/#/projects/default/definitions/text.pipeline?tab=graph");
  await expect(page.locator(".graph-node")).toHaveCount(3);
  await page.getByRole("button", { name: "运行", exact: true }).click();
  await page
    .getByRole("dialog")
    .locator("textarea")
    .first()
    .fill('{"text":"theme check"}');
  await page.getByRole("button", { name: "开始运行" }).click();
  await expect(page.locator(".active-run")).toContainText("已完成");
  const executionUrl = page.url();
  await page.locator(".panel-body tbody tr").nth(1).click();
  await expect(page.locator(".run-inspector")).toContainText("theme check");
  await page.setViewportSize({ width: 390, height: 844 });
  await selectTheme(page, "浅色");
  await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
  expect(page.url()).toBe(executionUrl);
  await expect(page.locator(".run-inspector")).toContainText("theme check");
  await selectTheme(page, "深色");
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
  await page.getByRole("button", { name: "关闭节点详情" }).click();
  await page.getByRole("button", { name: "打开导航" }).click();
  await expect(page.getByRole("combobox", { name: "切换项目" })).toBeVisible();
});
