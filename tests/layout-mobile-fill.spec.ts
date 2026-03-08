import { expect, test } from "@playwright/test";

function withPx(value?: number | null) {
  return typeof value === "number" ? value : 0;
}

test("填写页移动端主表占满宽度且撤销清空同排，桌面端截图可用", async ({ page }) => {
  await page.goto("/game");

  await page.setViewportSize({ width: 390, height: 844 });
  await page.reload();
  await page.waitForLoadState("networkidle");

  const boardWidth = await page.evaluate(() => {
    const slot = Array.from(document.querySelectorAll<HTMLElement>("[aria-label]")).find((node) =>
      node.getAttribute("aria-label")?.includes("选择第 1 格")
    );
    if (!slot) return 0;
    let current: HTMLElement | null = slot;
    while (current) {
      if (current.classList.contains("grid") && current.classList.contains("grid-cols-3")) {
        return current.getBoundingClientRect().width;
      }
      current = current.parentElement;
    }
    return 0;
  });

  expect(boardWidth).toBeGreaterThan(340);

  const undo = page.getByRole("button", { name: "撤销" });
  const clear = page.getByRole("button", { name: "清空" });
  const [undoBox, clearBox] = await Promise.all([undo.boundingBox(), clear.boundingBox()]);

  const undoY = withPx(undoBox?.y);
  const clearY = withPx(clearBox?.y);
  expect(Math.abs(undoY - clearY)).toBeLessThan(2);

  await page.screenshot({ path: "screenshot/layout-game-mobile.png", fullPage: true });

  await page.setViewportSize({ width: 1440, height: 900 });
  await page.reload();
  await page.waitForLoadState("networkidle");
  await page.screenshot({ path: "screenshot/layout-game-desktop.png", fullPage: true });
});
