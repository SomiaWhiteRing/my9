export const SYSTEM_THEME_INIT_SCRIPT = `
(() => {
  const root = document.documentElement;
  const mediaQuery = window.matchMedia("(prefers-color-scheme: dark)");
  const applyTheme = () => root.classList.toggle("dark", mediaQuery.matches);
  applyTheme();
  if (typeof mediaQuery.addEventListener === "function") {
    mediaQuery.addEventListener("change", applyTheme);
    return;
  }
  if (typeof mediaQuery.addListener === "function") {
    mediaQuery.addListener(applyTheme);
  }
})();
`;
