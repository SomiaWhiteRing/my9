import tseslint from "typescript-eslint";
import hooks from "eslint-plugin-react-hooks";

export default [
  { ignores: [".cf-build/**", ".next*/**", ".open-next/**", ".wrangler/**", ".my9/**", "dist/**",
    "coverage/**", "screenshot/**", "test-results/**"] },
  ...tseslint.configs.recommended,
  { files: ["**/*.{ts,tsx}"], plugins: { "react-hooks": hooks }, rules: {
    "react-hooks/rules-of-hooks": "error", "react-hooks/exhaustive-deps": "warn"
  } },
  { files: ["scripts/playwright-webserver.cjs", "tailwind.config.js"],
    rules: { "@typescript-eslint/no-require-imports": "off" } },
];
