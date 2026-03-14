import nextCoreWebVitals from "eslint-config-next/core-web-vitals";
import nextTypeScript from "eslint-config-next/typescript";

const eslintConfig = [
  ...nextCoreWebVitals,
  ...nextTypeScript,
  {
    ignores: [
      ".next-e2e/**",
      ".open-next/**",
      ".wrangler/**",
      "coverage/**",
      "screenshot/**",
      "test-results/**",
    ],
  },
  {
    files: ["scripts/playwright-webserver.cjs", "tailwind.config.js"],
    rules: {
      "@typescript-eslint/no-require-imports": "off",
    },
  },
];

export default eslintConfig;
