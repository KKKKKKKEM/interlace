import js from "@eslint/js";
import reactHooks from "eslint-plugin-react-hooks";
import globals from "globals";

export default [
  { ignores: ["node_modules/", "test-results/", "playwright-report/"] },
  js.configs.recommended,
  {
    files: ["src/**/*.{js,jsx}"],
    languageOptions: {
      globals: globals.browser,
      parserOptions: { ecmaFeatures: { jsx: true } },
    },
    plugins: { "react-hooks": reactHooks },
    rules: {
      // 经典 JSX 运行时把 <X/> 编译为 React.createElement，导入的 React 由编译产物使用。
      "no-unused-vars": ["error", { varsIgnorePattern: "^React$" }],
      // 只启用 Hook 调用顺序与依赖完整性；React Compiler 相关规则不在当前构建链中生效。
      "react-hooks/rules-of-hooks": "error",
      "react-hooks/exhaustive-deps": "error",
    },
  },
  {
    files: ["tests/**/*.js"],
    // page.evaluate 回调在浏览器中执行。
    languageOptions: { globals: { ...globals.node, ...globals.browser } },
  },
  {
    files: ["*.{js,mjs}"],
    languageOptions: { globals: globals.node },
  },
];
