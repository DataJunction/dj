const fs = require('fs');
const path = require('path');

const prettierOptions = JSON.parse(
  fs.readFileSync(path.resolve(__dirname, '.prettierrc'), 'utf8'),
);

module.exports = {
  extends: ['react-app', 'prettier'],
  plugins: ['prettier'],
  rules: {
    'prettier/prettier': ['error', prettierOptions],
  },
  overrides: [
    {
      files: ['**/*.ts?(x)'],
      rules: { 'prettier/prettier': ['warn', prettierOptions] },
    },
    {
      // Vitest globals (enabled via `globals: true` in vitest.config.ts).
      // We migrated from jest, so the `react-app/jest` config no longer
      // covers these. Until we move to a vitest-aware flat config, declare
      // the test globals here.
      files: [
        '**/__tests__/**/*.{js,jsx,ts,tsx}',
        '**/*.{test,spec}.{js,jsx,ts,tsx}',
        'src/setupTests.ts',
        'src/mocks/**/*.{js,ts}',
      ],
      globals: {
        vi: 'readonly',
        Mock: 'readonly',
        MockedFunction: 'readonly',
        Mocked: 'readonly',
      },
    },
  ],
};
