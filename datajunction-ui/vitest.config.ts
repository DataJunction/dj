import { defineConfig } from 'vitest/config';
import { loadEnv } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'node:path';

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  const reactEnv: Record<string, string> = {};
  for (const key of Object.keys(env)) {
    if (key.startsWith('REACT_')) {
      reactEnv[`process.env.${key}`] = JSON.stringify(env[key]);
    }
  }

  return {
    plugins: [react()],
    resolve: {
      alias: {
        app: path.resolve(__dirname, 'src/app'),
        styles: path.resolve(__dirname, 'src/styles'),
        utils: path.resolve(__dirname, 'src/utils'),
        mocks: path.resolve(__dirname, 'src/mocks'),
      },
      dedupe: [
        '@codemirror/state',
        '@codemirror/view',
        '@codemirror/language',
        '@lezer/highlight',
      ],
      // Force ESM resolution. Without this Vitest can pull @uiw/* in
      // through its `main` (CJS) field, which loads @codemirror/state's
      // CJS build — while our direct imports load the ESM build, giving
      // two module instances and breaking `instanceof Extension`.
      conditions: ['module', 'browser', 'import', 'default'],
    },
    define: {
      'process.env.NODE_ENV': JSON.stringify('test'),
      ...reactEnv,
    },
    test: {
      globals: true,
      environment: 'jsdom',
      setupFiles: ['./src/setupTests.ts'],
      css: true,
      isolate: true,
      // CodeMirror packages do `instanceof Extension` checks; vitest can
      // otherwise load them twice (once via @uiw's CJS bundle, once via
      // our direct ESM import). Inline + dedupe + ESM conditions together
      // pin a single module instance.
      server: {
        deps: {
          inline: [/^@codemirror\//, /^@lezer\//, /^@uiw\//, /^codemirror$/],
        },
      },
      // Each test file gets a fresh module graph + mock registry. Without
      // this, App.test imports NodePage indirectly and primes the lazy
      // module cache, defeating NodePage.test's `vi.mock('cronstrue', ...)`.
      pool: 'forks',
      poolOptions: { forks: { singleFork: false } },
      coverage: {
        provider: 'v8',
        include: ['src/**/*.{js,jsx,ts,tsx}'],
        exclude: [
          'src/**/*.d.ts',
          'src/**/Loadable.{js,jsx,ts,tsx}',
          'src/**/messages.ts',
          'src/**/types.ts',
          'src/index.tsx',
          'src/main.tsx',
        ],
        thresholds: {
          statements: 80,
          branches: 69,
          lines: 80,
          functions: 80,
        },
      },
    },
  };
});
