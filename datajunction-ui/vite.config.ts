import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';
import dts from 'vite-plugin-dts';
import path from 'node:path';

export default defineConfig(({ mode }) => {
  const isLib = process.env.BUILD_TARGET === 'lib';
  const env = loadEnv(mode, process.cwd(), '');

  // CRA exposed any var starting with REACT_ via process.env at build time.
  // Vite doesn't do that by default, so we re-inject them here so that
  // existing source code keeps working unchanged.
  const reactEnv: Record<string, string> = {};
  for (const key of Object.keys(env)) {
    if (key.startsWith('REACT_')) {
      reactEnv[`process.env.${key}`] = JSON.stringify(env[key]);
    }
  }

  return {
    plugins: [
      react(),
      ...(isLib
        ? [
            dts({
              include: ['src/**/*.ts', 'src/**/*.tsx'],
              exclude: [
                'src/**/__tests__/**',
                'src/**/*.test.*',
                'src/mocks/**',
              ],
              insertTypesEntry: true,
            }),
          ]
        : []),
    ],
    resolve: {
      alias: {
        // baseUrl: "./src" replacement — absolute imports resolve from src
        app: path.resolve(__dirname, 'src/app'),
        styles: path.resolve(__dirname, 'src/styles'),
        utils: path.resolve(__dirname, 'src/utils'),
        mocks: path.resolve(__dirname, 'src/mocks'),
      },
    },
    server: {
      host: '0.0.0.0',
      port: 3000,
      open: true,
    },
    define: {
      'process.env.NODE_ENV': JSON.stringify(mode),
      ...reactEnv,
    },
    build: isLib
      ? {
          lib: {
            entry: path.resolve(__dirname, 'src/index.tsx'),
            name: 'DataJunctionUI',
            formats: ['es', 'cjs'],
            fileName: format => `index.${format === 'es' ? 'js' : 'cjs'}`,
          },
          outDir: 'dist',
          sourcemap: true,
          rollupOptions: {
            external: [
              'react',
              'react-dom',
              'react/jsx-runtime',
              'react-router-dom',
              'react-redux',
              '@reduxjs/toolkit',
            ],
            output: {
              globals: {
                react: 'React',
                'react-dom': 'ReactDOM',
              },
            },
          },
        }
      : {
          outDir: 'build',
          sourcemap: true,
        },
  };
});
