const webpack = require('webpack');
const dotenv = require('dotenv').config();
const path = require('path');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');

require('dotenv').config({ path: './.env' });

var babelOptions = {
  presets: ['@babel/preset-react'],
};

const isProduction = process.env.NODE_ENV === 'production';

module.exports = {
  cache: true,
  entry: {
    main: './src/index.tsx',
    vendor: ['events', 'react', 'react-dom'],
  },
  target: 'web',
  mode: isProduction ? 'production' : 'development',
  stats: 'minimal',
  output: {
    path: path.resolve(__dirname, './dist'),
    filename: 'static/[name].[fullhash].js',
    chunkFilename: 'static/[name].[fullhash].chunk.js', // For lazy-loaded chunks
    library: 'datajunction-ui',
    libraryTarget: 'umd',
    globalObject: 'this',
    umdNamedDefine: true,
    publicPath: '/',
  },
  optimization: {
    splitChunks: {
      chunks: 'all',
      cacheGroups: {
        // Split ReactFlow and dagre into a separate chunk since they're heavy
        // and only used on the QueryPlannerPage
        reactflow: {
          test: /[\\/]node_modules[\\/](reactflow|@reactflow|dagre|d3-.*)[\\/]/,
          name: 'reactflow-vendor',
          chunks: 'async',
          priority: 20,
        },
        // Common vendor chunks
        vendor: {
          test: /[\\/]node_modules[\\/]/,
          name: 'vendors',
          chunks: 'initial',
          priority: 10,
        },
      },
    },
  },
  devServer: {
    historyApiFallback: {
      disableDotRule: true,
    },
    host: '0.0.0.0', // Allow connections from outside container
    port: 3000,
    hot: true, // Enable hot module replacement
    watchFiles: ['src/**/*'], // Watch for changes
  },
  resolve: {
    extensions: ['.js', '.jsx', '.json', '.ts', '.tsx', '.scss'],
    modules: ['src', 'node_modules'],
    fallback: {
      path: false,
      buffer: false,
      assert: false,
      fs: false,
      os: false,
      module: false,
      http: false,
      tls: false,
      https: false,
      url: false,
      browser: false,
      net: false,
      process: false,
    },
  },
  module: {
    rules: [
      {
        test: /\.(ts|tsx|jsx)$/,
        use: [
          {
            loader: 'babel-loader',
            options: babelOptions,
          },
          {
            loader: 'ts-loader',
            options: {
              compilerOptions: {
                noEmit: false,
              },
            },
          },
        ],
      },
      {
        test: /\.css$/,
        use: ['style-loader', 'css-loader'],
      },
      {
        test: /\.(s(a|c)ss)$/,
        use: [MiniCssExtractPlugin.loader, 'css-loader', 'sass-loader'],
      },
      {
        test: /\.js$/,
        exclude: /node_modules/,
        use: [
          {
            loader: 'babel-loader',
            options: babelOptions,
          },
        ],
      },
      {
        test: /\.(png|jpe?g|gif)$/i,
        use: [
          {
            loader: 'file-loader',
          },
        ],
      },
      {
        test: /node_modules[\\|/](name-of-the-umd-package)/,
        use: { loader: 'umd-compat-loader' },
      },
    ],
  },
  plugins: [
    new HtmlWebpackPlugin({
      template: path.resolve(__dirname, 'public', 'index.html'),
      favicon: path.resolve(__dirname, 'public', 'favicon.ico'),
    }),
    new webpack.DefinePlugin({
      'process.env': JSON.stringify(process.env),
    }),
    new MiniCssExtractPlugin({
      filename: '[name].css', // isDevelopment ? '[name].css' : '[name].[hash].css',
      chunkFilename: '[id].css', // isDevelopment ? '[id].css' : '[id].[hash].css'
    }),
  ],
};
