const webpack = require('webpack');
const path = require('path');
const HtmlWebpackPlugin = require('html-webpack-plugin');
// const ReactSelectPlugin = require('react-select');
// const MiniCssExtractPlugin = require('mini-css-extract-plugin');

var babelOptions = {
  presets: ['@babel/preset-react'],
};

module.exports = {
  cache: true,
  entry: {
    main: './src/index.tsx',
    vendor: ['events', 'react', 'react-dom'],
  },
  target: 'web',
  mode: 'development',
  output: {
    path: path.resolve(__dirname, './dist'),
    filename: 'static/[name].[fullhash].js',
    library: 'datajunction-ui',
    libraryTarget: 'umd',
    globalObject: 'this',
    umdNamedDefine: true,
    publicPath: '/',
  },
  devServer: {
    historyApiFallback: {
      disableDotRule: true,
    },
  },
  resolve: {
    extensions: ['.js', '.jsx', '.json', '.ts', '.tsx'],
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
    }),
    new webpack.DefinePlugin({
      REACT_APP_DJ_URL: 'http://localhost:8000',
    }),
    // new MiniCssExtractPlugin({
    //   filename: './styles/index.css',
    // }),
  ],
};
