const path = require('path');
const HtmlWebpackPlugin = require('html-webpack-plugin');
const MiniCssExtractPlugin = require('mini-css-extract-plugin');

var babelOptions = {
  presets: ['@babel/preset-react'],
};

module.exports = {
  cache: true,
  entry: {
    main: './src/index.tsx',
    vendor: ['events', 'fbemitter', 'flux', 'react', 'react-dom'],
  },
  target: 'web',
  mode: 'development',
  output: {
    path: path.resolve(__dirname, './dist'),
    filename: 'static/[name].[fullhash].js',
    publicPath: '/',
  },
  devServer: {
    historyApiFallback: true,
  },
  resolve: {
    extensions: ['.js', '.jsx', '.json', '.ts', '.tsx'],
    fallback: {
      path: false,
      buffer: false,
      assert: false,
      fs: false,
      os: false,
      module: false,
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
    // new MiniCssExtractPlugin({
    //   filename: './styles/index.css',
    // }),
  ],
};
