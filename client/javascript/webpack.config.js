const path = require('path')

module.exports = {
  entry: './src/index.js',
  mode: 'production',
  output: {
    path: path.resolve(__dirname, 'dist'),
    filename: 'datajunction.js',
    globalObject: 'this',
    library: {
      name: 'datajunction',
      type: 'umd',
    },
  },
}
