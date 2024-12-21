module.exports = {
    // Other Jest configuration options...
    moduleNameMapper: {
      // Add existing mappings here if needed
      "unist-util-visit-parents/do-not-use-color": "<rootDir>/node_modules/unist-util-visit-parents/lib/color.js",
      "vfile/do-not-use-conditional-minpath": "<rootDir>/node_modules/vfile/lib/minpath.browser.js",
      "vfile/do-not-use-conditional-minproc": "<rootDir>/node_modules/vfile/lib/minproc.browser.js",
      "vfile/do-not-use-conditional-minurl": "<rootDir>/node_modules/vfile/lib/minurl.browser.js",
    },
  };
