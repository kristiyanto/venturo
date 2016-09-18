'use strict';

Object.defineProperty(exports, '__esModule', {
  value: true
});
exports.list = list;
var fs = require('fs');

function list(settings, logger) {
  fs.readdirSync(settings.pluginDir).forEach(function (pluginFile) {
    logger.log(pluginFile);
  });
}
