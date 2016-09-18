'use strict';

var _interopRequireDefault = require('babel-runtime/helpers/interop-require-default')['default'];

Object.defineProperty(exports, '__esModule', {
  value: true
});

var _fs = require('fs');

var _lodash = require('lodash');

var _utilsFromRoot = require('../../utils/fromRoot');

var _utilsFromRoot2 = _interopRequireDefault(_utilsFromRoot);

var CONFIG_PATHS = [process.env.CONFIG_PATH, (0, _utilsFromRoot2['default'])('config/kibana.yml')].filter(Boolean);

var DATA_PATHS = [process.env.DATA_PATH, (0, _utilsFromRoot2['default'])('data'), '/var/lib/kibana'].filter(Boolean);

function findFile(paths) {
  var availablePath = (0, _lodash.find)(paths, function (configPath) {
    try {
      (0, _fs.accessSync)(configPath, _fs.R_OK);
      return true;
    } catch (e) {
      //Check the next path
    }
  });
  return availablePath || paths[0];
}

exports['default'] = {
  getConfig: function getConfig() {
    return findFile(CONFIG_PATHS);
  },
  getData: function getData() {
    return findFile(DATA_PATHS);
  }
};
module.exports = exports['default'];
