'use strict';

var _extends = require('babel-runtime/helpers/extends')['default'];

Object.defineProperty(exports, '__esModule', {
  value: true
});
exports['default'] = setHeaders;

var _lodash = require('lodash');

function setHeaders(originalHeaders, newHeaders) {
  if (!(0, _lodash.isPlainObject)(originalHeaders)) {
    throw new Error('Expected originalHeaders to be an object, but ' + typeof originalHeaders + ' given');
  }
  if (!(0, _lodash.isPlainObject)(newHeaders)) {
    throw new Error('Expected newHeaders to be an object, but ' + typeof newHeaders + ' given');
  }

  return _extends({}, originalHeaders, newHeaders);
}

module.exports = exports['default'];
