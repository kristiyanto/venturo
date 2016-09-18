'use strict';

Object.defineProperty(exports, '__esModule', {
  value: true
});
exports['default'] = fileType;
var TAR = '.tar.gz';
exports.TAR = TAR;
var ZIP = '.zip';

exports.ZIP = ZIP;

function fileType(filename) {
  if (/\.zip$/i.test(filename)) {
    return ZIP;
  }
  if (/\.tar\.gz$/i.test(filename)) {
    return TAR;
  }
  if (/\.tgz$/i.test(filename)) {
    return TAR;
  }
}
