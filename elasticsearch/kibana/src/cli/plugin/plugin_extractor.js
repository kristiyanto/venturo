'use strict';

Object.defineProperty(exports, '__esModule', {
  value: true
});
exports['default'] = extractArchive;

var _file_type = require('./file_type');

var zipExtract = require('./extractors/zip');
var tarGzExtract = require('./extractors/tar_gz');

function extractArchive(settings, logger, archiveType) {
  switch (archiveType) {
    case _file_type.ZIP:
      return zipExtract(settings, logger);
      break;
    case _file_type.TAR:
      return tarGzExtract(settings, logger);
      break;
    default:
      throw new Error('Unsupported archive format.');
  }
}

;
module.exports = exports['default'];
