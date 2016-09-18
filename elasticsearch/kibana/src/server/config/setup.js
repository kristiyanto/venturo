'use strict';

module.exports = function (kbnServer) {
  var Config = require('./config');
  var schema = require('./schema')();

  kbnServer.config = new Config(schema, kbnServer.settings || {});
};
