'use strict';

var _interopRequireDefault = require('babel-runtime/helpers/interop-require-default')['default'];

var _wrap_auth_config = require('./wrap_auth_config');

var _wrap_auth_config2 = _interopRequireDefault(_wrap_auth_config);

module.exports = function (kbnServer, server, config) {
  var _ = require('lodash');
  var ServerStatus = require('./ServerStatus');

  var _require = require('path');

  var join = _require.join;

  kbnServer.status = new ServerStatus(kbnServer.server);

  if (server.plugins.good) {
    kbnServer.mixin(require('./metrics'));
  }

  var wrapAuth = (0, _wrap_auth_config2['default'])(config.get('status.allowAnonymous'));

  server.route(wrapAuth({
    method: 'GET',
    path: '/api/status',
    handler: function handler(request, reply) {
      return reply({
        status: kbnServer.status.toJSON(),
        metrics: kbnServer.metrics
      });
    }
  }));

  server.decorate('reply', 'renderStatusPage', function () {
    var app = kbnServer.uiExports.getHiddenApp('statusPage');
    var resp = app ? this.renderApp(app) : this(kbnServer.status.toString());
    resp.code(kbnServer.status.isGreen() ? 200 : 503);
    return resp;
  });

  server.route(wrapAuth({
    method: 'GET',
    path: '/status',
    handler: function handler(request, reply) {
      return reply.renderStatusPage();
    }
  }));
};
