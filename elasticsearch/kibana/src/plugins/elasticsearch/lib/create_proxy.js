'use strict';

var createAgent = require('./create_agent');
var mapUri = require('./map_uri');

var _require = require('url');

var resolve = _require.resolve;

var _require2 = require('lodash');

var assign = _require2.assign;

function createProxy(server, method, route, config) {

  var options = {
    method: method,
    path: createProxy.createPath(route),
    config: {
      timeout: {
        socket: server.config().get('elasticsearch.requestTimeout')
      }
    },
    handler: {
      proxy: {
        mapUri: mapUri(server),
        agent: createAgent(server),
        xforward: true,
        timeout: server.config().get('elasticsearch.requestTimeout'),
        onResponse: function onResponse(err, responseFromUpstream, request, reply) {
          reply(err, responseFromUpstream);
        }
      }
    }
  };

  assign(options.config, config);

  server.route(options);
};

createProxy.createPath = function createPath(path) {
  var pre = '/elasticsearch';
  var sep = path[0] === '/' ? '' : '/';
  return '' + pre + sep + path;
};

module.exports = createProxy;
