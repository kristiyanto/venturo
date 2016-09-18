'use strict';

var _extends = require('babel-runtime/helpers/extends')['default'];

var _interopRequireDefault = require('babel-runtime/helpers/interop-require-default')['default'];

Object.defineProperty(exports, '__esModule', {
  value: true
});
exports['default'] = mapUri;

var _querystring = require('querystring');

var _querystring2 = _interopRequireDefault(_querystring);

var _url = require('url');

var _set_headers = require('./set_headers');

var _set_headers2 = _interopRequireDefault(_set_headers);

function mapUri(server, prefix) {

  var config = server.config();
  return function (request, done) {
    var path = request.path.replace('/elasticsearch', '');
    var url = config.get('elasticsearch.url');

    var _parseUrl = (0, _url.parse)(url);

    var host = _parseUrl.host;

    if (path) {
      if (/\/$/.test(url)) url = url.substring(0, url.length - 1);
      url += path;
    }
    var query = _querystring2['default'].stringify(request.query);
    if (query) url += '?' + query;
    // We want the host of elasticsearch rather than of Kibana
    var headers = _extends({}, request.headers, {
      host: host
    });
    var customHeaders = (0, _set_headers2['default'])(headers, config.get('elasticsearch.customHeaders'));
    done(null, url, customHeaders);
  };
}

;
module.exports = exports['default'];
