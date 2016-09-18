'use strict';

var elasticsearch = require('elasticsearch');
var _ = require('lodash');
var readFile = function readFile(file) {
  return require('fs').readFileSync(file, 'utf8');
};
var util = require('util');
var url = require('url');
var callWithRequest = require('./call_with_request');

module.exports = function (server) {
  var config = server.config();

  function createClient(options) {
    options = _.defaults(options || {}, {
      url: config.get('elasticsearch.url'),
      username: config.get('elasticsearch.username'),
      password: config.get('elasticsearch.password'),
      verifySsl: config.get('elasticsearch.ssl.verify'),
      clientCrt: config.get('elasticsearch.ssl.cert'),
      clientKey: config.get('elasticsearch.ssl.key'),
      ca: config.get('elasticsearch.ssl.ca'),
      apiVersion: config.get('elasticsearch.apiVersion'),
      pingTimeout: config.get('elasticsearch.pingTimeout'),
      requestTimeout: config.get('elasticsearch.requestTimeout'),
      keepAlive: true,
      auth: true
    });

    var uri = url.parse(options.url);

    var authorization = undefined;
    if (options.auth && options.username && options.password) {
      uri.auth = util.format('%s:%s', options.username, options.password);
    }

    var ssl = { rejectUnauthorized: options.verifySsl };
    if (options.clientCrt && options.clientKey) {
      ssl.cert = readFile(options.clientCrt);
      ssl.key = readFile(options.clientKey);
    }
    if (options.ca) {
      ssl.ca = options.ca.map(readFile);
    }

    var host = {
      host: uri.hostname,
      port: uri.port,
      protocol: uri.protocol,
      path: uri.pathname,
      auth: uri.auth,
      query: uri.query,
      headers: config.get('elasticsearch.customHeaders')
    };

    return new elasticsearch.Client({
      host: host,
      ssl: ssl,
      plugins: options.plugins,
      apiVersion: options.apiVersion,
      keepAlive: options.keepAlive,
      pingTimeout: options.pingTimeout,
      requestTimeout: options.requestTimeout,
      log: function log() {
        this.error = function (err) {
          server.log(['error', 'elasticsearch'], err);
        };
        this.warning = function (message) {
          server.log(['warning', 'elasticsearch'], message);
        };
        this.info = _.noop;
        this.debug = _.noop;
        this.trace = _.noop;
        this.close = _.noop;
      }
    });
  }

  var client = createClient();
  server.on('close', _.bindKey(client, 'close'));

  var noAuthClient = createClient({ auth: false });
  server.on('close', _.bindKey(noAuthClient, 'close'));

  server.expose('client', client);
  server.expose('createClient', createClient);
  server.expose('callWithRequestFactory', callWithRequest);
  server.expose('callWithRequest', callWithRequest(noAuthClient));
  server.expose('errors', elasticsearch.errors);

  return client;
};
