'use strict';

var _createClass = require('babel-runtime/helpers/create-class')['default'];

var _classCallCheck = require('babel-runtime/helpers/class-call-check')['default'];

var _get = require('babel-runtime/helpers/get')['default'];

var _inherits = require('babel-runtime/helpers/inherits')['default'];

var _Symbol = require('babel-runtime/core-js/symbol')['default'];

var _regeneratorRuntime = require('babel-runtime/regenerator')['default'];

var _Promise = require('babel-runtime/core-js/promise')['default'];

var _ = require('lodash');
var Joi = require('joi');
var Bluebird = require('bluebird');

var _require = require('path');

var resolve = _require.resolve;

var _require2 = require('util');

var inherits = _require2.inherits;

var extendInitFns = _Symbol('extend plugin initialization');

var defaultConfigSchema = Joi.object({
  enabled: Joi.boolean()['default'](true)
})['default']();

module.exports = (function () {
  function Plugin(kbnServer, path, pkg, opts) {
    _classCallCheck(this, Plugin);

    this.kbnServer = kbnServer;
    this.pkg = pkg;
    this.path = path;

    this.id = opts.id || pkg.name;
    this.uiExportsSpecs = opts.uiExports || {};
    this.requiredIds = opts.require || [];
    this.version = opts.version || pkg.version;
    this.publicDir = opts.publicDir !== false ? resolve(path, 'public') : null;
    this.externalCondition = opts.initCondition || _.constant(true);
    this.externalInit = opts.init || _.noop;
    this.getConfigSchema = opts.config || _.noop;
    this.init = _.once(this.init);
    this[extendInitFns] = [];
  }

  _createClass(Plugin, [{
    key: 'readConfig',
    value: function readConfig() {
      var schema, config;
      return _regeneratorRuntime.async(function readConfig$(context$2$0) {
        while (1) switch (context$2$0.prev = context$2$0.next) {
          case 0:
            context$2$0.next = 2;
            return _regeneratorRuntime.awrap(this.getConfigSchema(Joi));

          case 2:
            schema = context$2$0.sent;
            config = this.kbnServer.config;

            config.extendSchema(this.id, schema || defaultConfigSchema);

            if (!config.get([this.id, 'enabled'])) {
              context$2$0.next = 9;
              break;
            }

            return context$2$0.abrupt('return', true);

          case 9:
            config.removeSchema(this.id);
            return context$2$0.abrupt('return', false);

          case 11:
          case 'end':
            return context$2$0.stop();
        }
      }, null, this);
    }
  }, {
    key: 'init',
    value: function init() {
      var id, version, kbnServer, config, asyncRegister, register;
      return _regeneratorRuntime.async(function init$(context$2$0) {
        var _this2 = this;

        while (1) switch (context$2$0.prev = context$2$0.next) {
          case 0:
            id = this.id;
            version = this.version;
            kbnServer = this.kbnServer;
            config = kbnServer.config;

            asyncRegister = function asyncRegister(server, options, next) {
              return _regeneratorRuntime.async(function asyncRegister$(context$3$0) {
                var _this = this;

                while (1) switch (context$3$0.prev = context$3$0.next) {
                  case 0:
                    this.server = server;

                    // bind the server and options to all
                    // apps created by this plugin
                    context$3$0.next = 3;
                    return _regeneratorRuntime.awrap(_Promise.all(this[extendInitFns].map(function callee$3$0(fn) {
                      return _regeneratorRuntime.async(function callee$3$0$(context$4$0) {
                        while (1) switch (context$4$0.prev = context$4$0.next) {
                          case 0:
                            context$4$0.next = 2;
                            return _regeneratorRuntime.awrap(fn.call(this, server, options));

                          case 2:
                          case 'end':
                            return context$4$0.stop();
                        }
                      }, null, _this);
                    })));

                  case 3:

                    server.log(['plugins', 'debug'], {
                      tmpl: 'Initializing plugin <%= plugin.toString() %>',
                      plugin: this
                    });

                    if (this.publicDir) {
                      server.exposeStaticDir('/plugins/' + id + '/{path*}', this.publicDir);
                    }

                    this.status = kbnServer.status.create(this);
                    server.expose('status', this.status);

                    context$3$0.next = 9;
                    return _regeneratorRuntime.awrap(Bluebird.attempt(this.externalInit, [server, options], this));

                  case 9:
                    return context$3$0.abrupt('return', context$3$0.sent);

                  case 10:
                  case 'end':
                    return context$3$0.stop();
                }
              }, null, _this2);
            };

            register = function register(server, options, next) {
              Bluebird.resolve(asyncRegister(server, options)).nodeify(next);
            };

            register.attributes = { name: id, version: version };

            context$2$0.next = 9;
            return _regeneratorRuntime.awrap(Bluebird.fromNode(function (cb) {
              kbnServer.server.register({
                register: register,
                options: config.has(id) ? config.get(id) : null
              }, cb);
            }));

          case 9:

            // Only change the plugin status to green if the
            // intial status has not been changed
            if (this.status.state === 'uninitialized') {
              this.status.green('Ready');
            }

          case 10:
          case 'end':
            return context$2$0.stop();
        }
      }, null, this);
    }
  }, {
    key: 'extendInit',
    value: function extendInit(fn) {
      this[extendInitFns].push(fn);
    }
  }, {
    key: 'toJSON',
    value: function toJSON() {
      return this.pkg;
    }
  }, {
    key: 'toString',
    value: function toString() {
      return this.id + '@' + this.version;
    }
  }], [{
    key: 'scoped',
    value: function scoped(kbnServer, path, pkg) {
      return (function (_Plugin) {
        _inherits(ScopedPlugin, _Plugin);

        function ScopedPlugin(opts) {
          _classCallCheck(this, ScopedPlugin);

          _get(Object.getPrototypeOf(ScopedPlugin.prototype), 'constructor', this).call(this, kbnServer, path, pkg, opts || {});
        }

        return ScopedPlugin;
      })(Plugin);
    }
  }]);

  return Plugin;
})();

// setup the hapi register function and get on with it
