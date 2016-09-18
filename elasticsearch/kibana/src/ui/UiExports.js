'use strict';

var _createClass = require('babel-runtime/helpers/create-class')['default'];

var _classCallCheck = require('babel-runtime/helpers/class-call-check')['default'];

var _toConsumableArray = require('babel-runtime/helpers/to-consumable-array')['default'];

var _getIterator = require('babel-runtime/core-js/get-iterator')['default'];

var _regeneratorRuntime = require('babel-runtime/regenerator')['default'];

var _ = require('lodash');
var minimatch = require('minimatch');

var UiAppCollection = require('./UiAppCollection');

var UiExports = (function () {
  function UiExports(_ref) {
    var urlBasePath = _ref.urlBasePath;

    _classCallCheck(this, UiExports);

    this.apps = new UiAppCollection(this);
    this.aliases = {};
    this.urlBasePath = urlBasePath;
    this.exportConsumer = _.memoize(this.exportConsumer);
    this.consumers = [];
    this.bundleProviders = [];
    this.defaultInjectedVars = [];
  }

  _createClass(UiExports, [{
    key: 'consumePlugin',
    value: function consumePlugin(plugin) {
      var _this = this;

      plugin.apps = new UiAppCollection(this);

      var types = _.keys(plugin.uiExportsSpecs);
      if (!types) return false;

      var unkown = _.reject(types, this.exportConsumer, this);
      if (unkown.length) {
        throw new Error('unknown export types ' + unkown.join(', ') + ' in plugin ' + plugin.id);
      }

      var _iteratorNormalCompletion = true;
      var _didIteratorError = false;
      var _iteratorError = undefined;

      try {
        for (var _iterator = _getIterator(this.consumers), _step; !(_iteratorNormalCompletion = (_step = _iterator.next()).done); _iteratorNormalCompletion = true) {
          var consumer = _step.value;

          consumer.consumePlugin && consumer.consumePlugin(plugin);
        }
      } catch (err) {
        _didIteratorError = true;
        _iteratorError = err;
      } finally {
        try {
          if (!_iteratorNormalCompletion && _iterator['return']) {
            _iterator['return']();
          }
        } finally {
          if (_didIteratorError) {
            throw _iteratorError;
          }
        }
      }

      types.forEach(function (type) {
        _this.exportConsumer(type)(plugin, plugin.uiExportsSpecs[type]);
      });
    }
  }, {
    key: 'addConsumer',
    value: function addConsumer(consumer) {
      this.consumers.push(consumer);
    }
  }, {
    key: 'exportConsumer',
    value: function exportConsumer(type) {
      var _this2 = this;

      var _iteratorNormalCompletion2 = true;
      var _didIteratorError2 = false;
      var _iteratorError2 = undefined;

      try {
        for (var _iterator2 = _getIterator(this.consumers), _step2; !(_iteratorNormalCompletion2 = (_step2 = _iterator2.next()).done); _iteratorNormalCompletion2 = true) {
          var consumer = _step2.value;

          if (!consumer.exportConsumer) continue;
          var fn = consumer.exportConsumer(type);
          if (fn) return fn;
        }
      } catch (err) {
        _didIteratorError2 = true;
        _iteratorError2 = err;
      } finally {
        try {
          if (!_iteratorNormalCompletion2 && _iterator2['return']) {
            _iterator2['return']();
          }
        } finally {
          if (_didIteratorError2) {
            throw _iteratorError2;
          }
        }
      }

      switch (type) {
        case 'app':
        case 'apps':
          return function (plugin, specs) {
            var _iteratorNormalCompletion3 = true;
            var _didIteratorError3 = false;
            var _iteratorError3 = undefined;

            try {
              var _loop = function () {
                var spec = _step3.value;

                var app = _this2.apps['new'](_.defaults({}, spec, {
                  id: plugin.id,
                  urlBasePath: _this2.urlBasePath
                }));

                plugin.extendInit(function (server, options) {
                  // eslint-disable-line no-loop-func
                  var wrapped = app.getInjectedVars;
                  app.getInjectedVars = function () {
                    return wrapped.call(plugin, server, options);
                  };
                });

                plugin.apps.add(app);
              };

              for (var _iterator3 = _getIterator([].concat(specs || [])), _step3; !(_iteratorNormalCompletion3 = (_step3 = _iterator3.next()).done); _iteratorNormalCompletion3 = true) {
                _loop();
              }
            } catch (err) {
              _didIteratorError3 = true;
              _iteratorError3 = err;
            } finally {
              try {
                if (!_iteratorNormalCompletion3 && _iterator3['return']) {
                  _iterator3['return']();
                }
              } finally {
                if (_didIteratorError3) {
                  throw _iteratorError3;
                }
              }
            }
          };

        case 'visTypes':
        case 'fieldFormats':
        case 'spyModes':
        case 'chromeNavControls':
        case 'navbarExtensions':
        case 'settingsSections':
        case 'hacks':
          return function (plugin, spec) {
            _this2.aliases[type] = _.union(_this2.aliases[type] || [], spec);
          };

        case 'bundle':
          return function (plugin, spec) {
            _this2.bundleProviders.push(spec);
          };

        case 'aliases':
          return function (plugin, specs) {
            _.forOwn(specs, function (spec, adhocType) {
              _this2.aliases[adhocType] = _.union(_this2.aliases[adhocType] || [], spec);
            });
          };

        case 'injectDefaultVars':
          return function (plugin, injector) {
            plugin.extendInit(function callee$3$0(server, options) {
              return _regeneratorRuntime.async(function callee$3$0$(context$4$0) {
                while (1) switch (context$4$0.prev = context$4$0.next) {
                  case 0:
                    context$4$0.t0 = _;
                    context$4$0.t1 = this.defaultInjectedVars;
                    context$4$0.next = 4;
                    return _regeneratorRuntime.awrap(injector.call(plugin, server, options));

                  case 4:
                    context$4$0.t2 = context$4$0.sent;
                    context$4$0.t0.merge.call(context$4$0.t0, context$4$0.t1, context$4$0.t2);

                  case 6:
                  case 'end':
                    return context$4$0.stop();
                }
              }, null, _this2);
            });
          };
      }
    }
  }, {
    key: 'find',
    value: function find(patterns) {
      var aliases = this.aliases;
      var names = _.keys(aliases);
      var matcher = _.partialRight(minimatch.filter, { matchBase: true });

      return _.chain(patterns).map(function (pattern) {
        return names.filter(matcher(pattern));
      }).flattenDeep().reduce(function (found, name) {
        return found.concat(aliases[name]);
      }, []).value();
    }
  }, {
    key: 'getAllApps',
    value: function getAllApps() {
      var _ref2;

      var apps = this.apps;

      return (_ref2 = [].concat(_toConsumableArray(apps))).concat.apply(_ref2, _toConsumableArray(apps.hidden));
    }
  }, {
    key: 'getApp',
    value: function getApp(id) {
      return this.apps.byId[id];
    }
  }, {
    key: 'getHiddenApp',
    value: function getHiddenApp(id) {
      return this.apps.hidden.byId[id];
    }
  }, {
    key: 'getBundleProviders',
    value: function getBundleProviders() {
      return this.bundleProviders;
    }
  }]);

  return UiExports;
})();

module.exports = UiExports;
