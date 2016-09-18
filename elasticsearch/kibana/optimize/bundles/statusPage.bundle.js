webpackJsonp([1],{

/***/ 0:
/***/ function(module, exports, __webpack_require__) {

	
	/**
	 * Test entry file
	 *
	 * This is programatically created and updated, do not modify
	 *
	 * context: {"env":"production","urlBasePath":"","sourceMaps":false,"kbnVersion":"4.6.1","buildNum":10146}
	 * includes code from:
	 *  - elasticsearch@1.0.0
	 *  - kbn_vislib_vis_types@1.0.0
	 *  - kibana@1.0.0
	 *  - markdown_vis@1.0.0
	 *  - metric_vis@1.0.0
	 *  - spyModes@1.0.0
	 *  - statusPage@1.0.0
	 *  - table_vis@1.0.0
	 *
	 */

	'use strict';

	__webpack_require__(1);
	__webpack_require__(1070);
	__webpack_require__(900);
	__webpack_require__(901);
	__webpack_require__(902);
	__webpack_require__(903);
	__webpack_require__(904);
	__webpack_require__(905);
	__webpack_require__(906);
	__webpack_require__(907);
	__webpack_require__(908);
	__webpack_require__(909);
	__webpack_require__(910);
	__webpack_require__(911);
	__webpack_require__(912);
	__webpack_require__(913);
	__webpack_require__(914);
	__webpack_require__(915);
	__webpack_require__(900);
	__webpack_require__(916);
	__webpack_require__(917);
	__webpack_require__(1);
	__webpack_require__(212);
	__webpack_require__(1).bootstrap();
	/* xoxo */

/***/ },

/***/ 1070:
/***/ function(module, exports, __webpack_require__) {

	'use strict';

	var $ = __webpack_require__(208);
	var _ = __webpack_require__(194);
	var notify = __webpack_require__(340);

	__webpack_require__(1071);
	__webpack_require__(1076);

	var chrome = __webpack_require__(1).setTabs([{
	  id: '',
	  title: 'Server Status',
	  activeIndicatorColor: '#EFF0F2'
	}]).setRootTemplate(__webpack_require__(1077)).setRootController('ui', function ($http, $scope) {
	  var ui = this;
	  ui.loading = false;

	  ui.refresh = function () {
	    ui.loading = true;

	    // go ahead and get the info you want
	    return $http.get(chrome.addBasePath('/api/status')).then(function (resp) {

	      if (ui.fetchError) {
	        ui.fetchError.clear();
	        ui.fetchError = null;
	      }

	      var data = resp.data;
	      ui.metrics = data.metrics;
	      ui.statuses = data.status.statuses;

	      var overall = data.status.overall;
	      if (!ui.serverState || ui.serverState !== overall.state) {
	        ui.serverState = overall.state;
	        ui.serverStateMessage = overall.title;
	      }
	    })['catch'](function () {
	      if (ui.fetchError) return;
	      ui.fetchError = notify.error('Failed to request server ui. Perhaps your server is down?');
	      ui.metrics = ui.statuses = ui.overall = null;
	    }).then(function () {
	      ui.loading = false;
	    });
	  };

	  ui.refresh();
	});

/***/ },

/***/ 1071:
/***/ function(module, exports, __webpack_require__) {

	'use strict';

	var _ = __webpack_require__(194);
	var moment = __webpack_require__(243);
	var numeral = __webpack_require__(956);

	var toTitleCase = __webpack_require__(1072);
	var formatNumber = __webpack_require__(1073);
	var readStatData = __webpack_require__(1074);

	function calcAvg(metricList, metricNumberType) {
	  return metricList.map(function (data) {
	    var uglySum = data.values.reduce(function (sumSoFar, vector) {
	      return sumSoFar + vector.y;
	    }, 0);
	    return formatNumber(uglySum / data.values.length, metricNumberType);
	  });
	}

	__webpack_require__(215).get('kibana', []).directive('statusPageMetric', function () {
	  return {
	    restrict: 'E',
	    template: __webpack_require__(1075),
	    scope: {
	      name: '@',
	      data: '='
	    },
	    controllerAs: 'metric',
	    controller: function controller($scope) {
	      var self = this;

	      self.name = $scope.name;
	      self.title = toTitleCase(self.name);
	      self.extendedTitle = self.title;
	      self.numberType = 'precise';
	      self.seriesNames = [];

	      switch (self.name) {
	        case 'heapTotal':
	        case 'heapUsed':
	          self.numberType = 'byte';
	          break;

	        case 'responseTimeAvg':
	        case 'responseTimeMax':
	          self.numberType = 'ms';
	          break;

	        case 'load':
	          self.seriesNames = ['1min', '5min', '15min'];
	          break;
	      }

	      $scope.$watch('data', function (data) {
	        self.rawData = data;
	        self.chartData = readStatData(self.rawData, self.seriesNames);
	        self.averages = calcAvg(self.chartData, self.numberType);

	        var unit = '';
	        self.averages = self.averages.map(function (average) {
	          var parts = average.split(' ');
	          var value = parts.shift();
	          unit = parts.join(' ');
	          return value;
	        });
	        self.extendedTitle = self.title;
	        if (unit) {
	          self.extendedTitle = self.extendedTitle + ' (' + unit + ')';
	        }
	      });
	    }
	  };
	});

/***/ },

/***/ 1072:
/***/ function(module, exports, __webpack_require__) {

	'use strict';

	var _ = __webpack_require__(194);

	// Turns thisIsASentence to
	// This Is A Sentence
	module.exports = function toTitleCase(name) {
	  return name.split(/(?=[A-Z])/).map(function (word) {
	    return word[0].toUpperCase() + _.rest(word).join('');
	  }).join(' ');
	};

/***/ },

/***/ 1073:
/***/ function(module, exports, __webpack_require__) {

	'use strict';

	var moment = __webpack_require__(243);
	var numeral = __webpack_require__(956);

	module.exports = function formatNumber(num, which) {
	  var format = '0.00';
	  var postfix = '';
	  switch (which) {
	    case 'time':
	      return moment(num).format('HH:mm:ss');
	    case 'byte':
	      format += ' b';
	      break;
	    case 'ms':
	      postfix = ' ms';
	      break;
	  }
	  return numeral(num).format(format) + postfix;
	};

/***/ },

/***/ 1074:
/***/ function(module, exports, __webpack_require__) {

	'use strict';

	var _ = __webpack_require__(194);

	module.exports = function readStatData(data, seriesNames) {
	  // Metric Values format
	  // metric: [[xValue, yValue], ...]
	  // LoadMetric:
	  // metric: [[xValue, [yValue, yValue2, yValue3]], ...]
	  // return [
	  //    {type: 'line', key: name, yAxis: 1, values: [{x: xValue, y: yValue}, ...]},
	  //    {type: 'line', key: name, yAxis: 1, values: [{x: xValue, y: yValue1}, ...]},
	  //    {type: 'line', key: name, yAxis: 1, values: [{x: xValue, y: yValue2}, ...]}]
	  //
	  // Go through all of the metric values and split the values out.
	  // returns an array of all of the averages

	  var metricList = [];
	  seriesNames = seriesNames || [];
	  data.forEach(function (vector) {
	    vector = _.flatten(vector);
	    var x = vector.shift();
	    vector.forEach(function (yValue, i) {
	      var series = seriesNames[i] || '';

	      if (!metricList[i]) {
	        metricList[i] = {
	          key: series,
	          values: []
	        };
	      }
	      // unshift to make sure they're in the correct order
	      metricList[i].values.unshift({
	        x: x,
	        y: yValue
	      });
	    });
	  });

	  return metricList;
	};

/***/ },

/***/ 1075:
/***/ function(module, exports) {

	module.exports = "<div class=\"status_metric_wrapper col-md-4\">\n  <div class=\"content\">\n    <h3 class=\"title\">{{metric.extendedTitle}}</h3>\n    <h4 class=\"average\">{{ metric.averages.join(', ') }}</h4>\n  </div>\n</div>\n"

/***/ },

/***/ 1076:
/***/ function(module, exports) {

	// removed by extract-text-webpack-plugin

/***/ },

/***/ 1077:
/***/ function(module, exports) {

	module.exports = "<div class=\"container state_default state_{{ui.serverState}}\">\n  <header>\n    <h1>\n      Status: <span class=\"state_color\">{{ ui.serverStateMessage }}</span>\n      <i class=\"fa state_color state_icon\" />\n    </h1>\n  </header>\n\n  <div class=\"row metrics_wrapper\">\n    <div ng-repeat=\"(name, data) in ui.metrics\">\n      <status-page-metric name=\"{{name}}\" data=\"data\"></status-page-metric>\n    </div>\n  </div>\n\n  <div class=\"row plugin_status_wrapper\">\n    <h3>Installed Plugins</h3>\n    <div ng-if=\"!ui.statuses && ui.loading\" class=\"loading_statuses\">\n      <span class=\"spinner\"></span>\n    </div>\n\n    <h4 ng-if=\"!ui.statuses && !ui.loading\" class=\"missing_statuses\">\n      No plugin status information available\n    </h4>\n\n    <table class=\"plugin_status_breakdown\" ng-if=\"ui.statuses\">\n      <tr class=\"row\">\n        <th class=\"col-xs-2\">Name</th>\n        <th class=\"col-xs-2\">Version</th>\n        <th class=\"col-xs-8\">Status</th>\n      </tr>\n      <tr ng-repeat=\"status in ui.statuses\" class=\"status_row plugin_state_default plugin_state_{{status.state}} row\">\n        <td class=\"col-xs-2 status_name\">{{status.name}}</td>\n        <td class=\"col-xs-2 status_version\">{{status.version}}</td>\n        <td class=\"col-xs-8 status_message\">\n          <i class=\"fa plugin_state_color plugin_state_icon\" />\n          {{status.message}}\n        </td>\n      </tr>\n    </table>\n  </div>\n</div>\n"

/***/ }

});