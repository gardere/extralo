var _ = require('underscore');
var q = require('q');
var splunkjs = require('splunk-sdk');

var run1ceResolveAll = require('../../run-once-resolve-all.js');
var myCache = require('../../cache/my-cache');

var ETLdataSource = require('./base-ETL-data-source.js');

// var log = function () {};
var log = console.log;



var SplunkETLDataSource = function(rawDefinition, inputs) {
  ETLdataSource.call(this, rawDefinition, inputs);
};

SplunkETLDataSource.prototype = Object.create(ETLdataSource.prototype);
SplunkETLDataSource.prototype.constructor = SplunkETLDataSource;


SplunkETLDataSource.prototype.getRequiredParams = function(operationDefinition) {
  return [{
    name: 'hostname',
    accepted_types: ['string']
  }, {
    name: 'login',
    accepted_types: ['string']
  }, {
    name: 'password',
    accepted_types: ['string']
  }, {
    name: 'search',
    accepted_types: ['object']
  }];
};

SplunkETLDataSource.prototype.getName = function() {
  return 'SplunkDataSource';
};

SplunkETLDataSource.prototype.execute = function() {
  return executeSearch(this.parsedDefinition);
};

function executeSearch(definition) {
  var host = definition.hostname;
  var login = definition.login;
  var password = definition.password;
  var searchQuery = definition.search.query;
  var searchParams = definition.search.params;


  searchParams = _.extend({max_count: 50000}, searchParams);

  log('running search: ' + searchQuery, 'SPLUNK CLIENT', 'info');
  return run1ceResolveAll.runOnceResolveAll(
    function() {
      var deferred = q.defer();

      if (typeof searchParams.latest_time === 'undefined') {
        delete searchParams.latest_time;
      }

      loginToService(host, login, password).
      then(function(service) {
        service.search(
          searchQuery,
          searchParams,
          function(err, job) {
            if (err) {
              log('error executing search: ' + JSON.stringify(err));
              deferred.reject(err);
            } else {
              job.track({
                period: 200
              }, {
                done: function(job) {
                  log('search "' + searchQuery + '" was successful', 'SPLUNK CLIENT', 'info');
                  job.results({
                    count: 50000
                  }, function(err, results, job) {
                    deferred.resolve(results.rows);
                    job.remove();
                  });

                },
                failed: function(job) {
                  log('Job/Search failed', 'SPLUNK CLIENT', 'error');
                  deferred.reject('Job/Search failed');
                  job.remove();
                },
                error: function(err) {
                  log(err, 'SPLUNK CLIENT', 'error');
                  deferred.reject(err);
                  job.remove();
                }
              });
            }
          }
        );

      }).
      fail(function(err) {
        deferred.reject(err);
      });

      return deferred.promise;
    },
    searchQuery + JSON.stringify(searchParams || {})
  );
}

function loginToService(hostname, login, password) {
  var service = new splunkjs.Service({
    username: login,
    password: password,
    host: hostname
  });

  var deferred = q.defer();

  service.login(function(err, result) {
    if (err) {
      deferred.reject(err);
    } else {
      deferred.resolve(service);
    }
  });

  return deferred.promise;
}

module.exports = SplunkETLDataSource;


//************************************************************************************************************************
//************************************************************************************************************************



// function test() {

//     var datasourceDefinition = {
//         hostname: 'splunk.runtv.com.au',
//         login: 'mathieu',
//         password: 'I8the4kin$',
//         search: {
//             query: 'search event="channelChange" | top limit=10 channelId',
//             params: {
//                 exec_mode: 'normal',
//                 earliest_time: -5
//             }
//         }
//     };

//     var datasource = new SplunkETLDataSource(datasourceDefinition);
//     return datasource.run().
//     then(console.log).
//     fail(console.log);
// }

// test();
