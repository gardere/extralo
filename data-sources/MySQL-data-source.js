var ETLdataSource = require('./base-ETL-data-source.js');
var q = require('q');
var mysql = require('mysql');

var log = console.log;
// var log = function() {};


var MySQLDataSource = function(rawDefinition, inputs) {
  ETLdataSource.call(this, rawDefinition, inputs);
};

MySQLDataSource.prototype = Object.create(ETLdataSource.prototype);
MySQLDataSource.prototype.constructor = MySQLDataSource;


MySQLDataSource.prototype.getRequiredParams = function(operationDefinition) {
  return [{
    name: 'query',
    accepted_types: ['string']
  }, {
    name: 'connectionDetails',
    accepted_types: ['object']
  }];
};

MySQLDataSource.prototype.getName = function() {
  return 'MySQLDataSource';
};


MySQLDataSource.prototype.execute = function() {
  var connection = mysql.createConnection(this.parsedDefinition.connectionDetails);
  var deferred = q.defer();

  connection.connect();

  connection.query(this.parsedDefinition.query, this.parsedDefinition.params, function(err, result, fields) {
    if (err) {
      log('error: ' + err);
      deferred.reject(err);
    } else {
      log(result.length + ' rows found');
      deferred.resolve(result, fields);
      connection.end();
    }
  });
  return deferred.promise;
}

module.exports = MySQLDataSource;
