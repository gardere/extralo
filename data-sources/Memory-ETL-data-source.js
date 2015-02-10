var ETLdataSource = require('./base-ETL-data-source.js');
var q = require('q');

// var log = console.log;
var log = function() {};


var MemoryDataSource = function(rawDefinition, inputs) {
    ETLdataSource.call(this, rawDefinition, inputs);
};

MemoryDataSource.prototype = Object.create(ETLdataSource.prototype);
MemoryDataSource.prototype.constructor = MemoryDataSource;


MemoryDataSource.prototype.getRequiredParams = function(operationDefinition) {
    return [{
    	name: 'object', 
    	accepted_types: ['object']
    }];
};

MemoryDataSource.prototype.getName = function() {
    return 'MemoryDataSource';
};


MemoryDataSource.prototype.execute = function() {
	return q.when(this.parsedDefinition.object);
}

module.exports = MemoryDataSource;