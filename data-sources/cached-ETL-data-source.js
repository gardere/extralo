var _ = require('lodash');
var q = require('q');

var ETLdataSource = require('./base-ETL-data-source.js');


var CachedETLDataSource = function(rawDefinition, inputs) {
    ETLdataSource.call(this, rawDefinition, inputs);
};

CachedETLDataSource.prototype = Object.create(ETLdataSource.prototype);
CachedETLDataSource.prototype.constructor = CachedETLDataSource;


CachedETLDataSource.prototype.getRequiredParams = function(operationDefinition) {
    return [{
    	name: 'cacheKey', 
    	accepted_types: ['string']
    }, {
    	name: 'ttl', 
    	accepted_types: ['number']
    }];
};

CachedETLDataSource.prototype.getName = function() {
    return 'CachedDataSource';
};

CachedETLDataSource.prototype.getInputPromises = function() {
    //Override default behaviour to prevent datasource to pe queried
    return q.when([]);
}


CachedETLDataSource.prototype.execute = function() {
    return require('../index.js').getCache().getFromCacheOrFetch(this.parsedDefinition.cacheKey, _.bind(this.inputs[0].run, this.inputs[0]), this.parsedDefinition.ttl);
};

module.exports = CachedETLDataSource;


//************************************************************************************************************************
//************************************************************************************************************************


// var httpDataSource = require('./http-ETL-data-source.js');

// function test() {

//     var googleHttpDataSource = new httpDataSource({
//         URL: 'http://www.google.com'
//     });

//     var datasourceDefinition = {
//         cacheKey: 'testCacheKey',
//         ttl: 5,
//         dataSource: googleHttpDataSource
//     };

//     var fetchFn = function () {
// 	    var datasource = new CachedETLDataSource(datasourceDefinition);
// 	    return datasource.run().
// 	    then(console.log).
// 	    fail(function(err) {
// 	        console.log('error: ' + err);
// 	    });
//     }

//     fetchFn()
//     .then(fetchFn)
//     .then(function () {
//     	setTimeout(function () { return fetchFn() }, 10000);
//     });
// }

// myCache.configure({
//     cacheType: 'memory'
// }).
// then(test);
