var ETLdataSource = require('./base-ETL-data-source.js');
var http = require('http');
var url = require('url');
var q = require('q');

// var log = console.log;
var log = function() {};


var HttpETLDataSource = function(rawDefinition, inputs) {
    ETLdataSource.call(this, rawDefinition, inputs);
};

HttpETLDataSource.prototype = Object.create(ETLdataSource.prototype);
HttpETLDataSource.prototype.constructor = HttpETLDataSource;


HttpETLDataSource.prototype.getRequiredParams = function(operationDefinition) {
    return [{
    	name: 'URL', 
    	accepted_types: ['string']
    }];
};

HttpETLDataSource.prototype.getName = function() {
    return 'HttpDataSource';
};


HttpETLDataSource.prototype.execute = function() {
    log('fetching ' + this.parsedDefinition.URL + ' ', 'HTTP ETL data source', 'INFO');
    var deferred = q.defer();

    var parsedUrl = url.parse(this.parsedDefinition.URL);

    var requestDetails = {
        hostname: parsedUrl.hostname,
        port: parsedUrl.port || 80,
        path: parsedUrl.pathname,
        method: this.parsedDefinition.method || 'GET'
    };

    var req = http.request(requestDetails, function(res) {
        res.setEncoding('utf8');
        var respData = '';
        res.on('data', function(data) {
            respData += data;
        });
        res.on('end', function(data) {
            try {
                respData = JSON.parse(respData);
            } catch (err) {
                log('data is not JSON formatted');
            }
            deferred.resolve(respData);
        });
    });

    req.on('error', function(e) {
        deferred.reject(e);
    });

    req.end();

    return deferred.promise;
}

module.exports = HttpETLDataSource;


//************************************************************************************************************************
//************************************************************************************************************************

// function test() {

//     var datasourceDefinition = {
//         URL: 'http://www.google.com'
//     };

//     var datasource = new HttpETLDataSource(datasourceDefinition);
//     datasource.run().
//     then(console.log).
//     fail(function (err) {
//         console.log('error: ' + err);
//     });
// }

// test();
