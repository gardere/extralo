var _ = require('lodash');
var q = require('q');
var fs = require('fs');

var ETLdataSource = require('./base-ETL-data-source.js');

var log = function() {};
// var log = console.log;


var FileETLDataSource = function(rawDefinition, inputs) {
    ETLdataSource.call(this, rawDefinition, inputs);
};

FileETLDataSource.prototype = Object.create(ETLdataSource.prototype);
FileETLDataSource.prototype.constructor = FileETLDataSource;


FileETLDataSource.prototype.getRequiredParams = function(operationDefinition) {
    return [{
        name: 'filePath',
        accepted_types: ['string']
    }];
};

FileETLDataSource.prototype.getName = function() {
    return 'FileDataSource';
};

FileETLDataSource.prototype.execute = function() {
    var deferred = q.defer();
    var self = this;

    fs.readFile(self.parsedDefinition.filePath, function(err, data) {
        if (err) {
            deferred.reject(err);
        } else {
            if (self.parsedDefinition.csv) {
                data = self.parseCSV(self.parsedDefinition.csv, new String(data));
            }
            deferred.resolve(data);
        }
    });

    return deferred.promise;
};

FileETLDataSource.prototype.parseCSV = function(options, data) {
    options = options || {};
    var result = [];
    var lines = data.split('\n');
    var fieldDelimiter = options.fieldDelimiter || ',';
    var numberOfLinesToIgnore = options.numberOfLinesToIgnore || 0;
    var objectFieldsDefinition;
    var numberOfFields;

    if (options.objectForm) {
        numberOfLinesToIgnore += 1;
        objectFieldsDefinition = lines[numberOfLinesToIgnore - 1].split(fieldDelimiter);
    }


    for (var i = numberOfLinesToIgnore; i < lines.length; i += 1) {
        var line = lines[i];
        var objectToAdd = line.split(fieldDelimiter);
        if (!numberOfFields) {
            numberOfFields = objectToAdd.length;
        }

        if (options.trim) {
            for (var j = 0; j < objectToAdd.length; j+= 1) {
                objectToAdd[j] = objectToAdd[j].trim();
            }
        }

        if (numberOfFields === objectToAdd.length) {
            if (options.objectForm) {
                objectToAdd = convertArrayToObject(objectFieldsDefinition, objectToAdd);
            }
            result.push(objectToAdd);
        }
    }

    return result;
}

function convertArrayToObject(objectFieldsDefinition, values) {
    var obj = {};

    for (var i = 0; i < objectFieldsDefinition.length; i += 1) {
        obj[objectFieldsDefinition[i]] = values[i];
    }

    return obj;
}

module.exports = FileETLDataSource;

//************************************************************************************************************************
//************************************************************************************************************************

// function test() {

//     var datasourceDefinition = {
//         filePath: './models.csv',
//         csv: {
//             numberOfLinesToIgnore: 0,
//             objectForm: true
//         }
//     };

//     var datasource = new FileETLDataSource(datasourceDefinition);
//     datasource.run().
//     then(console.log).
//     fail(function(err) {
//         console.log('error: ' + err);
//     });
// }

// test();
