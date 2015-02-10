var _ = require('lodash');
var q = require('q');


var ETLobject = require('../base-ETL-object.js');

var ETLdataSource = function(rawDefinition, inputs) {
    ETLobject.call(this, rawDefinition, inputs);
};

ETLdataSource.prototype = Object.create(ETLobject.prototype);
ETLdataSource.prototype.constructor = ETLdataSource;



ETLdataSource.prototype.execute = function() {
	throw 'Not implemented - base object';
}

ETLdataSource.prototype.getName = function() {
	return 'BaseDataSource';
}

ETLdataSource.prototype.convertDictionaryToArrayOfObjects = function(keyFieldName, dictionary) {
    var array = [];
    for (var key in dictionary) {
        var newObject = _.extend({}, dictionary[key]);
        newObject[keyFieldName] = key;
        array.push(newObject);
    }
    return array;
}

// ETLdataSource.prototype.run = function() {
// 	return ETLobject.prototype.run.call(this);
// }

module.exports = ETLdataSource;
