var _ = require('lodash');
var q = require('q');

var ETLobject = require('../base-ETL-object.js');
var ETLdataSource = require('../data-sources/base-ETL-data-source.js');


var ETLoperation = function(rawDefinition, inputs) {
    ETLobject.call(this, rawDefinition, inputs);
};

ETLoperation.prototype = Object.create(ETLobject.prototype);
ETLoperation.prototype.constructor = ETLoperation;


ETLoperation.prototype.getName = function() {
    return 'BaseOperation';
};

module.exports = ETLoperation;
