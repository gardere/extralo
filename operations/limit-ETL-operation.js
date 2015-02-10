var _ = require('lodash');
var q = require('q');

var ETLoperation = require('./base-ETL-operation.js');


var LimitETLOperation = function(definition, input) {
    ETLoperation.call(this, definition, input);
}

LimitETLOperation.prototype = Object.create(ETLoperation.prototype);
LimitETLOperation.prototype.constructor = LimitETLOperation;


LimitETLOperation.prototype.getRequiredParams = function() {
    return [{
        name: 'number',
        accepted_types: ['number']
    }];
};

LimitETLOperation.prototype.getName = function() {
    return 'LimitOperation';
};

LimitETLOperation.prototype.execute = function(inputs) {
    //TODO: move this to 'check params' and refactor
    if (inputs.length !== 1) {
        throw '1 input is required for this operation';
    }

    return _.first(inputs[0], this.parsedDefinition.number);
};

module.exports = LimitETLOperation;