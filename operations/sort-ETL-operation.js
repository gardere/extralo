var _ = require('lodash');
var q = require('q');

var ETLoperation = require('./base-ETL-operation.js');


var SortETLOperation = function(definition, input) {
    ETLoperation.call(this, definition, input);
}

SortETLOperation.prototype = Object.create(ETLoperation.prototype);
SortETLOperation.prototype.constructor = SortETLOperation;


SortETLOperation.prototype.getRequiredParams = function() {
    return [{
        name: 'sortField',
        accepted_types: ['string', 'number']
    }, {

        name: 'sortOrder',
        accepted_types: ['string']
    }];
};

SortETLOperation.prototype.getName = function() {
    return 'SortOperation';
};

SortETLOperation.prototype.execute = function(inputs) {
    //TODO: move this to 'check params' and refactor
    if (inputs.length !== 1) {
        throw '1 input is required for this operation';
    }

    var input = inputs[0];

    var results = _.sortBy(input, _.bind(function(result) { 
        try { 
            return parseFloat(result[this.parsedDefinition.sortField]); 
        }
        catch (err)  {
            return result[this.parsedDefinition.sortField];
        }
    }, this));
    if ((this.parsedDefinition.sortOrder === 'desc') || (this.parsedDefinition.sortOrder === 'descending') || (this.parsedDefinition.sortOrder === 'descendant')) {
        results.reverse();
    }
    return results;
};

module.exports = SortETLOperation;

