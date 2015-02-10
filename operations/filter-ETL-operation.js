var _ = require('lodash');
var q = require('q');

var ETLoperation = require('./base-ETL-operation.js');


var FilterETLOperation = function(definition, input) {
    ETLoperation.call(this, definition, input);
}

FilterETLOperation.prototype = Object.create(ETLoperation.prototype);
FilterETLOperation.prototype.constructor = FilterETLOperation;


FilterETLOperation.prototype.getRequiredParams = function() {
    return [{
        name: 'filterField',
        accepted_types: ['string', 
        'number']
    }, {
        name: 'operator',
        accepted_types: ['string']
    }, {
        name: 'filterValue',
        accepted_types: ['string', 'object', 
        'number']
    }];
};

FilterETLOperation.prototype.getName = function() {
    return 'FilterOperation';
};

FilterETLOperation.prototype.execute = function(inputs) {
    //TODO: move this to 'check params' and refactor
    if (inputs.length !== 1) {
        throw '1 input is required for this operation';
    }

    var comparisonMethod;
    switch(this.parsedDefinition.operator) {
        case 'equals': 
        case 'eq': 
            comparisonMethod = function (a, b) {
                return a == b;
            }
            break;
        case 'lt': 
        case 'lowerthan': 
        case 'lower than': 
        case 'lower': 
            comparisonMethod = function (a, b) {
                return a < b;
            }
            break;
        case 'gt': 
        case 'greaterthan': 
        case 'greater than': 
        case 'greater': 
            comparisonMethod = function (a, b) {
                return a > b;
            }
            break;
        case 'lte': 
        case 'lower than or equal': 
        case 'lower than or equal to': 
        case 'lowerequal': 
        case 'lower or equal': 
            comparisonMethod = function (a, b) {
                return a <= b;
            }
            break;
        case 'gte': 
        case 'greater than or equal': 
        case 'greater than or equal to': 
        case 'greaterequal': 
        case 'greater or equal': 
            comparisonMethod = function (a, b) {
                return a >= b;
            }
            break;
        case 'in':
            comparisonMethod = function (a, b) {
                return b.indexOf(a) > -1;
            }
            break;
        default: 
            throw 'unknown operator';
    }

    var results = [];

    for (var i = 0; i < inputs[0].length; i += 1) {
        if (comparisonMethod(inputs[0][i][this.parsedDefinition.filterField], this.parsedDefinition.filterValue)) {
            results.push(inputs[0][i]);
        }
    }

    return results;
};

module.exports = FilterETLOperation;




//************************************************************************************************************************
//************************************************************************************************************************

// function test() {

//     var operationDefinition = {
//         filterField: 2,
//         operator: 'lt',
//         filterValue: 35
//     };

//     var data1 = [
//         [0, 32, 34, 45, 33, 11],
//         [1, 32, 322, 45, 33, 11],
//         [2, 32, 34, 36, 33, 11],
//         [3, 32, 34, 45, 33, 11],
//         [4, 32, 35, 25, 33, 11],
//         [5, 32, 5, 36, 33, 11]
//     ];



//     var operation = new FilterETLOperation(operationDefinition, [function() {
//         return data1;
//     }]);
//     operation.run().
//     then(function(result) {
//         console.log(JSON.stringify(result));
//     });
// }

// test();
