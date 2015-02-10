var _ = require('lodash');
var q = require('q');

var ETLoperation = require('./base-ETL-operation.js');


var JoinETLOperation = function(definition, input) {
    ETLoperation.call(this, definition, input);
}

JoinETLOperation.prototype = Object.create(ETLoperation.prototype);
JoinETLOperation.prototype.constructor = JoinETLOperation;


JoinETLOperation.prototype.getRequiredParams = function() {
    return [{
        name: 'input1Field',
        accepted_types: ['string', 
        'number']
    }, {

        name: 'input2Field',
        accepted_types: ['string', 
        'number']
    }, {
        name: 'operator',
        accepted_types: ['string']
    }];
};

JoinETLOperation.prototype.getName = function() {
    return 'JoinOperation';
};

function isArray(obj) {
    return (Object.prototype.toString.call(obj) === '[object Array]');
}

JoinETLOperation.prototype.execute = function(inputs) {
    //TODO: move this to 'check params' and refactor
    if (inputs.length !== 2) {
        throw '2 inputs are required for this operation';
    }

    var input1 = inputs[0];
    var input2 = inputs[1];
    var results = [];

    var arrays = false;
    var objects = false;

    if ((input1.length > 0) && (input2.length > 0)) {
        objects = !(isArray(input1[0]) || isArray(input2[0]));
        arrays = isArray(input1[0]) && isArray(input2[0]);
    }

    for (var i = 0; i < input1.length; i += 1) {
        for (var j = 0; j < input2.length; j += 1) {
            if (input1[i][this.parsedDefinition.input1Field] == input2[j][this.parsedDefinition.input2Field]) {
                if (arrays) {
                    results.push(_.flatten([input1[i], input2[j]]));
                } else if (objects) {
                    results.push(_.extend({}, input1[i], input2[j]));
                }
            }
        }
    }

    return results;
};

module.exports = JoinETLOperation;




//************************************************************************************************************************
//************************************************************************************************************************

// function test() {

//     var operationDefinition = {
//         input1Field: 0,
//         input2Field: 0,
//         operator: 'equals'
//     };

//     var data1 = [
//         [0, 32, 34, 45, 33, 11],
//         [1, 32, 322, 45, 33, 11],
//         [2, 32, 34, 36, 33, 11],
//         [3, 32, 34, 45, 33, 11],
//         [4, 32, 35, 25, 33, 11],
//         [5, 32, 5, 36, 33, 11]
//     ];

//     var data2 = [
//         [0, 32, 34, 45, 33, 11],
//         [6, 32, 322, 45, 33, 11],
//         [7, 32, 34, 36, 33, 11],
//         [8, 32, 34, 45, 33, 11],
//         [9, 32, 35, 25, 33, 11],
//         [5, 32, 5, 36, 33, 11]
//     ];

//     var operation = new JoinETLOperation(operationDefinition, [function() {
//         return data1;
//     }, function() {
//         return data2;
//     }]);
//     operation.run().
//     then(function(result) {
//         console.log(JSON.stringify(result));
//     });
// }

// test();
