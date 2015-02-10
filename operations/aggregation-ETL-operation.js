var _ = require('lodash');
var q = require('q');

var ETLoperation = require('./base-ETL-operation.js');

var AggregationETLOperation = function(definition, input) {
  ETLoperation.call(this, definition, input);
}

AggregationETLOperation.prototype = Object.create(ETLoperation.prototype);
AggregationETLOperation.prototype.constructor = AggregationETLOperation;

AggregationETLOperation.prototype.getRequiredParams = function() {
  return [{
    name: 'aggregate',
    accepted_types: ['object']
  }, {
    name: 'groupBy',
    accepted_types: ['string', 'number', 'object']
  }];
};

AggregationETLOperation.prototype.getName = function() {
  return 'AggregationOperation';
};

function getValueToGroupBy(valueObject, fieldsToGroupBy) {
  var values = [];

  for (var i = 0; i < fieldsToGroupBy.length; i += 1) {
    values.push(valueObject[fieldsToGroupBy[i]]);
  }

  return values;
}

function updateValue(results, key, value, valueToGroupBy) {
  var totalSum = 0;

  for (var lkey in results) {
    totalSum = results[lkey].totalSum;
  }

  if (typeof results[key] === 'undefined') {
    results[key] = {
      count: 0,
      sum: 0,
      totalSum: 0,
      valueToGroupBy: valueToGroupBy
    };
  }

  results[key].count++;
  results[key].sum += value;
  totalSum += value;

  for (var lkey in results) {
    results[lkey].totalSum = totalSum;
  }
}

function getMapForAggregate(aggregateDefinition) {
  switch (aggregateDefinition.type) {
    case 'sum':
      return function(results, key, value) {
        results[key].value = results[key].sum;
      };
      break;
    case 'count':
      return function(results, key, value) {
        results[key].value = results[key].count;
      };
      break;
    case 'average':
      return function(results, key, value) {
        results[key].value = results[key].sum / results[key].count;
      };
      break;
    case 'prc':
    case 'percentage':
      return function(results, key, value) {
        for (var lkey in results) {
          results[lkey].value = results[lkey].sum / results[lkey].totalSum * 100;
        }
      };
      break;
    case 'max':
    case 'maximum':
      return function(results, key, value) {
        if (typeof results[key].value === 'undefined') {
          results[key].value = value;
        }
        var numericComparison = ! (isNaN(value) || isNaN(results[key].value));
        var val1 = numericComparison ? parseFloat(value) : value;
        var val2 = numericComparison ? parseFloat(results[key].value) : results[key].value;
        results[key].value = (val1 > val2) ? val1 : val2;
      };
      break;
      case 'min':
      case 'minimum':
      return function(results, key, value) {
        if (typeof results[key].value === 'undefined') {
          results[key].value = value;
        }
        var numericComparison = ! (isNaN(value) || isNaN(results[key].value));
        var val1 = numericComparison ? parseFloat(value) : value;
        var val2 = numericComparison ? parseFloat(results[key].value) : results[key].value;
        results[key].value = (val1 < val2) ? val1 : val2;
      };
      break;
    default:
      throw 'unknown aggregate type: ' + this.parsedDefinition.aggregate.type;
  }
}

function wrapWithGetValue(fn, aggregateDefinition) {
  return function(results, key, row, valueToGroupBy) {
    var value = row[aggregateDefinition.property];
    updateValue(results, key, value, valueToGroupBy);
    return fn(results, key, value);
  }
}

AggregationETLOperation.prototype.execute = function(inputs) {
  //TODO: move this to 'check params' and refactor
  if (this.inputs.length > 1) {
    throw 'only 1 input is allowed for this operation';
  }

  var results = [];

  try {
    var maps = [];

    for (var i = 0; i < this.parsedDefinition.aggregate.length; i += 1) {
      maps.push(wrapWithGetValue(getMapForAggregate(this.parsedDefinition.aggregate[i]), this.parsedDefinition.aggregate[i]));
      results.push({});
    }

    for (var i = 0; i < inputs[0].length; i += 1) {
      var valueToGroupBy = getValueToGroupBy(inputs[0][i], this.parsedDefinition.groupBy);
      var keyForValueToGroupBy = valueToGroupBy.join('_');

      for (var j = 0; j < maps.length; j += 1) {
        maps[j](results[j], keyForValueToGroupBy, inputs[0][i], valueToGroupBy);
      }


      // var numValue = parseFloat(inputs[0][i][this.parsedDefinition.aggregate.property]);
      // var isNumericValue = !isNaN(value);
      // numValue = isNumericValue ? value : 0;

      // sums[keyForValueToGroupBy].sum += numValue;
      // sums[keyForValueToGroupBy].count++;
      // if ((isNumericValue ? numValue : value) > sums[keyForValueToGroupBy].maximum) {
      //   sums[keyForValueToGroupBy].maximum = (isNumericValue ? numValue : value);
      // }
      // if ((isNumericValue ? numValue : value) < sums[keyForValueToGroupBy].minimum) {
      //   sums[keyForValueToGroupBy].minimum = (isNumericValue ? numValue : value);
      // }
      // sums[keyForValueToGroupBy].average = totalSum / sums[keyForValueToGroupBy].count;
    }
  } catch (err) {
    console.log(err);
    console.log(err.trace);
    console.log(err.stack);
  }

  var result = [];

  var keys = _.keys(results[0])

  for (var i = 0; i < keys.length; i += 1) {
    var key = keys[i];
    var resultRow = [results[0][key].valueToGroupBy];
    for (var j = 0; j < results.length; j++) {
      resultRow.push(results[j][key].value);
    };
    result.push(_.flatten(resultRow, true));
  };


  return result;
};

AggregationETLOperation.prototype.parseDefinition = function() {
  ETLoperation.prototype.parseDefinition.call(this);
  if (typeof this.parsedDefinition.groupBy !== 'object') {
    this.parsedDefinition.groupBy = [this.parsedDefinition.groupBy];
  }

  if (typeof this.parsedDefinition.aggregate.push === 'undefined') {
    this.parsedDefinition.aggregate = [this.parsedDefinition.aggregate];
  }
};

module.exports = AggregationETLOperation;



// ************************************************************************************************************************
// ************************************************************************************************************************

// function test() {

//   var operationDefinition = {
//     aggregate: {
//       property: 2,
//       type: 'max'
//     },
//     groupBy: [3, 4]
//   };

//   var data = [
//     [0, 32, 34, 45, 33, 11],
//     [0, 32, 322, 45, 33, 11],
//     [0, 32, 34, 36, 33, 11],
//     [0, 32, 34, 45, 34, 11],
//     [0, 32, 35, 25, 33, 11],
//     [0, 32, 5, 36, 33, 11]
//   ];

//   var operation = new AggregationETLOperation(operationDefinition, function() {
//     return data;
//   });
//   operation.run().
//   then(function(result) {
//     console.log(JSON.stringify(result));
//   });
// }

// test();
