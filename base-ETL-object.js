var q = require('q');
var _ = require('lodash');

var log = console.log;

var ETLobject = function(rawDefinition, inputs) {
    this.rawDefinition = rawDefinition;
    this.parsedDefinition = null;
    this.inputs = inputs;
};

ETLobject.prototype.constructor = ETLobject;

ETLobject.prototype.parseDefinition = function() {
    if (typeof this.rawDefinition === 'string') {
        try {
            this.parsedDefinition = JSON.parse(this.rawDefinition);
        } catch (err) {
            throw 'Error: could not parse definition: \'' + this.rawDefinition + '\'';
        }
    } else if (typeof this.rawDefinition === 'object') {
        this.parsedDefinition = this.rawDefinition;        
    } else {
        throw 'Error: operation definition should be an object';
    }

    var params = this.getRequiredParams();
    for (var i=0; i < params.length; i+= 1) {
        var requiredParam = params[i];
        this.checkParamExists(requiredParam.name, requiredParam.accepted_types);
    }
};

ETLobject.prototype.getInputPromises = function() {
    var promises = [];

    for (var i = 0; i < this.inputs.length; i++) {
        var input = this.inputs[i];
        if (typeof input === 'function') {
            input = input.call(this);
        } else if (input instanceof ETLobject) {
            input = input.run.call(input);
        }
        promises.push(q.when(input));
    };

    return q.all(promises);
}

ETLobject.prototype.run = function() {
    this.parseDefinition();

    this.inputs = this.inputs || [];

    if (((typeof this.inputs === 'function') || (typeof this.inputs === 'object')) && (Object.prototype.toString.call(this.inputs) !== '[object Array]')){
        this.inputs = [this.inputs];
    }

    return this.getInputPromises().
    then(_.bind(this.execute, this));
};

ETLobject.prototype.getRequiredParams = function() {
    throw 'getRequiredParams not implemented -- ETLobject is an abstract object -- should be implemented in ' + this.getName();
};

ETLobject.prototype.checkParamExists = function(paramName, possibleParamTypes) {
    var paramValue = eval('this.parsedDefinition.' + paramName);

    //check param is present in operation definition
    if (typeof(paramValue) === 'undefined') {
        throw 'Param \'' + paramName + '\' required for operation ' + this.getName();
    }

    //check param is of right type
    var paramType = typeof paramValue;
    if (possibleParamTypes.indexOf(paramType) === -1) {
        throw 'Incorrect param type \'' + paramType + '\' for ' + paramName + '. Possible type(s) are: ' + possibleParamTypes.join();
    }
};

module.exports = ETLobject;
