var fs = require('fs');
var path = require('path');
var _ = require('lodash');
var registeredETLobjects = {};
var inited = false;

//var log = function () {};
var log = console.log;

var dataOperationsNormalizedPath = path.join(__dirname, "data-operations");

function registerETLobject(pathComponent, file) {
  var object = require("./" + pathComponent + "/" + file);
  var objectName = (new object()).getName();
  registeredETLobjects[objectName] = object;
  log('registered ' + objectName);
}

function registerETLobjects(pathComponent) {
  var normalizedPath = path.join(__dirname, pathComponent);
  fs.readdirSync(normalizedPath).forEach(_.bind(registerETLobject, this, pathComponent));
}

function init() {
  if (!inited) {
    registerETLobjects("data-sources");
    registerETLobjects("operations");
    inited = true;
  }
}

function isArray(obj) {
  return (Object.prototype.toString.call(obj) === '[object Array]');
}

function parseDescription(description) {
  init();

  if (typeof description === 'string') {
    description = JSON.parse(description);
  }

  if (!isArray(description)) {
    description = [description];
  }

  var previousStep;
  var currentStep;

  for (var i = 0; i < description.length; i += 1) {
    var step = description[i];

    if (isArray(step)) {
      currentStep = [];
      for (var j = 0; j < step.length; j += 1) {
        currentStep.push(parseDescription(step[j]))
      }
    } else {
      var stepType = _.keys(step)[0];
      log('Parsing step "' + stepType + '"');
      var currentStep;
      try {
        currentStep = new registeredETLobjects[stepType](step[stepType]);
      } catch (err) {
        log('unknown type' + stepType);
        throw err;
      }
      currentStep.inputs = previousStep;
    }

    previousStep = currentStep;
  }

  return currentStep;
}

module.exports.registerETLobject = registerETLobject;
module.exports.parseDescription = parseDescription;
