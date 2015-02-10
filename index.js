var ETLPipelineParser = require('./ETL-pipeline-parser.js');

var cache;


module.exports.setCache = function(_cache) {
	cache = _cache;
}

module.exports.getCache = function() {
	return cache;
}

module.exports.parsePipeline = ETLPipelineParser.parseDescription;
module.exports.registerETLobject = ETLPipelineParser.registerETLobject;