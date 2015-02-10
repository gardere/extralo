var _ = require('lodash');
var q = require('q');
var http = require('http');
var querystring = require('querystring');

var ETLdataSource = require('./base-ETL-data-source.js');

// var log = function () {};
var log = console.log;

var AUTH_ID = '00:07:67:E1:3C:3B';
var AUTH_TOKEN = '6m8w8a88qb';
var MW_HOST = 'mw.fetchtv.com.au';

var channelArrayObjectDefinition = ['name', 'image', 'id', 'type'];

var FetchTvETLDataSource = function(rawDefinition, inputs) {
    ETLdataSource.call(this, rawDefinition, inputs);
};

FetchTvETLDataSource.prototype = Object.create(ETLdataSource.prototype);
FetchTvETLDataSource.prototype.constructor = FetchTvETLDataSource;


FetchTvETLDataSource.prototype.getRequiredParams = function(operationDefinition) {
    return [{
        name: 'name',
        accepted_types: ['string']
    }];
};

FetchTvETLDataSource.prototype.getName = function() {
    return 'FetchtvDataSource';
};

FetchTvETLDataSource.prototype.execute = function() {
    switch (this.parsedDefinition.name) {
        case 'channels':
            var channelsPromise = getAuthCookie().
            then(getChannels).
            then(flattenChannelsList);

            if (this.parsedDefinition.objectsForm) {
                return channelsPromise;
            } else {
                return channelsPromise.then(channelsToArray);
            }

            break;
        case 'titles':
            return getTitlesList();
            break;
        default:
            throw 'unknown fetch data name: ' + this.parsedDefinition.name;
    }
};

function objectToArray(obj, def) {
    var array = [];
    _.each(def, function(fieldName) {
        array.push(obj[fieldName]);
    })
    return array
}

function channelsToArray(channels) {
    var results = [];

    _.each(channels, function(channel) {
        results.push(objectToArray(channel, channelArrayObjectDefinition));
    });

    return results;
}

function flattenChannelsList(channels) {
    var channelsDataObjects = [];


    for (var channelId in channels) {
        var channelData = channels[channelId]; 
        channelData.realChannelId = channelId;
        var channelSources = channelData.sources;
        delete channelData.sources;
        for (j = 0; j < channelSources.length; j += 1) {
            var channelSource = channelSources[j];
            channelsDataObjects.push(_.extend(channelData, channelSource));
        }
    }

    return channelsDataObjects;
}

function getAuthCookie() {
    var deferred = q.defer();

    var post_data = querystring.stringify({
        auth_id: AUTH_ID,
        auth_token: AUTH_TOKEN
    });

    var req = http.request({
        hostname: MW_HOST,
        port: 7777,
        path: '/v2/authenticate',
        method: 'POST',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Content-Length': post_data.length
        }
    }, function(res) {
        var resp = '';

        res.setEncoding('utf8');

        res.on('data', function(data) {
            resp += data;
        });

        res.on('end', function() {
            deferred.resolve(res.headers['set-cookie'].join(';'));
        });
    });

    req.on('error', function(e) {
        log('problem with request: ' + e.message);
        deferred.reject(e);
    });

    req.write(post_data);
    req.end();

    return deferred.promise;
}

function getTitlesList() {
    return getAuthCookie().
    then(getCatalogue).
    then(function(catalogue) {
        var titlesFlatList = [];

        _.each(catalogue.titles, function(obj, key) {
            var vodType = obj.price_rule ? 'TVOD' : 'SVOD';
            titlesFlatList.push(_.flatten([key, obj.title, vodType]));
        });

        return titlesFlatList;
    });
}

function getCatalogue(authCookie) {
    log('requested catalogue');
    var deferred = q.defer();

    var req = http.request({
        hostname: MW_HOST,
        port: 7777,
        path: '/v2/vod/catalogue',
        method: 'GET',
        headers: {
            'Cookie': authCookie
        }
    }, function(res) {
        res.setEncoding('utf8');
        var catalogue = '';
        res.on('data', function(data) {
            catalogue += data;
        });
        res.on('end', function(data) {
            log('returning catalogue');
            deferred.resolve(JSON.parse(catalogue));
        });
    });

    req.on('error', function(e) {
        console.log('problem with request: ' + e.message);
        deferred.reject(e);
    });

    req.end();

    return deferred.promise;
}

function getChannels(authCookie) {
    log('requested channels');
    var deferred = q.defer();

    var options = {
        hostname: MW_HOST,
        port: 7777,
        path: '/v2/epg/channels',
        method: 'GET',
        headers: {
            'Cookie': authCookie
        }
    };

    var req = http.request(options, function(res) {
        var responseData = '';
        res.on('data', function(data) {
            responseData += data;
        });
        res.on('end', function() {
            var JSONChannels;
            try {
                JSONChannels = JSON.parse(responseData).channels;
                // addChannelTypes(JSONChannels);
            } catch (err) {
                log('error getting channels: ' + err);
                deferred.reject(err);
            }
            deferred.resolve(JSONChannels);
            log('returned channels');
        });
    });

    req.on('error', function(e) {
        log('problem with request: ' + e.message);
        deferred.reject(e);
    });

    req.end();

    return deferred.promise;
}


module.exports = FetchTvETLDataSource;


//************************************************************************************************************************
//************************************************************************************************************************



// function test() {

//     var datasource1 = new FetchTvETLDataSource({
//         name: 'channels'
//     });

//     var datasource2 = new FetchTvETLDataSource({
//         name: 'titles'
//     });

//     datasource1.run().
//     then(console.log).
//     fail(console.log);

//     // datasource2.run().
//     // then(console.log).
//     // fail(console.log);

// }

// test();
