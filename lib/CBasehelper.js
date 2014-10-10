'use strict';

var async       = require('async');
var HTTPStatus  = require('http-status');
var couchbase   = require('couchbase');
var uuid        = require('uuid');

var dbc         = require('dbc');
var openiLogger = require('openi-logger');
var zmq         = require('m2nodehandler');
var openiUtils  = require('openi-cloudlet-utils');
var createViews = require('./couchbaseViews');
var conf        = require('./config.js');

var logger      = null;
var dbs         = {};


var init = function (logger_params) {
  logger = openiLogger(logger_params);

  dbs['openi'] = new couchbase.Connection({host: 'localhost:8091', bucket: 'openi'}, function(err){
    if (err) {
      console.log('Connection Error', err);
    } else {
      console.log('DAO: Connected openi bucket!');
    }
  });

  dbs['attachments'] = new couchbase.Connection({host: 'localhost:8091', bucket: 'attachments'}, function(err){
    if (err) {
      console.log('Connection Error', err);
    } else {
      console.log('DAO: Connected attachments bucket!');
    }
  });

  createViews(dbs['openi'], logger);
};


var getCouchbaseError = function(err, documentName, id) {

  switch (err.code){
    case 4:
      return 'Object too big';
    case 13:
      return 'Entity with that id does not exist';
    case 12:
      if (0 === documentName.indexOf('t_')) {
        return 'OPENi Type already exists (' + id + ').';
      }
      return 'Incorrect revision number provided';
    default:
      return "Reason unknown" + err.code;
    }
};


var getDb = function(name) {
  switch(name) {
    case "attachments":
      return dbs['attachments'];
    default:
      return dbs['openi'];
  }
};


var postAction = function (msg, callback) {

  var options = (undefined     === msg.format) ? {} : {format: msg.format };
  var db_use  = getDb(msg['bucket']);

  db_use.add(msg.database, msg.object_data, options, function (err, result) {
    if (err) {
      var httpCode = (12 === err.code) ? HTTPStatus.CONFLICT : HTTPStatus.INTERNAL_SERVER_ERROR;
      callback(null, { 'error' : 'Error adding entity: ' + getCouchbaseError(err, msg.database, msg.id) }, httpCode);
    } else {
      callback(null, { 'id' : msg.id }, HTTPStatus.CREATED);
    }
  });
};


var putAction = function (msg, callback) {

  var db_use  = getDb(msg['bucket']);

  db_use.get(msg.database, function (err, db_body) {

    var revisionParts = msg.revision.split('-');

    if (db_body.cas['0'] !== revisionParts[0] && db_body.cas['1'] !== revisionParts[1]) {
      callback(null, { 'error' : 'Entity already updated'}, HTTPStatus.CONFLICT);
      return;
    }

    if (db_body.value.openi_type === msg.object_data.openi_type && db_body.cas['1'] !== revisionParts[1]) {
      callback(null, { 'error' : 'Entity already updated'}, HTTPStatus.CONFLICT);
      return;
    }

    var options = (undefined === msg.format) ? {} : {format: msg.format };

    db_use.set(msg.database, msg.object_data, options, function (err,result) {

      if (err) {
        logger.log('error', err );

        var httpCode = (12 === err.code) ? HTTPStatus.CONFLICT : HTTPStatus.INTERNAL_SERVER_ERROR;

        callback(null, { 'error' : 'Error updating entity: ' + getCouchbaseError(err) }, httpCode);
      } else {
        callback(null, { 'id' : msg.id }, HTTPStatus.OK);
      }
    });
  });
};

var recurseReplaceSubRef = function(sub, subs) {

  for (var i in sub['@data']) {
    if(sub['@data'].hasOwnProperty(i)) {
      var val = sub['@data'][i];
      if (-1 !== val.indexOf('o_')) {
        for( var j in subs) {
          if(subs.hasOwnProperty(j)) {
            var subrep = subs[j];
            if (subrep.id === val) {
              var o = recurseReplaceSubRef(subrep.value, subs);
              sub['@data'][i] = o['@data'];
            }
          }

        }
      }
    }
  }

  return sub;
};

var getAction = function (msg, callback) {

  var db_use  = getDb(msg['bucket']);

  db_use.get(msg.database, function (err, db_body) {

    if (err) {
      logger.log('error', err );
      callback(null, { 'error' : 'Error getting data from datastore' }, HTTPStatus.INTERNAL_SERVER_ERROR);
    } else {
      processGetData(db_body, msg, callback);
    }
  });
};

//Takes in database response, iterates over the objects body looking for references to sub objects,
//creates an array of sub objects and RECURSIVLY requests subobjects until all are stored on the message.subids
//array. The original object is then populated
var processSubObjects = function(db_body, msg, callback) {

  var  obj = openiUtils.objectHelper(db_body, msg);
  var i;
  var sub;

  if (undefined === msg.resolve || msg.resolve === false) {
    //forward message to get with current object and say replace..... recursive call.... dangerous!!!
    //loop through find object references;
    callback(null, obj, HTTPStatus.OK, zmq.standard_headers.json );
  } else {
    var new_msg = {};
    new_msg.subIds      = (undefined === msg.subIds     ) ? []  : msg.subIds;
    new_msg.originalObj = (undefined === msg.originalObj) ? obj : msg.originalObj;
    new_msg.resolve     = true;


    for (i in obj['@data']) {
      if(obj['data'].hasOwnProperty(i)) {
        var val = obj['@data'][i];
        if (-1 !== val.indexOf('o_')) {
          new_msg.subIds.push({'key' : i, 'id': val});
        }
      }
    }

// TODO: sort out replication of these two loops
    for (i in new_msg.subIds) {
      if(new_msg.subIds.hasOwnProperty(i)) {
        sub = new_msg.subIds[i];
        if (sub.id === obj['@id']) {
          new_msg.subIds[i].value = obj;
        }
      }
    }


    for (i in new_msg.subIds) {
      if(new_msg.subIds.hasOwnProperty(i)) {
        sub = new_msg.subIds[i];
        if (undefined === sub.value) {
          var cloudletId   = msg.database.split('+')[0];
          new_msg.action   = 'GET';
          new_msg.database = cloudletId + '+' + sub.id;
          getAction(new_msg, callback);
          return;
        }
      }
    }

    var oobj = recurseReplaceSubRef(msg.originalObj, new_msg.subIds);

    callback(null, oobj, HTTPStatus.OK, zmq.standard_headers.json );
  }
};





var processGetData = function(db_body, msg, callback) {

  var headers = zmq.standard_headers.json;
  var resp = null;

  switch(msg['resp_type']) {
    case 'cloudlet':
      resp = openiUtils.cloudletHelper(db_body, msg);
      break;
    case 'binary':
      headers  = {'Content-Type': db_body.value["Content-Type"]};
      resp = new Buffer(db_body.value['file'], 'base64');
      break;
    case 'binary_meta':
      db_body.value['file'] = undefined;
      resp = db_body.value;
      break;
    case 'type':
      resp = openiUtils.typeHelper(db_body, msg);
      break;
    case 'object':
    default:
      processSubObjects(db_body, msg, callback);
      break;
  }

  if (null !== resp) {
    callback(null, resp, HTTPStatus.OK, headers );
  }
};


var getOrPostAction = function (msg, callback) {

  var db_use  = getDb(msg['bucket']);

  db_use.get(msg.database, function (err, db_body) {

    if (err) {
      postAction(msg, callback);
    } else {
      processGetData(db_body, msg, callback);
    }
  });
};





var viewAction = function (msg, callback) {

  msg.count    = (typeof msg.count !== 'number' || isNaN(msg.count) || msg.count > 30) ? 30 : msg.count;
  msg.skip     = (typeof msg.skip  !== 'number' || isNaN(msg.skip))                    ? 0  : msg.skip;
  msg.startkey = typeof msg.startkey !== 'string' ? "" : msg.startkey;

  var params = {
    startkey_docid : msg.startkey,
    skip           : msg.skip,
    stale          : false,
    limit          : msg.count
  };

  if (undefined !== msg.key) {
    params.startkey = msg.key;
    params.endkey   = msg.key;
  }

  if ( undefined !== msg.group && msg.group ) {
    params.group = msg.group;
  }

  getDb('openi').view(msg.design_doc, msg.view_name, params).query(function(err, res){
    var respObj = [];

    if (err) {
      logger.log('error', err );
      callback(null, { 'error' : 'Error getting view: ' + getCouchbaseError(err) },
            HTTPStatus.INTERNAL_SERVER_ERROR );
    } else if(res !== null) {
      for (var i = 0; i < res.length; i++) {
        if (msg.resp_type === 'type') {
          respObj[i] = openiUtils.typeHelper(res[i], msg);
        } else if (msg.resp_type === 'type_stats') {
          respObj[i] = openiUtils.typeStats(res[i], msg);
        } else if (msg.resp_type === 'cloudlet') {
          respObj[i] = openiUtils.cloudletHelper(res[i], msg);
        } else {
          respObj[i] = openiUtils.objectHelper(res[i], msg);
        }
      }
    }

    callback(null,  respObj, HTTPStatus.OK);
  });
};


var fetchAction = function (msg, callback) {

  getDb('openi').get(msg.database, function (err, db_body) {
    if (err) {
      logger.log('error', err);
      callback(null, { 'error' : 'Error getting entity: ' + getCouchbaseError(err) },
               HTTPStatus.INTERNAL_SERVER_ERROR );
    } else {
      callback(null,  { 'data' : db_body }, HTTPStatus.OK);
    }
  });
};


var createDBAction = function(msg, callback ) {

  getDb('openi').add(msg.database, {}, function(err, res) {

    if(err && err.code !== 12) {
      //logger.log('error', err)
    	callback(null, { 'error' : 'Error creating entity: ' + getCouchbaseError(err) },
            HTTPStatus.INTERNAL_SERVER_ERROR );
    } else {
      logger.log('debug', 'Cloudlet created, id: ' + msg.database );
      callback(null, { 'id' : msg.database }, HTTPStatus.OK);
    }
	});
};


var deleteDBAction = function( msg, callback ) {

  getDb(msg['bucket']).remove(msg.database, function (err) {
    if (err) {
      logger.log('error', err);
      callback(null, { 'error' : 'Error deleting entity : ' + getCouchbaseError(err) }, HTTPStatus.NOT_FOUND);
    } else {
      logger.log('debug', 'entity deleted, id: ' + msg.database );
      callback(null, { 'id' : msg.id }, HTTPStatus.OK);
    }
  });
};


var evaluateAction = function (msg, callback) {

  dbc.conditionalHasMember(msg, 'database', (msg.action !== 'VIEW') );
  dbc.hasMemberIn(         msg, 'action',      ['POST', 'PUT', 'GET', 'FETCH', 'CREATE', 'DELETE', 'VIEW', 'GET_OR_POST']);
  dbc.conditionalHasMember(msg, 'object_data', (msg.action === 'POST'   || msg.action === 'PUT'));
  dbc.conditionalHasMember(msg, 'revision',    (msg.action === 'PUT'));

  var resp_msg = '';


  switch (msg.action) {
    case 'POST':
      resp_msg = postAction(msg, callback);
      break;
    case 'PUT':
      resp_msg = putAction(msg, callback);
      break;
    case 'GET':
      resp_msg = getAction(msg, callback);
      break;
    case 'GET_OR_POST':
      resp_msg = getOrPostAction(msg, callback);
      break;
    case 'FETCH':
      resp_msg = fetchAction(msg, callback);
      break;
    case 'CREATE':
      resp_msg = createDBAction(msg, callback);
      break;
    case 'DELETE':
      resp_msg = deleteDBAction(msg, callback);
      break;
    case 'VIEW':
      resp_msg = viewAction(msg, callback);
      break;
  }
  return resp_msg;
};


var actionToFunction = function(action) {
  return (function(action) {
    return function(callback) {
      evaluateAction(action, callback);
    };
  }(action));
};


var evaluateMessage = function (msg, myCallback) {

  dbc.hasMember(msg, 'dao_actions');
  dbc.hasMember(msg, 'clients');

  dbc.assert(msg.clients.length     > 0);
  dbc.assert(msg.dao_actions.length > 0);

  var arr = {};

  for ( var i = 0; i < msg.dao_actions.length; i++){

    var action = msg.dao_actions[i];
    arr[i]     = actionToFunction(action);

  }

  async.series(arr, function(err, results, httpStatusCode){
    myCallback(err, results, httpStatusCode);
  });

};



module.exports.init            = init;
module.exports.evaluateMessage = evaluateMessage;
